use async_trait::async_trait;
use rusqlite::{params_from_iter, types::ValueRef, Connection};
use serde_json::Value;
use slog_scope::debug;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::SQLiteConfig;
use crate::error::{DatabaseError, Result};
use crate::traits::{Database, QueryResult, TableSchema};
use crate::{generate_scan_temp_table_name, get_scan_base_table_name, get_scan_state_table_name};
use crate::{SCAN_BASE_TABLE_BASE_NAME, SCAN_STATE_TABLE_BASE_NAME, SCAN_TEMP_TABLE_BASE_NAME};

const FILE_SCAN_COLUMNS_DEFINITION: &str = "
    path TEXT PRIMARY KEY,
    size INTEGER,
    ext TEXT,
    ctime INTEGER,
    mtime INTEGER,
    atime INTEGER,
    perm INTEGER,
    is_symlink BOOLEAN,
    is_dir BOOLEAN,
    is_regular_file BOOLEAN,
    file_handle TEXT,
    current_state INTEGER
";

pub struct SQLiteDatabase {
    connection: Arc<Mutex<Connection>>,
    config: SQLiteConfig,
    job_id: String,
    scan_temp_table_name: String,
}
impl SQLiteDatabase {
    pub fn new(config: SQLiteConfig, job_id: String) -> Result<Self> {
        let conn = Connection::open(&config.path)?;

        // Configure SQLite
        conn.pragma_update(None, "busy_timeout", config.busy_timeout)?;

        if let Some(journal_mode) = &config.journal_mode {
            conn.pragma_update(None, "journal_mode", journal_mode)?;
        }

        if let Some(synchronous) = &config.synchronous {
            conn.pragma_update(None, "synchronous", synchronous)?;
        }

        if let Some(cache_size) = config.cache_size {
            conn.pragma_update(None, "cache_size", cache_size)?;
        }

        let scan_temp_table_name = format!("{}_{}", SCAN_TEMP_TABLE_BASE_NAME, job_id);

        Ok(Self {
            connection: Arc::new(Mutex::new(conn)),
            config,
            job_id,
            scan_temp_table_name,
        })
    }

    async fn create_scan_base_table(&self) -> Result<()> {
        let conn = self.connection.lock().await;
        let table_name = get_scan_base_table_name(&self.job_id);

        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table_name, FILE_SCAN_COLUMNS_DEFINITION
        );

        debug!("Creating SQLite scan base table: {}", table_name);
        conn.execute(&create_table_sql, [])?;

        debug!("SQLite scan base table created successfully");
        Ok(())
    }

    fn convert_sqlite_value(value: ValueRef) -> Value {
        match value {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(i) => Value::Number(serde_json::Number::from(i)),
            ValueRef::Real(f) => Value::Number(
                serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0)),
            ),
            ValueRef::Text(t) => Value::String(String::from_utf8_lossy(t).to_string()),
            ValueRef::Blob(b) => Value::Array(
                b.iter()
                    .map(|&b| Value::Number(serde_json::Number::from(b)))
                    .collect(),
            ),
        }
    }

    fn convert_sqlite_type(sqlite_type: &str) -> String {
        match sqlite_type.to_uppercase().as_str() {
            "INTEGER" => "INTEGER".to_string(),
            "REAL" => "REAL".to_string(),
            "TEXT" => "TEXT".to_string(),
            "BLOB" => "BLOB".to_string(),
            "NUMERIC" => "NUMERIC".to_string(),
            _ => sqlite_type.to_string(),
        }
    }
}

#[async_trait]
impl Database for SQLiteDatabase {
    async fn initialize(&self) -> Result<()> {
        self.ping().await
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let conn = self.connection.lock().await;

        let mut stmt = conn.prepare(sql)?;
        let column_names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
        let rusqlite_params: Vec<String> = params.iter().map(|p| p.to_string()).collect();

        let rows = stmt.query_map(params_from_iter(rusqlite_params.iter()), |row| {
            let mut row_map = HashMap::new();
            for (i, column_name) in column_names.iter().enumerate() {
                let value = row.get_ref(i)?;
                row_map.insert(column_name.clone(), Self::convert_sqlite_value(value));
            }
            Ok(row_map)
        })?;

        let mut result_rows = Vec::new();
        for row_result in rows {
            result_rows.push(row_result?);
        }

        Ok(QueryResult {
            rows: result_rows,
            affected_rows: 0,
            last_insert_id: None,
        })
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        let conn = self.connection.lock().await;

        let mut stmt = conn.prepare(sql)?;
        let rusqlite_params: Vec<String> = params.iter().map(|p| p.to_string()).collect();

        let affected_rows = stmt.execute(params_from_iter(rusqlite_params.iter()))? as u64;
        let last_insert_id = conn.last_insert_rowid();

        Ok(QueryResult {
            rows: Vec::new(),
            affected_rows,
            last_insert_id: Some(last_insert_id as u64),
        })
    }

    async fn execute_batch(
        &self, sql: &str, params_batch: &[Vec<Value>],
    ) -> Result<Vec<QueryResult>> {
        let mut conn = self.connection.lock().await;

        let tx = conn.transaction()?;
        let mut results = Vec::new();

        for params in params_batch {
            let mut stmt = tx.prepare(sql)?;
            let rusqlite_params: Vec<String> = params.iter().map(|p| p.to_string()).collect();
            let affected_rows = stmt.execute(params_from_iter(rusqlite_params.iter()))? as u64;
            let last_insert_id = tx.last_insert_rowid();

            results.push(QueryResult {
                rows: Vec::new(),
                affected_rows,
                last_insert_id: Some(last_insert_id as u64),
            });
        }

        tx.commit()?;
        Ok(results)
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        let conn = self.connection.lock().await;

        let mut stmt = conn.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?")?;

        let exists = stmt.exists([table_name])?;

        Ok(exists)
    }

    async fn create_table(&self, schema: &TableSchema) -> Result<()> {
        let conn = self.connection.lock().await;

        let mut columns_sql = Vec::new();
        for column in &schema.columns {
            let null_str = if column.nullable { "" } else { "NOT NULL" };
            let default_str = column
                .default_value
                .as_ref()
                .map(|d| format!(" DEFAULT {}", d))
                .unwrap_or_default();
            let primary_key_str = if column.is_primary_key {
                " PRIMARY KEY"
            } else {
                ""
            };

            columns_sql.push(format!(
                "{} {} {}{}{}",
                column.name, column.data_type, null_str, default_str, primary_key_str
            ));
        }

        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            schema.name,
            columns_sql.join(", ")
        );

        conn.execute(&sql, [])?;
        Ok(())
    }

    async fn ping(&self) -> Result<()> {
        let conn = self.connection.lock().await;

        // 使用 query_row 而不是 execute 来处理 SELECT 语句
        conn.query_row("SELECT 1", [], |_| Ok(()))?;
        Ok(())
    }

    async fn close(&self) -> Result<()> {
        let _conn = self.connection.lock().await;
        // SQLite connection closes automatically when dropped
        Ok(())
    }

    fn database_type(&self) -> &'static str {
        "sqlite"
    }

    fn get_scan_temp_table_name(&self) -> Option<&str> {
        Some(&self.scan_temp_table_name)
    }

    async fn create_scan_temporary_table(&mut self) -> Result<()> {
        let uuid = Uuid::new_v4().to_string().replace('-', "_");
        let temp_table_name = format!("{}_{}", SCAN_TEMP_TABLE_BASE_NAME, uuid);

        let conn = self.connection.lock().await;
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            temp_table_name, FILE_SCAN_COLUMNS_DEFINITION
        );

        debug!("Creating SQLite scan temporary table: {}", temp_table_name);
        conn.execute(&create_table_sql, [])?;

        // 更新临时表名
        self.scan_temp_table_name = temp_table_name;

        debug!("SQLite scan temporary table created successfully");
        Ok(())
    }

    async fn drop_scan_temporary_table(&mut self) -> Result<()> {
        let conn = self.connection.lock().await;
        let temp_table_name = self.get_scan_temp_table_name().ok_or_else(|| {
            DatabaseError::UnsupportedType("No temporary table available".to_string())
        })?;

        let drop_table_sql = format!("DROP TABLE IF EXISTS {}", temp_table_name);

        debug!("Dropping SQLite scan temporary table: {}", temp_table_name);
        conn.execute(&drop_table_sql, [])?;

        debug!("SQLite scan temporary table dropped successfully");
        Ok(())
    }

    async fn batch_insert_temp_record_sync(&self, events: Vec<serde_json::Value>) -> Result<()> {
        let event_count = events.len();
        if event_count == 0 {
            debug!("No events to insert");
            return Ok(());
        }

        let mut conn = self.connection.lock().await;
        let transaction = conn.transaction()?;

        // 使用正确的临时表名
        let temp_table_name = &self.scan_temp_table_name;

        let insert_sql = format!(
            "INSERT INTO {} (path, size, ext, ctime, mtime, atime, perm, is_symlink, is_dir, is_regular_file, file_handle, current_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            temp_table_name
        );

        {
            let mut stmt = transaction.prepare(&insert_sql)?;

            for event in &events {
                // 从JSON中提取字段值
                let path = event["path"].as_str().unwrap_or("").to_string();
                let size = event["size"].as_i64().unwrap_or(0);
                let ext = event["ext"].as_str().unwrap_or("").to_string();
                let ctime = event["ctime"].as_i64().unwrap_or(0);
                let mtime = event["mtime"].as_i64().unwrap_or(0);
                let atime = event["atime"].as_i64().unwrap_or(0);
                let perm = event["perm"].as_u64().unwrap_or(0) as i64;
                let is_symlink = event["is_symlink"].as_bool().unwrap_or(false);
                let is_dir = event["is_dir"].as_bool().unwrap_or(false);
                let is_regular_file = event["is_regular_file"].as_bool().unwrap_or(false);
                let file_handle = event["file_handle"].as_str().unwrap_or("").to_string();
                let current_state = event["current_state"].as_u64().unwrap_or(0) as i64;

                stmt.execute([
                    &path as &dyn rusqlite::ToSql,
                    &size,
                    &ext,
                    &ctime,
                    &mtime,
                    &atime,
                    &perm,
                    &is_symlink,
                    &is_dir,
                    &is_regular_file,
                    &file_handle,
                    &current_state,
                ])?;
            }
        }

        transaction.commit()?;
        debug!("Inserted {} events to temporary table", event_count);
        Ok(())
    }
}
