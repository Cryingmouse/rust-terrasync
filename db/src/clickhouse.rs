use async_trait::async_trait;
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use slog_scope::debug;
use uuid::Uuid;

use crate::config::ClickHouseConfig;
use crate::error::{DatabaseError, Result};
use crate::traits::{Database, QueryResult, TableSchema};

/// 文件扫描事件结构体 - 用于异步插入
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct FileScanRecord {
    pub path: String,
    pub size: i64,
    pub ext: String,
    pub ctime: i64, // 恢复为i64类型
    pub mtime: i64, // 恢复为i64类型
    pub atime: i64, // 恢复为i64类型
    pub perm: u32,
    pub is_symlink: bool,
    pub is_dir: bool,
    pub is_regular_file: bool,
    pub dir_handle: String,
    pub current_state: u8,
}

pub struct ClickHouseDatabase {
    config: ClickHouseConfig,
    sync_client: Client,
    async_client: Client,
    job_id: String,
    scan_temp_table_name: Option<String>,
}

// Table name constants
const SCAN_BASE_TABLE_BASE_NAME: &str = "scan_base";
const SCAN_TEMP_TABLE_BASE_NAME: &str = "temp_files";
const SCAN_STATE_TABLE_BASE_NAME: &str = "scan_state";

/// 文件扫描记录的标准列定义
const FILE_SCAN_COLUMNS_DEFINITION: &str = r#"
    path String,
    size Int64,
    ext String,
    ctime DateTime64(3),
    mtime DateTime64(3),
    atime DateTime64(3),
    perm UInt32,
    is_symlink Bool,
    is_dir Bool,
    is_regular_file Bool,
    dir_handle String,
    current_state UInt8
"#;

impl ClickHouseDatabase {
    pub fn new(config: ClickHouseConfig, job_id: String) -> Self {
        // 创建同步客户端
        let mut sync_client = Client::default()
            .with_url(&config.dsn)
            .with_database(config.database.as_deref().unwrap_or("default"));

        // 创建异步客户端（配置异步插入参数）
        let mut async_client = Client::default()
            .with_url(&config.dsn)
            .with_database(config.database.as_deref().unwrap_or("default"))
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");

        // 可选的用户名和密码配置（两个客户端都配置）
        if let Some(username) = &config.username {
            sync_client = sync_client.with_user(username);
            async_client = async_client.with_user(username);
        }
        if let Some(password) = &config.password {
            sync_client = sync_client.with_password(password);
            async_client = async_client.with_password(password);
        }

        Self {
            config,
            sync_client,
            async_client,
            job_id,
            scan_temp_table_name: None,
        }
    }

    fn get_scan_base_table_name(&self) -> String {
        format!("{}_{}", SCAN_BASE_TABLE_BASE_NAME, self.job_id)
    }

    fn get_scan_state_table_name(&self) -> String {
        format!("{}_{}", SCAN_STATE_TABLE_BASE_NAME, self.job_id)
    }

    /// 创建主扫描表
    /// 创建包含完整文件信息字段的主表，用于存储扫描结果
    /// 表结构包含：路径、大小、扩展名、创建时间、修改时间、访问时间、权限、符号链接标志、目录标志、普通文件标志、目录句柄、当前状态
    /// 使用ReplacingMergeTree引擎，基于path字段排序，自动处理重复数据
    pub async fn create_scan_base_table(&self) -> Result<()> {
        let table_name = self.get_scan_base_table_name();
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = ReplacingMergeTree() ORDER BY (path)",
            table_name, FILE_SCAN_COLUMNS_DEFINITION
        );

        debug!("Creating ClickHouse scan base table: {}", table_name);
        self.execute(&create_table_sql, &[]).await?;

        Ok(())
    }

    /// 创建状态表
    /// 创建用于存储扫描状态信息的表，包含id和origin_state字段
    /// 使用ReplacingMergeTree引擎，基于id字段排序，确保状态数据唯一性
    pub async fn create_scan_state_table(&self) -> Result<()> {
        let table_name = self.get_scan_state_table_name();
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (id UInt8, origin_state UInt8) ENGINE = ReplacingMergeTree() ORDER BY id",
            table_name
        );

        debug!("Creating ClickHouse scan state table: {}", table_name);
        self.execute(&create_table_sql, &[]).await?;

        Ok(())
    }

    /// 根据表名删除指定表
    pub async fn drop_table_by_name(&self, table_name: &str) -> Result<()> {
        let drop_table_sql = format!("DROP TABLE IF EXISTS {}", table_name);

        debug!("Dropping ClickHouse table: {}", table_name);
        self.execute(&drop_table_sql, &[]).await?;

        debug!("ClickHouse table '{}' dropped successfully", table_name);
        Ok(())
    }

    /// 删除所有以指定前缀开头的表
    pub async fn drop_tables_with_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let query = format!(
            "SELECT name FROM system.tables WHERE name LIKE '{}%' AND database = currentDatabase()",
            prefix
        );

        let table_names: Vec<String> = self
            .sync_client
            .query(&query)
            .fetch_all::<String>()
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        let mut dropped_tables = Vec::new();
        for table_name in table_names {
            self.drop_table_by_name(&table_name).await?;
            dropped_tables.push(table_name);
        }

        debug!(
            "Dropped {} tables with prefix '{}'",
            dropped_tables.len(),
            prefix
        );
        Ok(dropped_tables)
    }

    /// 异步插入单个文件扫描事件
    pub async fn insert_file_record_async(&self, event: FileScanRecord) -> Result<()> {
        let table_name = self.get_scan_base_table_name();
        let mut insert = self
            .async_client
            .insert::<FileScanRecord>(&table_name)
            .map_err(|e| DatabaseError::ClickHouseError(e))?;

        insert
            .write(&event)
            .await
            .map_err(|e| DatabaseError::ClickHouseError(e))?;

        insert
            .end()
            .await
            .map_err(|e| DatabaseError::ClickHouseError(e))?;

        debug!("Async inserted 1 file event");
        Ok(())
    }

    /// 同步插入scan_state表，id固定为1
    pub async fn insert_scan_state_sync(&self, origin_state: u8) -> Result<()> {
        let table_name = self.get_scan_state_table_name();
        let insert_sql = format!(
            "INSERT INTO {} (id, origin_state) VALUES (?, ?)",
            table_name
        );

        debug!("Inserting scan state: id=1, origin_state={}", origin_state);

        self.sync_client
            .query(&insert_sql)
            .bind(1u8)
            .bind(origin_state)
            .execute()
            .await
            .map_err(|e| DatabaseError::ClickHouseError(e))?;

        debug!(
            "Inserted scan state record: id=1, origin_state={}",
            origin_state
        );
        Ok(())
    }

    /// 查询scan_base表，支持指定列查询，使用FINAL关键字
    pub async fn query_scan_base_table(&self, columns: &[&str]) -> Result<Vec<FileScanRecord>> {
        let table_name = self.get_scan_base_table_name();
        let select_columns = if columns.is_empty() {
            "*".to_string()
        } else {
            columns.join(", ")
        };
        
        let query = format!(
            "SELECT {} FROM {} FINAL",
            select_columns,
            table_name
        );

        let rows = self
            .sync_client
            .query(&query)
            .fetch_all::<FileScanRecord>()
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(rows)
    }

    /// 查询scan_state表
    pub async fn query_scan_state_table(&self) -> Result<Vec<(u8, u8)>> {
        let table_name = self.get_scan_state_table_name();
        let query = format!(
            "SELECT id, origin_state FROM {} FINAL",
            table_name
        );

        let rows = self
            .sync_client
            .query(&query)
            .fetch_all::<(u8, u8)>()
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(rows)
    }
}

#[async_trait]
impl Database for ClickHouseDatabase {
    async fn initialize(&self) -> Result<()> {
        debug!("Initializing ClickHouse connection to: {}", self.config.dsn);

        // 测试连接
        self.sync_client
            .query("SELECT 1")
            .fetch_one::<u8>()
            .await
            .map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;

        debug!("ClickHouse connection established successfully");
        Ok(())
    }

    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        debug!("Executing ClickHouse query: {}", sql);

        let mut query = self.sync_client.query(sql);

        // 绑定参数
        for param in params {
            if let Some(s) = param.as_str() {
                query = query.bind(s);
            } else if let Some(n) = param.as_i64() {
                query = query.bind(n);
            } else if let Some(b) = param.as_bool() {
                query = query.bind(b);
            } else {
                query = query.bind(param.to_string());
            }
        }

        // 使用简化方式获取结果
        let _rows = query
            .fetch_all::<Vec<(String, String)>>()
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        // 转换结果格式 - 这里简化处理，返回空结果
        // 在实际应用中，需要根据具体表结构来处理
        Ok(QueryResult {
            rows: Vec::new(),
            affected_rows: 0,
            last_insert_id: None,
        })
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult> {
        debug!("Executing ClickHouse statement: {}", sql);

        let mut query = self.sync_client.query(sql);

        // 绑定参数
        for param in params {
            if let Some(s) = param.as_str() {
                query = query.bind(s);
            } else if let Some(n) = param.as_i64() {
                query = query.bind(n);
            } else if let Some(b) = param.as_bool() {
                query = query.bind(b);
            } else {
                query = query.bind(param.to_string());
            }
        }

        query
            .execute()
            .await
            .map_err(|e| DatabaseError::ClickHouseError(e))?;

        Ok(QueryResult {
            rows: Vec::new(),
            affected_rows: 0, // ClickHouse execute返回()，无法获取affected_rows
            last_insert_id: None,
        })
    }

    async fn execute_batch(
        &self, sql: &str, params_batch: &[Vec<Value>],
    ) -> Result<Vec<QueryResult>> {
        debug!(
            "Executing ClickHouse batch: {} with {} sets of parameters",
            sql,
            params_batch.len()
        );

        let mut results = Vec::new();
        for _ in params_batch {
            results.push(self.execute(sql, &[]).await?);
        }
        Ok(results)
    }

    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        println!("Checking if ClickHouse table exists: {}", table_name);

        let query = format!(
            "SELECT count(*) FROM system.tables WHERE name = '{}'",
            table_name
        );

        let count: u64 = self
            .sync_client
            .query(&query)
            .fetch_one()
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(count > 0)
    }

    async fn create_table(&self, schema: &TableSchema) -> Result<()> {
        // 根据表名调用相应的创建方法
        match schema.name.as_str() {
            SCAN_BASE_TABLE_BASE_NAME => self.create_scan_base_table().await,
            SCAN_STATE_TABLE_BASE_NAME => self.create_scan_state_table().await,
            SCAN_TEMP_TABLE_BASE_NAME => {
                // 临时表创建需要可变引用，通过通用接口不支持
                Err(DatabaseError::UnsupportedType(
                    "Temporary table creation requires mutable reference. Use create_scan_temporary_table() instead.".to_string()
                ))
            }
            _ => {
                // 通用表创建 - 对于未知表名，直接返回错误
                Err(DatabaseError::UnsupportedType(format!(
                    "Unknown table: {}",
                    schema.name
                )))
            }
        }
    }

    async fn ping(&self) -> Result<()> {
        println!("Pinging ClickHouse server...");
        Ok(()) // Mock implementation
    }

    async fn close(&self) -> Result<()> {
        println!("Closing ClickHouse connection...");
        Ok(()) // Mock implementation
    }

    fn database_type(&self) -> &'static str {
        "clickhouse"
    }

    async fn create_scan_temporary_table(&mut self) -> Result<()> {
        let uuid = Uuid::new_v4().to_string().replace('-', "_");
        let temp_table_name = format!("temp_files_{}", uuid);
        let create_table_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} ({}) ENGINE = MergeTree() ORDER BY (path)",
            temp_table_name, FILE_SCAN_COLUMNS_DEFINITION
        );

        debug!(
            "Creating ClickHouse scan temporary table: {}",
            temp_table_name
        );
        self.execute(&create_table_sql, &[]).await?;

        self.scan_temp_table_name = Some(temp_table_name);
        Ok(())
    }

    async fn drop_scan_temporary_table(&mut self) -> Result<()> {
        if let Some(temp_table_name) = &self.scan_temp_table_name {
            let drop_table_sql = format!("DROP TABLE IF EXISTS {}", temp_table_name);

            debug!(
                "Dropping ClickHouse scan temporary table: {}",
                temp_table_name
            );
            self.execute(&drop_table_sql, &[]).await?;

            debug!(
                "ClickHouse scan temporary table '{}' dropped successfully",
                temp_table_name
            );

            // 清除临时表名记录
            self.scan_temp_table_name = None;
        } else {
            debug!("No temporary table to drop");
        }
        Ok(())
    }

    async fn batch_insert_temp_record_sync(&self, events: Vec<serde_json::Value>) -> Result<()> {
        let temp_table_name = self.scan_temp_table_name.as_deref().ok_or_else(|| {
            DatabaseError::UnsupportedType("No temporary table available".to_string())
        })?;

        if events.is_empty() {
            debug!("No events to insert");
            return Ok(());
        }

        // Convert JSON values to FileScanRecord
        let records: Vec<FileScanRecord> = events
            .into_iter()
            .map(|event| serde_json::from_value(event).map_err(DatabaseError::SerializationError))
            .collect::<Result<Vec<_>>>()?;

        let record_count = records.len();
        let mut insert = self
            .sync_client
            .insert::<FileScanRecord>(&temp_table_name)
            .map_err(|e| DatabaseError::ClickHouseError(e))?;

        for record in &records {
            insert.write(record).await?;
        }

        insert.end().await?;

        debug!(
            "Synchronously inserted {} events to temporary table",
            record_count
        );
        Ok(())
    }

    /// 获取当前临时表名
    fn get_scan_temp_table_name(&self) -> Option<&str> {
        self.scan_temp_table_name.as_deref()
    }
}
