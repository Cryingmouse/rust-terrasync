use async_trait::async_trait;
use clickhouse::Client;
use serde_json::Value;
use slog_scope::debug;

use crate::config::ClickHouseConfig;
use crate::error::{DatabaseError, Result};
use crate::traits::FileScanRecord;
use crate::traits::{Database, QueryResult};
use crate::{SCAN_BASE_TABLE_BASE_NAME, SCAN_STATE_TABLE_BASE_NAME};
use crate::{generate_scan_temp_table_name, get_scan_base_table_name, get_scan_state_table_name};

pub struct ClickHouseDatabase {
    sync_client: Client,
    job_id: String,
    scan_temp_table_name: Option<String>,
}

/// 文件扫描记录的标准列定义
const FILE_SCAN_COLUMNS_DEFINITION: &str = r#"
    path String,
    size UInt64,
    ext Nullable(String),
    ctime UInt64,
    mtime UInt64,
    atime UInt64,
    perm UInt32,
    is_symlink UInt8,
    is_dir UInt8,
    is_regular_file UInt8,
    file_handle Nullable(String),
    current_state UInt8
"#;

impl ClickHouseDatabase {
    pub fn new(config: ClickHouseConfig, job_id: String) -> Self {
        // 创建同步客户端
        let mut sync_client = Client::default()
            .with_url(&config.dsn)
            .with_database(config.database)
            .with_user(config.username);

        // 可选的密码配置
        if let Some(password) = &config.password {
            sync_client = sync_client.with_password(password);
        }

        Self {
            sync_client,
            job_id,
            scan_temp_table_name: None,
        }
    }

    /// 创建主扫描表
    /// 创建包含完整文件信息字段的主表，用于存储扫描结果
    /// 表结构包含：路径、大小、扩展名、创建时间、修改时间、访问时间、权限、符号链接标志、目录标志、普通文件标志、目录句柄、当前状态
    /// 使用ReplacingMergeTree引擎，基于path字段排序，自动处理重复数据
    pub async fn create_scan_base_table(&self) -> Result<()> {
        let table_name = get_scan_base_table_name(&self.job_id);
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
        let table_name = get_scan_state_table_name(&self.job_id);
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
}

#[async_trait]
impl Database for ClickHouseDatabase {
    async fn ping(&self) -> Result<()> {
        // 测试连接
        self.sync_client
            .query("SELECT 1")
            .fetch_one::<u8>()
            .await
            .map_err(|e| DatabaseError::ConnectionError(e.to_string()))?;

        debug!("ClickHouse connection established successfully");
        Ok(())
    }

    async fn create_table(&self, table_name: &str) -> Result<()> {
        // 根据表名调用相应的创建方法
        match table_name {
            SCAN_BASE_TABLE_BASE_NAME => self.create_scan_base_table().await,
            SCAN_STATE_TABLE_BASE_NAME => self.create_scan_state_table().await,
            _ => {
                // 通用表创建 - 对于未知表名，直接返回错误
                Err(DatabaseError::UnsupportedType(format!(
                    "Unknown table: {}",
                    table_name
                )))
            }
        }
    }

    async fn drop_table(&self, table_name: &str) -> Result<()> {
        // 根据表名调用相应的删除方法
        match table_name {
            SCAN_BASE_TABLE_BASE_NAME => {
                self.drop_table_by_name(&get_scan_base_table_name(&self.job_id))
                    .await
            }
            SCAN_STATE_TABLE_BASE_NAME => {
                self.drop_table_by_name(&get_scan_state_table_name(&self.job_id))
                    .await
            }
            _ => {
                // 通用表删除 - 对于未知表名，直接删除指定表名
                self.drop_table_by_name(table_name).await
            }
        }
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

    async fn table_exists(&self, table_name: &str) -> Result<bool> {
        debug!("Checking if ClickHouse table exists: {}", table_name);

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

    async fn close(&self) -> Result<()> {
        debug!("Closing ClickHouse connection...");
        Ok(()) // Mock implementation
    }

    fn database_type(&self) -> &'static str {
        "clickhouse"
    }

    async fn create_scan_temporary_table(&mut self) -> Result<()> {
        let temp_table_name = generate_scan_temp_table_name();
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

    async fn batch_insert_temp_record_sync(&self, records: Vec<FileScanRecord>) -> Result<()> {
        let temp_table_name = self.scan_temp_table_name.as_deref().ok_or_else(|| {
            DatabaseError::UnsupportedType("No temporary table available".to_string())
        })?;

        if records.is_empty() {
            debug!("No events to insert");
            return Ok(());
        }

        let record_count = records.len();

        // 使用标准insert方法进行批量插入
        let mut insert = self
            .sync_client
            .insert(temp_table_name)
            .map_err(|e| DatabaseError::ClickHouseError(e))?;

        // 批量写入所有记录
        for record in &records {
            insert
                .write(record)
                .await
                .map_err(|e| DatabaseError::ClickHouseError(e))?;
        }

        // 确保最终完成
        insert
            .end()
            .await
            .map_err(|e| DatabaseError::ClickHouseError(e))?;

        debug!(
            "Successfully inserted {} events to temporary table",
            record_count
        );
        Ok(())
    }

    /// 获取当前临时表名
    fn get_scan_temp_table_name(&self) -> Option<&str> {
        self.scan_temp_table_name.as_deref()
    }

    async fn batch_insert_base_record_sync(&self, records: Vec<FileScanRecord>) -> Result<()> {
        let base_table_name = get_scan_base_table_name(&self.job_id);

        if records.is_empty() {
            debug!("No events to insert");
            return Ok(());
        }

        let record_count = records.len();

        // 使用标准insert方法进行批量插入
        let mut insert = self
            .sync_client
            .insert(&base_table_name)
            .map_err(|e| DatabaseError::ClickHouseError(e))?;

        // 批量写入所有记录
        for record in &records {
            insert
                .write(record)
                .await
                .map_err(|e| DatabaseError::ClickHouseError(e))?;
        }

        // 确保最终完成
        insert
            .end()
            .await
            .map_err(|e| DatabaseError::ClickHouseError(e))?;

        debug!(
            "Successfully inserted {} events to temporary table",
            record_count
        );
        Ok(())
    }

    /// 查询scan_base表，支持指定列查询，使用FINAL关键字
    async fn query_scan_base_table(&self, columns: &[&str]) -> Result<Vec<FileScanRecord>> {
        let table_name = get_scan_base_table_name(&self.job_id);
        let select_columns = if columns.is_empty() {
            "*".to_string()
        } else {
            columns.join(", ")
        };

        let query = format!("SELECT {} FROM {} FINAL", select_columns, table_name);

        let rows = self
            .sync_client
            .query(&query)
            .fetch_all::<FileScanRecord>()
            .await
            .map_err(|e| DatabaseError::QueryError(e.to_string()))?;

        Ok(rows)
    }

    /// 查询scan_state表，返回id=1的origin_state值
    /// 当记录不存在时返回错误
    async fn query_scan_state_table(&self) -> Result<u8> {
        let table_name = get_scan_state_table_name(&self.job_id);
        let query = format!("SELECT origin_state FROM {} FINAL WHERE id = 1", table_name);

        let origin_state = self
            .sync_client
            .query(&query)
            .fetch_one::<u8>()
            .await
            .map_err(|e| match e {
                clickhouse::error::Error::RowNotFound => {
                    DatabaseError::QueryError("No scan state record found for id=1".to_string())
                }
                _ => DatabaseError::QueryError(format!(
                    "Failed to query scan_state table: {}",
                    e.to_string()
                )),
            })?;

        Ok(origin_state)
    }

    /// 切换scan_state表状态
    async fn switch_scan_state(&self) -> Result<()> {
        // 查询当前状态
        let current_state = self.query_scan_state_table().await?;

        // 反转状态（1 - 当前状态）
        let new_state = 1 - current_state;

        // 调用insert_scan_state_sync设置新状态
        self.insert_scan_state_sync(new_state).await?;

        debug!("Switched scan state: {} -> {}", current_state, new_state);

        Ok(())
    }

    /// 同步插入scan_state表，id固定为1
    async fn insert_scan_state_sync(&self, origin_state: u8) -> Result<()> {
        let table_name = get_scan_state_table_name(&self.job_id);
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
}
