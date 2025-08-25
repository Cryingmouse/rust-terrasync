use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub rows: Vec<serde_json::Value>,
    pub affected_rows: u64,
    pub last_insert_id: Option<u64>,
}

/// 文件扫描事件结构体 - 统一的数据结构
#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct FileScanRecord {
    pub path: String,
    pub size: u64,
    pub ext: Option<String>,
    pub ctime: u64,
    pub mtime: u64,
    pub atime: u64,
    pub perm: u32,
    pub is_symlink: bool,
    pub is_dir: bool,
    pub is_regular_file: bool,
    pub file_handle: Option<String>,
    pub current_state: u8,
}

#[async_trait]
pub trait Database: Send + Sync {
    /// Ping database to check connection
    async fn ping(&self) -> Result<()>;

    /// Create table by name
    async fn create_table(&self, table_name: &str) -> Result<()>;

    /// Drop table by name
    async fn drop_table(&self, table_name: &str) -> Result<()>;

    /// Execute a query without returning results (INSERT, UPDATE, DELETE)
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Check if table exists
    async fn table_exists(&self, table_name: &str) -> Result<bool>;

    /// Close database connection
    async fn close(&self) -> Result<()>;

    /// Get database type
    fn database_type(&self) -> &'static str;

    /// 创建临时扫描表
    async fn create_scan_temporary_table(&mut self) -> Result<()>;

    /// 删除当前临时表
    async fn drop_scan_temporary_table(&mut self) -> Result<()>;

    /// 同步批量插入数据到临时表
    async fn batch_insert_temp_record_sync(&self, records: Vec<FileScanRecord>) -> Result<()>;

    /// 获取当前临时表名
    fn get_scan_temp_table_name(&self) -> Option<&str>;

    /// 同步批量插入数据到base表
    async fn batch_insert_base_record_sync(&self, records: Vec<FileScanRecord>) -> Result<()>;

    /// 异步批量插入数据到base表
    async fn batch_insert_base_record_async(&self, records: Vec<FileScanRecord>) -> Result<()>;

    /// 查询scan_base表，支持指定列查询
    async fn query_scan_base_table(&self, columns: &[&str]) -> Result<Vec<FileScanRecord>>;

    /// 查询scan_state表
    async fn query_scan_state_table(&self) -> Result<u8>;

    /// 切换scan_state表状态
    async fn switch_scan_state(&self) -> Result<()>;

    async fn insert_scan_state_sync(&self, origin_state: u8) -> Result<()>;
}
