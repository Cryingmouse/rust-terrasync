use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResult {
    pub rows: Vec<HashMap<String, Value>>,
    pub affected_rows: u64,
    pub last_insert_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub default_value: Option<String>,
    pub is_primary_key: bool,
}

#[async_trait]
pub trait Database: Send + Sync {
    /// Initialize database connection
    async fn initialize(&self) -> Result<()>;

    /// Execute a query and return results
    async fn query(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Execute a query without returning results (INSERT, UPDATE, DELETE)
    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult>;

    /// Execute batch queries
    async fn execute_batch(
        &self, sql: &str, params_batch: &[Vec<Value>],
    ) -> Result<Vec<QueryResult>>;

    /// Check if table exists
    async fn table_exists(&self, table_name: &str) -> Result<bool>;

    /// Create table from schema
    async fn create_table(&self, schema: &TableSchema) -> Result<()>;

    /// Ping database to check connection
    async fn ping(&self) -> Result<()>;

    /// Close database connection
    async fn close(&self) -> Result<()>;

    /// Get database type
    fn database_type(&self) -> &'static str;

    /// 创建临时扫描表
    /// 在内部维护临时表名，不返回表名
    async fn create_scan_temporary_table(&mut self) -> Result<()>;

    /// 删除当前临时表
    async fn drop_scan_temporary_table(&mut self) -> Result<()>;

    /// 同步批量插入数据到临时表
    async fn batch_insert_temp_record_sync(&self, events: Vec<serde_json::Value>) -> Result<()>;

    fn get_scan_temp_table_name(&self) -> Option<&str>;
}
