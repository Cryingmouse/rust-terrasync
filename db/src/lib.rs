pub mod clickhouse;
pub mod config;
pub mod error;
pub mod factory;
pub mod traits;

// 共享的表名常量
pub const SCAN_BASE_TABLE_BASE_NAME: &str = "scan_base";
pub const SCAN_TEMP_TABLE_BASE_NAME: &str = "scan_temp";
pub const SCAN_STATE_TABLE_BASE_NAME: &str = "scan_state";

pub use clickhouse::ClickHouseDatabase;
pub use config::{ClickHouseConfig, DatabaseConfig, DatabaseType};
pub use error::{DatabaseError, Result};
pub use factory::{DatabaseFactory, create_database};
pub use traits::{Database, QueryResult};

/// 根据job_id生成扫描基础表名
pub fn get_scan_base_table_name(job_id: &str) -> String {
    format!("{}_{}", SCAN_BASE_TABLE_BASE_NAME, job_id)
}

/// 根据job_id生成扫描状态表名
pub fn get_scan_state_table_name(job_id: &str) -> String {
    format!("{}_{}", SCAN_STATE_TABLE_BASE_NAME, job_id)
}

/// 生成唯一的临时扫描表名
pub fn generate_scan_temp_table_name() -> String {
    use uuid::Uuid;
    let uuid = Uuid::new_v4().to_string().replace('-', "_");
    format!("{}_{}", SCAN_TEMP_TABLE_BASE_NAME, uuid)
}
