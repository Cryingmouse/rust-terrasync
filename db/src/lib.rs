pub mod clickhouse;
pub mod config;
pub mod error;
pub mod factory;
pub mod sqlite;
pub mod traits;

// 共享的表名常量
pub const SCAN_BASE_TABLE_BASE_NAME: &str = "scan_base";
pub const SCAN_TEMP_TABLE_BASE_NAME: &str = "scan_temp";
pub const SCAN_STATE_TABLE_BASE_NAME: &str = "scan_state";

pub use clickhouse::ClickHouseDatabase;
pub use config::{ClickHouseConfig, DatabaseConfig, DatabaseType, SQLiteConfig};
pub use error::{DatabaseError, Result};
pub use factory::{DatabaseFactory, DatabaseManager};
pub use sqlite::SQLiteDatabase;
pub use traits::{ColumnInfo, Database, QueryResult, TableSchema};

/// Initialize the database framework
pub fn init() -> Result<()> {
    DatabaseFactory::initialize()
}
