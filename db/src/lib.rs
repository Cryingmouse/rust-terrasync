pub mod clickhouse;
pub mod config;
pub mod error;
pub mod factory;
pub mod sqlite;
pub mod traits;

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
