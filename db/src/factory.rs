use dashmap::DashMap;
use once_cell::sync::Lazy;

use crate::config::DatabaseConfig;
use crate::error::{DatabaseError, Result};
use crate::traits::Database;

use crate::clickhouse::ClickHouseDatabase;
use std::sync::Arc;

pub type DatabaseCreator = fn(config: &DatabaseConfig, job_id: String) -> Result<Arc<dyn Database>>;

static DATABASE_REGISTRY: Lazy<DashMap<String, DatabaseCreator>> = Lazy::new(|| {
    let registry = DashMap::new();
    // 自动注册内置数据库类型
    let _ = register_builtin_types(&registry);
    registry
});

pub struct DatabaseFactory;

impl DatabaseFactory {
    /// Register a new database type
    pub fn register_database_type(db_type: &str, creator: DatabaseCreator) -> Result<()> {
        DATABASE_REGISTRY.insert(db_type.to_string(), creator);
        Ok(())
    }

    /// Create a database instance based on configuration
    pub fn create_database(config: &DatabaseConfig, job_id: String) -> Result<Arc<dyn Database>> {
        if !config.enabled {
            return Err(DatabaseError::ConfigError("Database is disabled".to_string()));
        }

        if config.db_type != "clickhouse" {
            return Err(DatabaseError::UnsupportedType(format!(
                "Database type '{}' is not supported",
                config.db_type
            )));
        }

        let clickhouse_config = config.clickhouse.as_ref()
            .ok_or_else(|| DatabaseError::ConfigError("ClickHouse configuration missing".to_string()))?;

        let db = ClickHouseDatabase::new(clickhouse_config.clone(), job_id);
        Ok(Arc::new(db) as Arc<dyn Database>)
    }
}

/// Convenience function to create a database from configuration
pub fn create_database(config: &DatabaseConfig, job_id: String) -> Result<Arc<dyn Database>> {
    DatabaseFactory::create_database(config, job_id)
}

// 内置类型注册函数
fn register_builtin_types(registry: &DashMap<String, DatabaseCreator>) -> Result<()> {
    // Register ClickHouse
    registry.insert("clickhouse".to_string(), |config, job_id| {
        let clickhouse_config = config.clickhouse.as_ref()
            .ok_or_else(|| DatabaseError::ConfigError("ClickHouse configuration missing".to_string()))?;
        
        let db = ClickHouseDatabase::new(clickhouse_config.clone(), job_id);
        Ok(Arc::new(db) as Arc<dyn Database>)
    });

    Ok(())
}
