use dashmap::DashMap;
use once_cell::sync::Lazy;

use crate::config::DatabaseConfig;
use crate::error::{DatabaseError, Result};
use crate::traits::Database;

use crate::clickhouse::ClickHouseDatabase;
use crate::sqlite::SQLiteDatabase;

pub type DatabaseCreator = fn(config: &DatabaseConfig) -> Result<Box<dyn Database>>;

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
    pub fn create_database(config: &DatabaseConfig) -> Result<Box<dyn Database>> {
        if !config.enabled {
            return Err(DatabaseError::ConfigError(
                "Database is disabled".to_string(),
            ));
        }

        let db_type = &config.db_type;

        if let Some(creator) = DATABASE_REGISTRY.get(db_type) {
            creator(config)
        } else {
            Err(DatabaseError::UnsupportedType(db_type.clone()))
        }
    }

    /// Get all registered database types
    pub fn get_supported_types() -> Vec<String> {
        DATABASE_REGISTRY
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }
}

/// Convenience function to create a database from configuration
pub fn create_database(config: &DatabaseConfig) -> Result<Box<dyn Database>> {
    DatabaseFactory::create_database(config)
}

// 内置类型注册函数
fn register_builtin_types(registry: &DashMap<String, DatabaseCreator>) -> Result<()> {
    // Register ClickHouse
    registry.insert("clickhouse".to_string(), |config| {
        if let Some(clickhouse_config) = &config.clickhouse {
            Ok(Box::new(ClickHouseDatabase::new(
                clickhouse_config.clone(),
                config.job_id.clone(),
            )))
        } else {
            Err(DatabaseError::ConfigError(
                "ClickHouse configuration missing".to_string(),
            ))
        }
    });

    // Register SQLite
    registry.insert("sqlite".to_string(), |config| {
        if let Some(sqlite_config) = &config.sqlite {
            Ok(Box::new(SQLiteDatabase::new(
                sqlite_config.clone(),
                config.job_id.clone(),
            )?))
        } else {
            Err(DatabaseError::ConfigError(
                "SQLite configuration missing".to_string(),
            ))
        }
    });

    Ok(())
}
