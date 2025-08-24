use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::collections::HashMap;

use crate::config::DatabaseConfig;
use crate::error::{DatabaseError, Result};
use crate::traits::Database;

use crate::clickhouse::ClickHouseDatabase;
use crate::sqlite::SQLiteDatabase;

pub type DatabaseCreator = fn(config: &DatabaseConfig) -> Result<Box<dyn Database>>;

static DATABASE_REGISTRY: Lazy<DashMap<String, DatabaseCreator>> = Lazy::new(DashMap::new);

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

    /// Initialize built-in database types
    pub fn initialize() -> Result<()> {
        // Register ClickHouse
        Self::register_database_type("clickhouse", |config| {
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
        })?;

        // Register SQLite
        Self::register_database_type("sqlite", |config| {
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
        })?;

        Ok(())
    }
}

/// Convenience function to create a database from configuration
pub fn create_database(config: &DatabaseConfig) -> Result<Box<dyn Database>> {
    DatabaseFactory::create_database(config)
}

/// Database manager for handling multiple database instances
pub struct DatabaseManager {
    databases: HashMap<String, Box<dyn Database>>,
}

impl DatabaseManager {
    pub fn new() -> Self {
        Self {
            databases: HashMap::new(),
        }
    }

    pub fn add_database(&mut self, name: String, database: Box<dyn Database>) -> Result<()> {
        self.databases.insert(name, database);
        Ok(())
    }

    pub fn get_database(&self, name: &str) -> Option<&Box<dyn Database>> {
        self.databases.get(name)
    }

    pub fn get_database_mut(&mut self, name: &str) -> Option<&mut Box<dyn Database>> {
        self.databases.get_mut(name)
    }

    pub fn remove_database(&mut self, name: &str) -> Option<Box<dyn Database>> {
        self.databases.remove(name)
    }

    pub async fn initialize_all(&mut self) -> Result<()> {
        for (name, db) in &mut self.databases {
            db.initialize().await.map_err(|e| {
                DatabaseError::ConnectionError(format!("Failed to initialize {}: {}", name, e))
            })?;
        }
        Ok(())
    }

    pub async fn close_all(&mut self) -> Result<()> {
        for (name, db) in &mut self.databases {
            db.close()
                .await
                .map_err(|e| DatabaseError::Other(format!("Failed to close {}: {}", name, e)))?;
        }
        Ok(())
    }

    pub fn list_databases(&self) -> Vec<String> {
        self.databases.keys().cloned().collect()
    }
}

impl Default for DatabaseManager {
    fn default() -> Self {
        Self::new()
    }
}
