use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DatabaseType {
    #[serde(rename = "sqlite")]
    SQLite,
    #[serde(rename = "clickhouse")]
    ClickHouse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub enabled: bool,
    pub db_type: String,
    pub batch_size: u32,
    pub job_id: String,
    pub clickhouse: Option<ClickHouseConfig>,
    pub sqlite: Option<SQLiteConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    pub dsn: String,
    pub dial_timeout: u32,
    pub read_timeout: u32,
    pub database: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SQLiteConfig {
    pub path: String,
    pub busy_timeout: u32,
    pub journal_mode: Option<String>,
    pub synchronous: Option<String>,
    pub cache_size: Option<i32>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            db_type: "clickhouse".to_string(),
            batch_size: 200000,
            job_id: "default".to_string(),
            clickhouse: Some(ClickHouseConfig::default()),
            sqlite: None,
        }
    }
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            dsn: "tcp://localhost:9000".to_string(),
            dial_timeout: 10,
            read_timeout: 30,
            database: Some("default".to_string()),
            username: Some("default".to_string()),
            password: None,
        }
    }
}

impl Default for SQLiteConfig {
    fn default() -> Self {
        Self {
            path: String::new(),
            busy_timeout: 5000,
            journal_mode: Some("WAL".to_string()),
            synchronous: Some("NORMAL".to_string()),
            cache_size: Some(1000),
        }
    }
}
