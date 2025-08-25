use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DatabaseType {
    #[serde(rename = "clickhouse")]
    ClickHouse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub enabled: bool,
    pub db_type: String,
    pub batch_size: u32,
    pub clickhouse: Option<ClickHouseConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    pub dsn: String,
    pub dial_timeout: u32,
    pub read_timeout: u32,
    pub database: String,
    pub username: String,
    pub password: Option<String>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            db_type: "clickhouse".to_string(),
            batch_size: 200000,
            clickhouse: Some(ClickHouseConfig::default()),
        }
    }
}

impl Default for ClickHouseConfig {
    fn default() -> Self {
        Self {
            dsn: "tcp://localhost:9000".to_string(),
            dial_timeout: 10,
            read_timeout: 30,
            database: "default".to_string(),
            username: "default".to_string(),
            password: None,
        }
    }
}
