use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("Connection error: {0}")]
    ConnectionError(String),

    #[error("Query error: {0}")]
    QueryError(String),

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Database type '{0}' not supported")]
    UnsupportedType(String),

    #[error("Database not found: {0}")]
    DatabaseNotFound(String),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("ClickHouse error: {0}")]
    ClickHouseError(#[from] clickhouse::error::Error),

    #[error("SQLite error: {0}")]
    SQLiteError(#[from] rusqlite::Error),

    #[error("Other error: {0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, DatabaseError>;
