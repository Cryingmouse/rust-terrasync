use thiserror::Error;

#[derive(Error, Debug)]
pub enum DatabaseError {
    #[error("ClickHouse error: {0}")]
    ClickHouseError(#[from] clickhouse::error::Error),
    
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    #[error("Unsupported database type: {0}")]
    UnsupportedType(String),
    
    #[error("Database operation error: {0}")]
    OperationError(String),
    
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    #[error("Table not found: {0}")]
    TableNotFound(String),
    
    #[error("Connection error: {0}")]
    ConnectionError(String),
    
    #[error("Query error: {0}")]
    QueryError(String),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("UUID error: {0}")]
    UuidError(#[from] uuid::Error),
}

pub type Result<T> = std::result::Result<T, DatabaseError>;
