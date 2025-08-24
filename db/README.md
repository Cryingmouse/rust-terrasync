# Database Factory

A flexible database factory implementation that supports multiple database backends with a unified interface.

## Features

- **Unified Interface**: All database operations use the same API regardless of the backend
- **Extensible**: Easy to add new database types
- **Type-safe**: Strongly typed configuration for each database
- **Async/Await**: Built for async Rust applications
- **Factory Pattern**: Clean separation between creation and usage

## Supported Databases

- **SQLite**: Embedded database with file-based storage
- **ClickHouse**: Column-oriented database for analytical workloads

## Usage

### 1. Initialize the Factory

```rust
use db::init;

// Initialize the database factory with built-in types
init().unwrap();
```

### 2. Create Database Configuration

#### SQLite Configuration

```rust
use db::{DatabaseConfig, SQLiteConfig};

let config = DatabaseConfig {
    enabled: true,
    db_type: "sqlite".to_string(),
    batch_size: 100,
    clickhouse: None,
    sqlite: Some(SQLiteConfig {
        path: "data/app.db".to_string(),
        busy_timeout: 5000,
        journal_mode: Some("WAL".to_string()),
        synchronous: Some("NORMAL".to_string()),
        cache_size: Some(10000),
    }),
};
```

#### ClickHouse Configuration

```rust
use db::{DatabaseConfig, ClickHouseConfig};

let config = DatabaseConfig {
    enabled: true,
    db_type: "clickhouse".to_string(),
    batch_size: 1000,
    clickhouse: Some(ClickHouseConfig {
        dsn: "tcp://localhost:9000".to_string(),
        dial_timeout: 10,
        read_timeout: 30,
        database: Some("default".to_string()),
        username: Some("default".to_string()),
        password: Some("password".to_string()),
    }),
    sqlite: None,
};
```

### 3. Create Database Instance

```rust
use db::create_database;

let database = create_database(&config)?;
database.initialize().await?;
```

### 4. Use the Database

All database operations use the same unified interface:

```rust
use db::{TableSchema, ColumnInfo};
use serde_json::json;

// Create a table
let schema = TableSchema {
    name: "users".to_string(),
    columns: vec![
        ColumnInfo {
            name: "id".to_string(),
            data_type: "INTEGER".to_string(),
            nullable: false,
            default_value: None,
            is_primary_key: true,
        },
        ColumnInfo {
            name: "name".to_string(),
            data_type: "TEXT".to_string(),
            nullable: false,
            default_value: None,
            is_primary_key: false,
        },
    ],
};

database.create_table(&schema).await?;

// Insert data
let sql = "INSERT INTO users (name) VALUES (?)";
let result = database.execute(sql, &[json!("Alice")]).await?;

// Query data
let sql = "SELECT * FROM users";
let query_result = database.query(sql, &[]).await?;

// Batch operations
let sql = "INSERT INTO users (name) VALUES (?)";
let batch_params = vec![
    vec![json!("Bob")],
    vec![json!("Charlie")],
];
let results = database.execute_batch(sql, &batch_params).await?;
```

### 5. Database Manager for Multiple Databases

```rust
use db::DatabaseManager;

let mut manager = DatabaseManager::new();

// Add multiple databases
manager.add_database("sqlite_main".to_string(), sqlite_db)?;
manager.add_database("clickhouse_analytics".to_string(), clickhouse_db)?;

// Initialize all
manager.initialize_all().await?;

// Get specific database
if let Some(db) = manager.get_database_mut("sqlite_main") {
    // Use the database
}

// Close all
manager.close_all().await?;
```

## Adding New Database Types

To add a new database type, implement the `Database` trait and register it:

```rust
use async_trait::async_trait;
use db::{Database, DatabaseConfig, DatabaseFactory, Result};

pub struct MyCustomDatabase {
    // Your implementation
}

#[async_trait]
impl Database for MyCustomDatabase {
    async fn initialize(&self) -> Result<()> {
        // Implementation
    }
    
    // Implement all required methods...
    
    fn database_type(&self) -> &'static str {
        "custom"
    }
}

// Register the new type
DatabaseFactory::register_database_type("custom", |config| {
    Ok(Box::new(MyCustomDatabase::new(config)?))
})?;
```

## Testing

Run the tests:

```bash
cargo test
```

Run the example:

```bash
cargo run --example basic_usage
```

## Error Handling

All operations return `db::Result<T>` which wraps the custom `DatabaseError` type:

- `ConnectionError`: Database connection issues
- `QueryError`: SQL query execution errors
- `ConfigError`: Configuration validation errors
- `UnsupportedType`: When trying to use an unregistered database type
- `DatabaseNotFound`: When a requested database doesn't exist

## Configuration

Each database type has its own specific configuration structure:

- **SQLite**: File path, timeout settings, journal mode, etc.
- **ClickHouse**: Connection string, timeouts, authentication, etc.

The factory automatically selects the appropriate configuration based on the `db_type` field.