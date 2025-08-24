# Database Configuration Setup Guide

This guide explains how to configure and use different database backends in the application.

## Supported Database Types

1. **SQLite** - Lightweight, file-based database
2. **ClickHouse** - Column-oriented database for analytics (currently disabled in factory)

## Configuration Methods

### Method 1: Configuration File

Create a `config.toml` file in your project root:

```toml
[database]
enabled = true
type = "sqlite"  # or "clickhouse"
batch_size = 1000

[database.sqlite]
path = "data/app.db"
busy_timeout = 5000
journal_mode = "WAL"
synchronous = "NORMAL"
cache_size = 10000

[database.clickhouse]
dsn = "tcp://localhost:9000"
dial_timeout = 5
read_timeout = 30
database = "default"
username = "default"
password = ""
```

### Method 2: Programmatic Setup

```rust
use app::consumer::db::{create_db_manager, presets};

// Load from config file
let manager = create_db_manager(Some("config.toml")).await?;

// Quick SQLite setup
let sqlite_manager = presets::sqlite_default("data/app.db");
sqlite_manager.initialize().await?;

// Quick ClickHouse setup
let clickhouse_manager = presets::clickhouse_default("tcp://localhost:9000");
clickhouse_manager.initialize().await?;
```

### Method 3: Custom Configuration

```rust
use app::consumer::db::{AppDatabaseManager, AppDatabaseConfig};
use db::config::{SQLiteConfig, ClickHouseConfig};

let config = AppDatabaseConfig {
    enabled: true,
    db_type: "sqlite".to_string(),
    batch_size: 5000,
    sqlite: Some(SQLiteConfig {
        path: "/custom/path/to.db".to_string(),
        busy_timeout: 10000,
        journal_mode: Some("WAL".to_string()),
        synchronous: Some("NORMAL".to_string()),
        cache_size: Some(20000),
    }),
    clickhouse: None,
};

let manager = AppDatabaseManager::new(config);
manager.initialize().await?;
```

## Usage Examples

### Basic Usage

```rust
use app::consumer::db::create_db_manager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_manager = create_db_manager(None).await?;
    
    if db_manager.is_enabled() {
        println!("Using database: {}", db_manager.get_db_type());
        println!("Batch size: {}", db_manager.get_batch_size());
        
        // Use the database...
        let db = db_manager.get_database().await.unwrap();
        // db.query(...)
        
        db_manager.close().await?;
    }
    
    Ok(())
}
```

### Configuration Priority

1. **Config file** (config.toml) - Highest priority
2. **Environment variables** - Can be added later
3. **Default values** - Fallback when no config provided

## Default Values

- **SQLite**: `data/app.db` with WAL journal mode
- **ClickHouse**: `tcp://localhost:9000` with default settings
- **Batch size**: 1000 for SQLite, 200000 for ClickHouse

## Notes

- ClickHouse support is currently disabled in the factory (commented out)
- SQLite is fully functional with WAL mode for better concurrency
- All database connections are properly closed on shutdown
- Thread-safe with async/await support