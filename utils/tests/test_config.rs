use utils::app_config::*;

pub fn initialize() {
    // Reset to original test configuration
    let config_contents = include_str!("resources/test_config.toml");
    AppConfig::init(Some(config_contents)).unwrap();
}

#[test]
fn fetch_config() {
    initialize();

    // Fetch an instance of Config
    let config = AppConfig::fetch().unwrap();

    // Test all log configuration items
    assert_eq!(config.log.max_size, 100);
    assert_eq!(config.log.max_backups, 10);
    assert_eq!(config.log.level, "info");

    // Test all scan configuration items
    assert_eq!(config.scan.concurrency, 5);

    // Test all migrate configuration items
    assert_eq!(config.migrate.overwrite, false);
    assert_eq!(config.migrate.concurrency, 1);

    // Test all database configuration items
    assert_eq!(config.database.enabled, true);
    assert_eq!(config.database.r#type, "sqlite");
    assert_eq!(config.database.batch_size, 10000);

    // Test all database.clickhouse configuration items
    assert_eq!(config.database.clickhouse.dsn, "tcp://localhost:9000");
    assert_eq!(config.database.clickhouse.dial_timeout, 5);
    assert_eq!(config.database.clickhouse.read_timeout, 30);

    // Test all kafka configuration items
    assert_eq!(config.kafka.enabled, false);
    assert_eq!(config.kafka.host, "localhost");
    assert_eq!(config.kafka.port, 9092);
    assert_eq!(config.kafka.topic, "test");
    assert_eq!(config.kafka.concurrency, 5);
}

#[test]
fn verify_get() {
    initialize();

    // Test getting all log configuration items via get
    assert_eq!(AppConfig::get::<u64>("log.max_size").unwrap(), 100);
    assert_eq!(AppConfig::get::<u8>("log.max_backups").unwrap(), 10);
    assert_eq!(AppConfig::get::<String>("log.level").unwrap(), "info");

    // Test getting all scan configuration items via get
    assert_eq!(AppConfig::get::<u32>("scan.concurrency").unwrap(), 5);

    // Test getting all migrate configuration items via get
    assert_eq!(AppConfig::get::<bool>("migrate.overwrite").unwrap(), false);
    assert_eq!(AppConfig::get::<u32>("migrate.concurrency").unwrap(), 1);

    // Test getting all database configuration items via get
    assert_eq!(AppConfig::get::<bool>("database.enabled").unwrap(), true);
    assert_eq!(AppConfig::get::<String>("database.type").unwrap(), "sqlite");
    assert_eq!(AppConfig::get::<u32>("database.batch_size").unwrap(), 10000);

    // Test getting all database.clickhouse configuration items via get
    assert_eq!(
        AppConfig::get::<String>("database.clickhouse.dsn").unwrap(),
        "tcp://localhost:9000"
    );
    assert_eq!(
        AppConfig::get::<u64>("database.clickhouse.dial_timeout").unwrap(),
        5
    );
    assert_eq!(
        AppConfig::get::<u64>("database.clickhouse.read_timeout").unwrap(),
        30
    );

    // Test getting all kafka configuration items via get
    assert_eq!(AppConfig::get::<bool>("kafka.enabled").unwrap(), false);
    assert_eq!(AppConfig::get::<String>("kafka.host").unwrap(), "localhost");
    assert_eq!(AppConfig::get::<u16>("kafka.port").unwrap(), 9092);
    assert_eq!(AppConfig::get::<String>("kafka.topic").unwrap(), "test");
    assert_eq!(AppConfig::get::<u32>("kafka.concurrency").unwrap(), 5);
}

#[test]
fn verify_set() {
    initialize();

    // Test setting various configuration items
    AppConfig::set("log.level", "debug").unwrap();
    AppConfig::set("scan.concurrency", "10").unwrap();
    AppConfig::set("migrate.overwrite", "true").unwrap();
    AppConfig::set("database.batch_size", "50000").unwrap();
    AppConfig::set("kafka.concurrency", "50").unwrap();

    // Fetch a new instance of Config
    let config = AppConfig::fetch().unwrap();

    // Check all values were modified
    assert_eq!(config.log.level, "debug");
    assert_eq!(config.scan.concurrency, 10);
    assert_eq!(config.migrate.overwrite, true);
    assert_eq!(config.database.batch_size, 50000);
    assert_eq!(config.kafka.concurrency, 50);
}

#[test]
fn test_config_validation() {
    initialize();

    let config = AppConfig::fetch().unwrap();

    // Validate all configuration values are within expected ranges
    assert!(config.log.max_size > 0, "Log max_size should be positive");
    assert!(
        config.log.max_backups > 0,
        "Log max_backups should be positive"
    );
    assert!(
        config.scan.concurrency > 0,
        "Scan concurrency should be positive"
    );
    assert!(
        config.migrate.concurrency > 0,
        "Migrate concurrency should be positive"
    );
    assert!(
        config.database.batch_size > 0,
        "Database batch_size should be positive"
    );
    assert!(config.kafka.port > 0, "Kafka port should be positive");
    assert!(
        config.kafka.concurrency > 0,
        "Kafka concurrency should be positive"
    );
}

#[test]
fn test_nested_configuration_access() {
    initialize();

    // Test accessing nested configuration structures
    let log_config = AppConfig::get::<LogConfig>("log").unwrap();
    assert_eq!(log_config.level, "info");
    assert_eq!(log_config.max_size, 100);
    assert_eq!(log_config.max_backups, 10);

    let db_config = AppConfig::get::<DatabaseConfig>("database").unwrap();
    assert_eq!(db_config.enabled, true);
    assert_eq!(db_config.r#type, "sqlite");
    assert_eq!(db_config.batch_size, 10000);

    let clickhouse_config = AppConfig::get::<DatabaseClickhouse>("database.clickhouse").unwrap();
    assert_eq!(clickhouse_config.dsn, "tcp://localhost:9000");
    assert_eq!(clickhouse_config.dial_timeout, 5);
    assert_eq!(clickhouse_config.read_timeout, 30);

    let kafka_config = AppConfig::get::<KafkaConfig>("kafka").unwrap();
    assert_eq!(kafka_config.enabled, false);
    assert_eq!(kafka_config.host, "localhost");
    assert_eq!(kafka_config.port, 9092);
    assert_eq!(kafka_config.topic, "test");
    assert_eq!(kafka_config.concurrency, 5);
}

#[test]
fn test_string_configuration_values() {
    initialize();

    // Test string-based configuration values
    assert_eq!(AppConfig::get::<String>("log.level").unwrap(), "info");
    assert_eq!(AppConfig::get::<String>("database.type").unwrap(), "sqlite");
    assert_eq!(
        AppConfig::get::<String>("database.clickhouse.dsn").unwrap(),
        "tcp://localhost:9000"
    );
    assert_eq!(AppConfig::get::<String>("kafka.host").unwrap(), "localhost");
    assert_eq!(AppConfig::get::<String>("kafka.topic").unwrap(), "test");
}

#[test]
fn test_configuration_types() {
    initialize();

    // Test that configuration values have correct types
    let max_size: u64 = AppConfig::get("log.max_size").unwrap();
    assert_eq!(max_size, 100);

    let max_backups: u8 = AppConfig::get("log.max_backups").unwrap();
    assert_eq!(max_backups, 10);

    let concurrency: u32 = AppConfig::get("scan.concurrency").unwrap();
    assert_eq!(concurrency, 5);

    let overwrite: bool = AppConfig::get("migrate.overwrite").unwrap();
    assert_eq!(overwrite, false);

    let enabled: bool = AppConfig::get("database.enabled").unwrap();
    assert_eq!(enabled, true);

    let port: u16 = AppConfig::get("kafka.port").unwrap();
    assert_eq!(port, 9092);
}
