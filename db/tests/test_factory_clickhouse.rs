use db::config::{ClickHouseConfig, DatabaseConfig};
use db::{create_database, DatabaseFactory};
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// 生成唯一的job_id用于测试隔离
fn generate_unique_job_id(prefix: &str) -> String {
    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}_{}", prefix, std::process::id(), count)
}

/// 设置ClickHouse测试配置
fn setup_clickhouse_config(job_id: &str) -> DatabaseConfig {
    DatabaseConfig {
        db_type: "clickhouse".to_string(),
        enabled: true,
        batch_size: 200000,
        job_id: job_id.to_string(),
        clickhouse: Some(ClickHouseConfig {
            dsn: "http://10.131.9.20:8123".to_string(),
            dial_timeout: 10,
            read_timeout: 30,
            database: Some("default".to_string()),
            username: Some("default".to_string()),
            password: None,
        }),
        sqlite: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试获取支持的数据库类型
    #[tokio::test]
    async fn test_get_supported_types() {
        let types = DatabaseFactory::get_supported_types();
        assert!(!types.is_empty(), "No database types registered");
        assert!(types.contains(&"clickhouse".to_string()));
        assert!(types.contains(&"sqlite".to_string()));
    }

    /// 测试创建ClickHouse数据库
    #[tokio::test]
    async fn test_create_clickhouse_database() {
        let job_id = generate_unique_job_id("factory_clickhouse");
        let config = setup_clickhouse_config(&job_id);

        let result = DatabaseFactory::create_database(&config);
        assert!(result.is_ok(), "Failed to create ClickHouse database");

        let db = result.unwrap();
        let init_result = db.initialize().await;
        assert!(
            init_result.is_ok(),
            "Failed to initialize database: {:?}",
            init_result
        );

        // 清理
        let _ = db.close().await;
    }

    /// 测试创建禁用状态的数据库
    #[tokio::test]
    async fn test_create_disabled_database() {
        let job_id = generate_unique_job_id("factory_disabled");
        let mut config = setup_clickhouse_config(&job_id);
        config.enabled = false;

        let result = DatabaseFactory::create_database(&config);
        assert!(result.is_err(), "Should fail for disabled database");
        assert!(matches!(
            result,
            Err(db::error::DatabaseError::ConfigError(_))
        ));
    }

    /// 测试不支持的类型
    #[tokio::test]
    async fn test_unsupported_database_type() {
        let job_id = generate_unique_job_id("factory_unsupported");
        let config = DatabaseConfig {
            db_type: "unsupported".to_string(),
            enabled: true,
            batch_size: 200000,
            job_id: job_id.to_string(),
            clickhouse: None,
            sqlite: None,
        };

        let result = DatabaseFactory::create_database(&config);
        assert!(result.is_err(), "Should fail for unsupported type");
        assert!(matches!(
            result,
            Err(db::error::DatabaseError::UnsupportedType(_))
        ));
    }

    /// 测试缺失配置的情况
    #[tokio::test]
    async fn test_missing_clickhouse_config() {
        let job_id = generate_unique_job_id("factory_missing_config");
        let config = DatabaseConfig {
            db_type: "clickhouse".to_string(),
            enabled: true,
            batch_size: 200000,
            job_id: job_id.to_string(),
            clickhouse: None,
            sqlite: None,
        };

        let result = DatabaseFactory::create_database(&config);
        assert!(result.is_err(), "Should fail for missing ClickHouse config");
        assert!(matches!(
            result,
            Err(db::error::DatabaseError::ConfigError(_))
        ));
    }

    /// 测试完整的工厂创建流程
    #[tokio::test]
    async fn test_complete_factory_workflow() {
        let job_id = generate_unique_job_id("factory_workflow");

        // 创建配置
        let config = DatabaseConfig {
            db_type: "clickhouse".to_string(),
            enabled: true,
            batch_size: 200000,
            job_id: job_id.clone(),
            clickhouse: Some(ClickHouseConfig {
                dsn: "http://10.131.9.20:8123".to_string(),
                dial_timeout: 10,
                read_timeout: 30,
                database: Some("default".to_string()),
                username: Some("default".to_string()),
                password: None,
            }),
            sqlite: None,
        };

        // 使用工厂函数创建数据库
        let db = create_database(&config).expect("Failed to create database via factory function");

        // 初始化数据库
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // 测试数据库基本操作
        let ping_result = db.ping().await;
        assert!(
            ping_result.is_ok(),
            "Failed to ping database: {:?}",
            ping_result
        );

        // 清理
        db.close().await.expect("Failed to close database");
    }
}
