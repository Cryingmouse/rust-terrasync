use db::config::{ClickHouseConfig, DatabaseConfig};
use db::{DatabaseFactory, create_database};
use std::sync::atomic::{AtomicUsize, Ordering};

static COUNTER: AtomicUsize = AtomicUsize::new(0);

/// 生成唯一的job_id用于测试隔离
fn generate_unique_job_id(prefix: &str) -> String {
    let count = COUNTER.fetch_add(1, Ordering::SeqCst);
    format!("{}_{}_{}", prefix, std::process::id(), count)
}

/// 设置ClickHouse测试配置
fn setup_clickhouse_config() -> DatabaseConfig {
    DatabaseConfig {
        db_type: "clickhouse".to_string(),
        enabled: true,
        batch_size: 200000,
        clickhouse: Some(ClickHouseConfig {
            dsn: "http://10.131.9.20:8123".to_string(),
            dial_timeout: 10,
            read_timeout: 30,
            database: "default".to_string(),
            username: "default".to_string(),
            password: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 测试创建ClickHouse数据库
    #[tokio::test]
    async fn test_create_clickhouse_database() {
        let job_id = generate_unique_job_id("factory_clickhouse");
        let config = setup_clickhouse_config();

        let result = DatabaseFactory::create_database(&config, job_id.clone());
        assert!(result.is_ok(), "Failed to create ClickHouse database");

        let db = result.unwrap();
        let init_result = db.ping().await;
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
        let mut config = setup_clickhouse_config();
        config.enabled = false;

        let result = DatabaseFactory::create_database(&config, job_id.clone());
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
            clickhouse: None,
        };

        let result = DatabaseFactory::create_database(&config, job_id.to_string());
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
            clickhouse: None,
        };

        let result = DatabaseFactory::create_database(&config, job_id.to_string());
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

        // 使用工厂函数创建数据库
        let db = create_database(&setup_clickhouse_config(), job_id.clone())
            .expect("Failed to create database via factory function");

        // 初始化数据库
        db.ping().await.expect("Failed to ping database");

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

    /// 从factory层面验证Database的所有接口
    #[tokio::test]
    async fn test_all_database_interfaces_via_factory() {
        use db::{SCAN_BASE_TABLE_BASE_NAME, SCAN_STATE_TABLE_BASE_NAME};

        let job_id = generate_unique_job_id("factory_all_interfaces");
        let config = setup_clickhouse_config();

        let db = DatabaseFactory::create_database(&config, job_id.clone())
            .expect("Failed to create database");

        // 测试ping
        let ping_result = db.ping().await;
        if ping_result.is_err() {
            println!("ClickHouse server not available, skipping comprehensive test");
            return;
        }
        assert!(ping_result.is_ok(), "Ping should succeed");

        // 验证数据库类型
        assert_eq!(db.database_type(), "clickhouse");

        // 测试表存在检查
        let base_table_name = format!("scan_base_{}", job_id);
        let state_table_name = format!("scan_state_{}", job_id);

        let exists_result = db.table_exists(&base_table_name).await;
        assert!(exists_result.is_ok(), "table_exists should succeed");
        assert!(
            !exists_result.unwrap(),
            "Base table should not exist initially"
        );

        // 测试通用create_table接口 - 创建scan_base表
        let create_result = db.create_table(SCAN_BASE_TABLE_BASE_NAME).await;
        assert!(
            create_result.is_ok(),
            "Generic create_table should succeed for base table"
        );

        // 验证表已创建
        let exists_result = db.table_exists(&base_table_name).await;
        assert!(
            exists_result.unwrap(),
            "Base table should exist after creation"
        );

        // 测试创建scan_state表
        let create_result = db.create_table(SCAN_STATE_TABLE_BASE_NAME).await;
        assert!(
            create_result.is_ok(),
            "Generic create_table should succeed for state table"
        );

        // 测试临时表操作 - 使用trait方法而不是具体实现
        let temp_table_name = db.get_scan_temp_table_name();
        assert!(
            temp_table_name.is_none(),
            "Should not have temp table initially"
        );

        // 测试状态表操作
        let state_query_result = db.query_scan_state_table().await;
        assert!(
            state_query_result.is_err(),
            "Empty state table should return error"
        );

        // 使用通用execute插入状态数据
        let insert_state_sql = format!(
            "INSERT INTO {} (id, origin_state) VALUES (1, 42)",
            state_table_name
        );
        let execute_result = db.execute(&insert_state_sql, &[]).await;
        assert!(
            execute_result.is_ok(),
            "Execute should succeed for state insert"
        );

        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let state_result = db.query_scan_state_table().await;
        assert!(state_result.is_ok(), "Should query state successfully");
        assert_eq!(state_result.unwrap(), 42, "Should return correct state");

        // 测试基础表查询
        let base_query_result = db.query_scan_base_table(&[]).await;
        assert!(base_query_result.is_ok(), "Query base table should succeed");
        assert!(
            base_query_result.unwrap().is_empty(),
            "Empty base table should return empty vec"
        );

        // 测试通用execute接口
        let execute_result = db
            .execute(&format!("SELECT count(*) FROM {}", base_table_name), &[])
            .await;
        assert!(execute_result.is_ok(), "Generic execute should succeed");

        // 测试drop_table接口
        let drop_result = db.drop_table(&format!("test_custom_{}", job_id)).await;
        assert!(drop_result.is_ok(), "Generic drop_table should succeed");

        // 测试关闭连接
        let close_result = db.close().await;
        assert!(close_result.is_ok(), "Close should succeed");

        println!("✅ All Database interfaces verified successfully via factory");
    }
}
