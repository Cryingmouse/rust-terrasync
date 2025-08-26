#[cfg(test)]
mod tests {
    use db::Database;
    use db::clickhouse::ClickHouseDatabase;
    use db::config::ClickHouseConfig;
    use db::traits::FileScanRecord;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    // 注意：这些测试需要实际的ClickHouse服务器运行
    // 在CI环境中可能需要跳过或使用mock

    // 使用原子计数器确保每个测试用例都有唯一的job_id
    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    fn generate_unique_job_id(prefix: &str) -> String {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{}_{}_{}", prefix, counter, timestamp)
    }

    fn setup_test_db_with_job_id(job_id: &str) -> ClickHouseDatabase {
        let config = ClickHouseConfig {
            dsn: "http://10.131.9.20:8123".to_string(),
            dial_timeout: 10,
            read_timeout: 30,
            database: "default".to_string(),
            username: "default".to_string(),
            password: None,
        };

        ClickHouseDatabase::new(config, job_id.to_string())
    }

    // 测试清理辅助函数
    async fn cleanup_test_tables(
        db: &ClickHouseDatabase, job_id: &str,
    ) -> Result<(), db::error::DatabaseError> {
        // 清理该测试用例创建的所有表
        let base_table = format!("scan_base_{}", job_id);
        let state_table = format!("scan_state_{}", job_id);

        let _ = db.drop_table_by_name(&base_table).await;
        let _ = db.drop_table_by_name(&state_table).await;

        // 清理临时表（如果有）
        let _ = db
            .drop_tables_with_prefix(&format!("scan_temp_{}", job_id))
            .await;

        // 清理所有以temp_files_开头的临时表
        let _ = db.drop_tables_with_prefix("temp_files_").await;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_scan_base_table() {
        let job_id = generate_unique_job_id("test_base");
        let db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        let result = db.create_scan_base_table().await;
        assert!(
            result.is_ok(),
            "Failed to create scan base table: {:?}",
            result
        );

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_create_scan_state_table() {
        let job_id = generate_unique_job_id("test_state");
        let db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        let result = db.create_scan_state_table().await;
        assert!(
            result.is_ok(),
            "Failed to create scan state table: {:?}",
            result
        );

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_create_scan_temporary_table() {
        let job_id = generate_unique_job_id("test_temp");
        let mut db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        let result = db.create_scan_temporary_table().await;
        assert!(
            result.is_ok(),
            "Failed to create scan temporary table: {:?}",
            result
        );

        let temp_name = db
            .get_scan_temp_table_name()
            .expect("Should have temporary table name");
        assert!(temp_name.starts_with("scan_temp_"));
        assert!(db.get_scan_temp_table_name().is_some());
        assert_eq!(db.get_scan_temp_table_name().unwrap(), temp_name);

        // 测试结束后清理临时表
        let _ = db.drop_scan_temporary_table().await;
    }

    #[tokio::test]
    async fn test_create_all_tables_individually() {
        let job_id = generate_unique_job_id("test_all");
        let db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        let result = db.create_scan_base_table().await;
        assert!(
            result.is_ok(),
            "Failed to create scan base table: {:?}",
            result
        );

        let result = db.create_scan_state_table().await;
        assert!(
            result.is_ok(),
            "Failed to create scan state table: {:?}",
            result
        );

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_drop_scan_temporary_table() {
        let job_id = generate_unique_job_id("test_drop_temp");
        let mut db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        // 先创建临时表
        let _ = db
            .create_scan_temporary_table()
            .await
            .expect("Failed to create temporary table");

        // 验证临时表名已设置
        assert!(db.get_scan_temp_table_name().is_some());

        // 删除临时表
        let result = db.drop_scan_temporary_table().await;
        assert!(
            result.is_ok(),
            "Failed to drop scan temporary table: {:?}",
            result
        );

        // 验证临时表名已清除
        assert!(db.get_scan_temp_table_name().is_none());
    }

    #[tokio::test]
    async fn test_drop_table_by_name() {
        let job_id = generate_unique_job_id("test_drop");
        let db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        let table_name = format!("test_drop_table_{}", job_id);

        // 先创建测试表
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (id UInt64, name String) ENGINE = MergeTree() ORDER BY id",
            table_name
        );
        db.execute(&create_sql, &[])
            .await
            .expect("Failed to create test table");

        // 再删除表
        let result = db.drop_table_by_name(&table_name).await;
        assert!(result.is_ok(), "Failed to drop table by name: {:?}", result);
    }

    #[tokio::test]
    async fn test_drop_tables_with_prefix() {
        let job_id = generate_unique_job_id("test_prefix");
        let db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        let prefix = format!("test_prefix_{}_", job_id);
        let table1 = format!("{}table1", prefix);
        let table2 = format!("{}table2", prefix);

        // 先创建测试表
        let create_sql1 = format!(
            "CREATE TABLE IF NOT EXISTS {} (id UInt64) ENGINE = MergeTree() ORDER BY id",
            table1
        );
        let create_sql2 = format!(
            "CREATE TABLE IF NOT EXISTS {} (name String) ENGINE = MergeTree() ORDER BY name",
            table2
        );

        db.execute(&create_sql1, &[])
            .await
            .expect("Failed to create test table1");
        db.execute(&create_sql2, &[])
            .await
            .expect("Failed to create test table2");

        // 删除前缀表
        let result = db.drop_tables_with_prefix(&prefix).await;
        assert!(
            result.is_ok(),
            "Failed to drop tables with prefix: {:?}",
            result
        );

        let dropped_tables = result.unwrap();
        assert!(dropped_tables.contains(&table1));
        assert!(dropped_tables.contains(&table2));
        assert_eq!(dropped_tables.len(), 2);
    }

    #[tokio::test]
    async fn test_query_scan_state_table() {
        let job_id = generate_unique_job_id("test_query_state");
        let db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        // 创建新的状态表（使用唯一job_id，无需清理其他表）
        db.create_scan_state_table()
            .await
            .expect("Failed to create scan state table");

        // 测试查询空表 - 应该返回错误
        let result = db.query_scan_state_table().await;
        assert!(result.is_err(), "Query should fail for empty table");

        // 使用traits定义的接口插入状态数据
        let result = db.insert_scan_state_sync(0).await;
        assert!(result.is_ok(), "Failed to insert test data");

        // 验证插入的数据
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let result = db.query_scan_state_table().await;
        assert!(result.is_ok(), "Query should succeed after data insertion");

        let state = result.unwrap();
        assert_eq!(state, 0, "Should return the inserted state value");

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_query_scan_base_table() {
        let job_id = generate_unique_job_id("test_query_base");
        let db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        // 创建新的基础表（使用唯一job_id，无需清理其他表）
        db.create_scan_base_table()
            .await
            .expect("Failed to create scan base table");

        // 测试查询空表
        let result = db.query_scan_base_table(&[]).await;
        assert!(result.is_ok(), "Query should succeed even for empty table");

        // 测试查询指定列
        let result = db.query_scan_base_table(&["path", "size"]).await;
        assert!(result.is_ok(), "Query with specific columns should succeed");

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_batch_insert_temp_table() {
        let job_id = generate_unique_job_id("test_batch_temp");
        let mut db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        // 创建新的临时表（使用唯一job_id，无需清理其他表）
        db.create_scan_temporary_table()
            .await
            .expect("Failed to create scan temporary table");

        // 准备测试数据 - 使用Unix时间戳
        let base_time = 1609459200; // 2021-01-01 00:00:00 UTC
        let test_records = vec![
            FileScanRecord {
                path: "/test/path/file1.txt".to_string(),
                size: 1024,
                ext: Some("txt".to_string()),
                ctime: base_time,
                mtime: base_time,
                atime: base_time,
                perm: Some(String::from("rw-r--r--")),
                is_symlink: false,
                is_dir: false,
                is_regular_file: true,
                current_state: 1,
                hard_links: 1,
            },
            FileScanRecord {
                path: "/test/path/file2.jpg".to_string(),
                size: 2048,
                ext: Some("jpg".to_string()),
                ctime: base_time,
                mtime: base_time,
                atime: base_time,
                perm: Some(String::from("rw-r--r--")),
                is_symlink: false,
                is_dir: false,
                is_regular_file: true,
                current_state: 1,
                hard_links: 2,
            },
        ];

        // 测试批量插入 - 使用trait接口
        let result = db.batch_insert_temp_record_sync(test_records.clone()).await;
        assert!(
            result.is_ok(),
            "Failed to batch insert temp records: {:?}",
            result
        );

        println!(
            "Successfully inserted {} records to temporary table",
            test_records.len()
        );

        // 测试结束后清理
        let _ = db.drop_scan_temporary_table().await;
    }

    #[tokio::test]
    async fn test_batch_insert_empty_temp_table() {
        let job_id = generate_unique_job_id("test_empty_batch");
        let mut db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        // 创建新的临时表（使用唯一job_id，无需清理其他表）
        db.create_scan_temporary_table()
            .await
            .expect("Failed to create scan temporary table");

        // 测试空数据批量插入
        let empty_records: Vec<FileScanRecord> = vec![];
        let result = db.batch_insert_temp_record_sync(empty_records).await;
        assert!(
            result.is_ok(),
            "Empty batch insert should succeed: {:?}",
            result
        );

        println!("Empty batch insert test passed");

        // 测试结束后清理
        let _ = db.drop_scan_temporary_table().await;
    }

    #[tokio::test]
    async fn test_batch_insert_large_temp_table() {
        let job_id = generate_unique_job_id("test_large_temp");
        let mut db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        // 创建新的临时表
        db.create_scan_temporary_table()
            .await
            .expect("Failed to create scan temporary table");

        // 生成中等量测试数据
        let base_time = 1609459200; // 2021-01-01 00:00:00 UTC
        let mut test_records = Vec::new();
        for i in 0..5 {
            test_records.push(FileScanRecord {
                path: format!("/test/path/file_{}.txt", i),
                size: i * 1024,
                ext: Some("txt".to_string()),
                ctime: base_time,
                mtime: base_time,
                atime: base_time,
                perm: Some(String::from("rw-r--r--")),
                hard_links: 1,
                is_symlink: false,
                is_dir: false,
                is_regular_file: true,
                current_state: 1,
            });
        }

        // 测试批量插入 - 使用trait接口
        let result = db.batch_insert_temp_record_sync(test_records.clone()).await;
        assert!(
            result.is_ok(),
            "Failed to batch insert large temp records: {:?}",
            result
        );

        println!(
            "Successfully inserted {} records to temporary table",
            test_records.len()
        );

        // 测试结束后清理
        let _ = db.drop_scan_temporary_table().await;
    }

    #[tokio::test]
    async fn test_insert_scan_state_sync() {
        let job_id = generate_unique_job_id("test_insert_state");
        let db = setup_test_db_with_job_id(&job_id);

        if db.ping().await.is_err() {
            println!("ClickHouse server not available, skipping test");
            return;
        }

        // 创建新的scan_state表（使用唯一job_id，无需清理其他表）
        db.create_scan_state_table()
            .await
            .expect("Failed to create scan state table");

        // 使用execute方法插入状态数据
        let insert_sql = format!(
            "INSERT INTO scan_state_{} (id, origin_state) VALUES (?, ?)",
            job_id
        );

        let result = db
            .execute(
                &insert_sql,
                &[
                    serde_json::Value::Number(1.into()),
                    serde_json::Value::Number(5.into()),
                ],
            )
            .await;

        assert!(result.is_ok(), "Failed to insert scan state: {:?}", result);

        println!("Successfully inserted scan state");

        // 验证数据插入成功
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let query_sql = format!(
            "SELECT origin_state FROM scan_state_{} WHERE id = 1",
            job_id
        );

        let result = db.execute(&query_sql, &[]).await;

        assert!(
            result.is_ok(),
            "Failed to query inserted state: {:?}",
            result
        );
        println!("Successfully verified scan state insertion");

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }
}
