#[cfg(test)]
mod tests {
    use db::clickhouse::{ClickHouseDatabase, FileScanRecord};
    use db::config::ClickHouseConfig;
    use db::Database;

    // 注意：这些测试需要实际的ClickHouse服务器运行
    // 在CI环境中可能需要跳过或使用mock

    fn setup_test_db() -> ClickHouseDatabase {
        let config = ClickHouseConfig {
            dsn: "http://10.131.9.20:8123".to_string(),
            dial_timeout: 10,
            read_timeout: 30,
            database: Some("default".to_string()),
            username: Some("default".to_string()),
            password: None,
        };

        ClickHouseDatabase::new(config, "test_job".to_string())
    }

    #[tokio::test]
    async fn test_create_scan_base_table() {
        let db = setup_test_db();
        db.initialize()
            .await
            .expect("Failed to initialize database");

        let result = db.create_scan_base_table().await;
        assert!(
            result.is_ok(),
            "Failed to create scan base table: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_create_scan_state_table() {
        let db = setup_test_db();
        db.initialize()
            .await
            .expect("Failed to initialize database");

        let result = db.create_scan_state_table().await;
        assert!(
            result.is_ok(),
            "Failed to create scan state table: {:?}",
            result
        );
    }

    #[tokio::test]
    async fn test_create_scan_temporary_table() {
        let mut db = setup_test_db();
        db.initialize()
            .await
            .expect("Failed to initialize database");

        let result = db.create_scan_temporary_table().await;
        assert!(
            result.is_ok(),
            "Failed to create scan temporary table: {:?}",
            result
        );

        let temp_name = db.get_scan_temp_table_name().expect("Should have temporary table name");
        assert!(temp_name.starts_with("temp_files_"));
        assert!(db.get_scan_temp_table_name().is_some());
        assert_eq!(db.get_scan_temp_table_name().unwrap(), temp_name);
    }

    #[tokio::test]
    async fn test_create_all_tables_individually() {
        let db = setup_test_db();
        db.initialize()
            .await
            .expect("Failed to initialize database");

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
    }

    #[tokio::test]
    async fn test_drop_scan_temporary_table() {
        let mut db = setup_test_db();
        db.initialize()
            .await
            .expect("Failed to initialize database");

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
        let db = setup_test_db();
        db.initialize()
            .await
            .expect("Failed to initialize database");

        let table_name = "test_drop_table";

        // 先创建测试表
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (id UInt64, name String) ENGINE = MergeTree() ORDER BY id",
            table_name
        );
        db.execute(&create_sql, &[])
            .await
            .expect("Failed to create test table");

        // 再删除表
        let result = db.drop_table_by_name(table_name).await;
        assert!(result.is_ok(), "Failed to drop table by name: {:?}", result);
    }

    #[tokio::test]
    async fn test_drop_tables_with_prefix() {
        let db = setup_test_db();
        db.initialize()
            .await
            .expect("Failed to initialize database");

        let prefix = "test_prefix_";
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
        let result = db.drop_tables_with_prefix(prefix).await;
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
        let db = setup_test_db();
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // Clean up and create fresh table
        let _ = db.drop_tables_with_prefix("scan_state_").await;
        db.create_scan_state_table().await.expect("Failed to create scan state table");

        // 测试查询空表的情况 - 应该返回空结果而不是错误
        let result = db.query_scan_state_table().await;
        assert!(result.is_ok(), "Query should succeed even for empty table");
        // 不检查具体行数，只验证查询成功
    }

    #[tokio::test]
    async fn test_query_scan_base_table() {
        let db = setup_test_db();
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // Clean up and create fresh table
        let _ = db.drop_tables_with_prefix("scan_base_").await;
        db.create_scan_base_table().await.expect("Failed to create scan base table");

        // 测试查询空表
        let result = db.query_scan_base_table(&[]).await;
        assert!(result.is_ok(), "Query should succeed even for empty table");
        // 不检查具体行数，只验证查询成功

        // 测试查询指定列
        let result = db.query_scan_base_table(&["path", "size"]).await;
        assert!(result.is_ok(), "Query with specific columns should succeed");
    }

    #[tokio::test]
    async fn test_insert_file_record_async() {
        let db = setup_test_db();
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // Clean up any existing tables and create fresh ones
        let _ = db.drop_tables_with_prefix("scan_base_").await;
        db.create_scan_base_table().await.expect("Failed to create scan base table");

        let test_record = FileScanRecord {
            path: "/test/path/file.txt".to_string(),
            size: 1024,
            ext: "txt".to_string(),
            ctime: 1234567890,  // 使用i64类型的时间戳
            mtime: 1234567890,
            atime: 1234567890,
            perm: 0o644,
            is_symlink: false,
            is_dir: false,
            is_regular_file: true,
            dir_handle: "handle123".to_string(),
            current_state: 1,
        };

        let result = db.insert_file_record_async(test_record.clone()).await;
        if result.is_err() {
            eprintln!("Insert failed: {:?}", result);
        }
        assert!(result.is_ok(), "Failed to insert file record async");

        // 使用更合理的等待策略
        let mut attempts = 0;
        let max_attempts = 50; // 增加到50次，最多5秒
        
        loop {
            attempts += 1;
            
            // 查询完整记录，确保所有字段都能正确反序列化
            let records = db.query_scan_base_table(&[
                "path", "size", "ext", "ctime", "mtime", "atime", 
                "perm", "is_symlink", "is_dir", "is_regular_file", 
                "dir_handle", "current_state"
            ]).await;
            
            match records {
                Ok(records) => {
                    if !records.is_empty() {
                        let found = records.iter().find(|r| r.path == test_record.path);
                        if let Some(found_record) = found {
                            // 验证所有字段都匹配
                            assert_eq!(found_record.size, test_record.size);
                            assert_eq!(found_record.ext, test_record.ext);
                            assert_eq!(found_record.ctime, test_record.ctime);
                            assert_eq!(found_record.mtime, test_record.mtime);
                            assert_eq!(found_record.atime, test_record.atime);
                            assert_eq!(found_record.perm, test_record.perm);
                            assert_eq!(found_record.is_symlink, test_record.is_symlink);
                            assert_eq!(found_record.is_dir, test_record.is_dir);
                            assert_eq!(found_record.is_regular_file, test_record.is_regular_file);
                            assert_eq!(found_record.dir_handle, test_record.dir_handle);
                            assert_eq!(found_record.current_state, test_record.current_state);
                            
                            println!("Async insert completed successfully after {} attempts", attempts);
                            break;
                        }
                    }
                }
                Err(e) => {
                    println!("Query error on attempt {}: {}", attempts, e);
                }
            }
            
            if attempts >= max_attempts {
                panic!("Timeout waiting for async insert to complete after {} attempts ({}ms)", 
                       attempts, attempts * 100);
            }
            
            println!("Waiting for async insert flush... attempt {}", attempts);
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    }
}
