#[cfg(test)]
mod tests {
    use db::config::SQLiteConfig;
    use db::sqlite::SQLiteDatabase;
    use db::Database;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::NamedTempFile;

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

    fn setup_test_db_with_job_id(job_id: &str) -> (SQLiteDatabase, NamedTempFile) {
        // 创建临时SQLite数据库文件
        let temp_file = NamedTempFile::new().expect("Failed to create temp file");
        
        let config = SQLiteConfig {
            path: temp_file.path().to_string_lossy().to_string(),
            busy_timeout: 5000,
            journal_mode: Some("WAL".to_string()),
            synchronous: Some("NORMAL".to_string()),
            cache_size: Some(1000),
        };

        let db = SQLiteDatabase::new(config, job_id.to_string())
            .expect("Failed to create SQLite database");
        
        (db, temp_file)
    }

    // 测试清理辅助函数
    async fn cleanup_test_tables(
        db: &SQLiteDatabase,
        job_id: &str,
    ) -> Result<(), db::error::DatabaseError> {
        // 清理该测试用例创建的所有表
        let base_table = format!("scan_base_{}", job_id);
        let state_table = format!("scan_state_{}", job_id);
        let temp_table = format!("scan_temp_{}", job_id);

        let _ = db.execute(&format!("DROP TABLE IF EXISTS {}", base_table), &[]).await;
        let _ = db.execute(&format!("DROP TABLE IF EXISTS {}", state_table), &[]).await;
        let _ = db.execute(&format!("DROP TABLE IF EXISTS {}", temp_table), &[]).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_create_scan_base_table() {
        let job_id = generate_unique_job_id("test_base");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        let table_name = format!("scan_base_{}", job_id);
        let result = db.create_table(&db::traits::TableSchema {
            name: table_name.clone(),
            columns: vec![
                db::traits::ColumnInfo {
                    name: "path".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                db::traits::ColumnInfo {
                    name: "size".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "ext".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "ctime".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "mtime".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "atime".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "perm".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "is_symlink".to_string(),
                    data_type: "BOOLEAN".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "is_dir".to_string(),
                    data_type: "BOOLEAN".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "is_regular_file".to_string(),
                    data_type: "BOOLEAN".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "file_handle".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "current_state".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
            ],
        }).await;

        assert!(
            result.is_ok(),
            "Failed to create scan base table: {:?}",
            result
        );

        // 验证表已创建
        let exists = db.table_exists(&table_name).await.expect("Failed to check table existence");
        assert!(exists, "Scan base table should exist after creation");

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_create_scan_state_table() {
        let job_id = generate_unique_job_id("test_state");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        let table_name = format!("scan_state_{}", job_id);
        let result = db.create_table(&db::traits::TableSchema {
            name: table_name.clone(),
            columns: vec![
                db::traits::ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                db::traits::ColumnInfo {
                    name: "origin_state".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
            ],
        }).await;

        assert!(
            result.is_ok(),
            "Failed to create scan state table: {:?}",
            result
        );

        // 验证表已创建
        let exists = db.table_exists(&table_name).await.expect("Failed to check table existence");
        assert!(exists, "Scan state table should exist after creation");

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_create_scan_temporary_table() {
        let job_id = generate_unique_job_id("test_temp");
        let (mut db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

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

        // 验证表已创建
        let exists = db.table_exists(temp_name).await.expect("Failed to check table existence");
        assert!(exists, "Scan temporary table should exist after creation");

        // 测试结束后清理
        let _ = db.drop_scan_temporary_table().await;
    }

    #[tokio::test]
    async fn test_create_all_tables_individually() {
        let job_id = generate_unique_job_id("test_all");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // 创建scan_base表
        let base_table_name = format!("scan_base_{}", job_id);
        let result = db.create_table(&db::traits::TableSchema {
            name: base_table_name.clone(),
            columns: vec![
                db::traits::ColumnInfo {
                    name: "path".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                db::traits::ColumnInfo {
                    name: "size".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "current_state".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
            ],
        }).await;
        assert!(result.is_ok(), "Failed to create scan base table: {:?}", result);

        // 创建scan_state表
        let state_table_name = format!("scan_state_{}", job_id);
        let result = db.create_table(&db::traits::TableSchema {
            name: state_table_name.clone(),
            columns: vec![
                db::traits::ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                db::traits::ColumnInfo {
                    name: "origin_state".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
            ],
        }).await;
        assert!(result.is_ok(), "Failed to create scan state table: {:?}", result);

        // 验证两个表都存在
        let base_exists = db.table_exists(&base_table_name).await.expect("Failed to check table existence");
        let state_exists = db.table_exists(&state_table_name).await.expect("Failed to check table existence");
        
        assert!(base_exists, "Scan base table should exist after creation");
        assert!(state_exists, "Scan state table should exist after creation");

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_drop_scan_temporary_table() {
        let job_id = generate_unique_job_id("test_drop_temp");
        let (mut db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
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

        let temp_name = db.get_scan_temp_table_name().unwrap().to_string();

        // 验证表存在
        let exists = db.table_exists(&temp_name).await.expect("Failed to check table existence");
        assert!(exists, "Temporary table should exist before drop");

        // 删除临时表
        let result = db.drop_scan_temporary_table().await;
        assert!(
            result.is_ok(),
            "Failed to drop scan temporary table: {:?}",
            result
        );

        // 验证临时表名仍然存在（drop方法不会清除表名）
        assert!(db.get_scan_temp_table_name().is_some());

        // 验证表已不存在
        let exists = db.table_exists(&temp_name).await.expect("Failed to check table existence");
        assert!(!exists, "Temporary table should not exist after drop");
    }

    #[tokio::test]
    async fn test_drop_table_by_name() {
        let job_id = generate_unique_job_id("test_drop");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        let table_name = format!("test_drop_table_{}", job_id);

        // 先创建测试表
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (id INTEGER PRIMARY KEY, name TEXT)",
            table_name
        );
        db.execute(&create_sql, &[])
            .await
            .expect("Failed to create test table");

        // 验证表存在
        let exists = db.table_exists(&table_name).await.expect("Failed to check table existence");
        assert!(exists, "Test table should exist after creation");

        // 再删除表
        let result = db.execute(&format!("DROP TABLE IF EXISTS {}", table_name), &[]).await;
        assert!(result.is_ok(), "Failed to drop table by name: {:?}", result);

        // 验证表已不存在
        let exists = db.table_exists(&table_name).await.expect("Failed to check table existence");
        assert!(!exists, "Test table should not exist after drop");
    }

    #[tokio::test]
    async fn test_query_scan_state_table() {
        let job_id = generate_unique_job_id("test_query_state");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // 创建新的状态表
        let table_name = format!("scan_state_{}", job_id);
        let _ = db.create_table(&db::traits::TableSchema {
            name: table_name.clone(),
            columns: vec![
                db::traits::ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                db::traits::ColumnInfo {
                    name: "origin_state".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
            ],
        }).await;

        // 插入测试数据
        let insert_sql = format!(
            "INSERT INTO {} (id, origin_state) VALUES (?, ?)",
            table_name
        );
        let _ = db.execute(&insert_sql, &[serde_json::Value::from(1), serde_json::Value::from(5)]).await;

        // 测试查询
        let result = db.query(&format!("SELECT * FROM {}", table_name), &[]).await;
        assert!(result.is_ok(), "Query should succeed");

        let query_result = result.unwrap();
        assert_eq!(query_result.rows.len(), 1, "Should have one row");
        assert_eq!(query_result.rows[0]["id"].as_i64().unwrap_or(0), 1, "ID should be 1");
        assert_eq!(query_result.rows[0]["origin_state"].as_i64().unwrap_or(0), 5, "Origin state should be 5");

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_query_scan_base_table() {
        let job_id = generate_unique_job_id("test_query_base");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // 创建新的基础表
        let table_name = format!("scan_base_{}", job_id);
        let _ = db.create_table(&db::traits::TableSchema {
            name: table_name.clone(),
            columns: vec![
                db::traits::ColumnInfo {
                    name: "path".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                db::traits::ColumnInfo {
                    name: "size".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "ext".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "current_state".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
            ],
        }).await;

        // 插入测试数据
        let insert_sql = format!(
            "INSERT INTO {} (path, size, ext, current_state) VALUES (?, ?, ?, ?)",
            table_name
        );
        let _ = db.execute(&insert_sql, &[
            serde_json::Value::from("/test/path/file.txt"),
            serde_json::Value::from(1024),
            serde_json::Value::from("txt"),
            serde_json::Value::from(1),
        ]).await;

        // 测试查询空表
        let result = db.query(&format!("SELECT * FROM {}", table_name), &[]).await;
        assert!(result.is_ok(), "Query should succeed");

        let query_result = result.unwrap();
        assert_eq!(query_result.rows.len(), 1, "Should have one row");

        // 测试查询指定列
        let result = db.query(&format!("SELECT path, size FROM {}", table_name), &[]).await;
        assert!(result.is_ok(), "Query with specific columns should succeed");

        let query_result = result.unwrap();
        assert_eq!(query_result.rows.len(), 1);
        assert!(query_result.rows[0].contains_key("path"));
        assert!(query_result.rows[0].contains_key("size"));

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_insert_and_query_file_record() {
        let job_id = generate_unique_job_id("test_insert");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // 创建新的基础表
        let table_name = format!("scan_base_{}", job_id);
        let _ = db.create_table(&db::traits::TableSchema {
            name: table_name.clone(),
            columns: vec![
                db::traits::ColumnInfo {
                    name: "path".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                db::traits::ColumnInfo {
                    name: "size".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "ext".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "ctime".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "mtime".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "atime".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "perm".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "is_symlink".to_string(),
                    data_type: "BOOLEAN".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "is_dir".to_string(),
                    data_type: "BOOLEAN".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "is_regular_file".to_string(),
                    data_type: "BOOLEAN".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "file_handle".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
                db::traits::ColumnInfo {
                    name: "current_state".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
            ],
        }).await;

        // 插入测试记录
        let insert_sql = format!(
            "INSERT INTO {} (path, size, ext, ctime, mtime, atime, perm, is_symlink, is_dir, is_regular_file, file_handle, current_state) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            table_name
        );
        
        let test_record = vec![
            serde_json::Value::from("/test/path/file.txt"),
            serde_json::Value::from(1024),
            serde_json::Value::from("txt"),
            serde_json::Value::from(1234567890),
            serde_json::Value::from(1234567890),
            serde_json::Value::from(1234567890),
            serde_json::Value::from(0o644),
            serde_json::Value::from(false),
            serde_json::Value::from(false),
            serde_json::Value::from(true),
            serde_json::Value::from("handle123"),
            serde_json::Value::from(1),
        ];

        let result = db.execute(&insert_sql, &test_record).await;
        assert!(result.is_ok(), "Failed to insert file record: {:?}", result);

        // 验证记录已插入
        let select_sql = format!("SELECT * FROM {} WHERE path = ?", table_name);
        let result = db.query(&select_sql, &[serde_json::Value::from("/test/path/file.txt")]).await;
        assert!(result.is_ok(), "Failed to query file record");

        let query_result = result.unwrap();
        assert_eq!(query_result.rows.len(), 1, "Should have one row");
        
        // 使用更灵活的字符串比较方式
        let path_str = query_result.rows[0]["path"].as_str().unwrap_or("");
        let clean_path = path_str.trim_matches('"');
        assert_eq!(clean_path, "/test/path/file.txt");
        
        assert_eq!(query_result.rows[0]["size"].as_i64().unwrap_or(0), 1024);
        
        let ext_str = query_result.rows[0]["ext"].as_str().unwrap_or("");
        let clean_ext = ext_str.trim_matches('"');
        assert_eq!(clean_ext, "txt");
        assert_eq!(query_result.rows[0]["current_state"].as_i64().unwrap_or(0), 1);

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_batch_insert_temp_table() {
        let job_id = generate_unique_job_id("test_batch_temp");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // 创建新的临时表
        let mut db_with_temp = db;
        let _ = db_with_temp.create_scan_temporary_table().await.expect("Failed to create temporary table");

        // 准备测试数据
        let test_records = vec![
            serde_json::json!({
                "path": "/test/path/file1.txt",
                "size": 1024,
                "ext": "txt",
                "ctime": 1234567890,
                "mtime": 1234567890,
                "atime": 1234567890,
                "perm": 420,
                "is_symlink": false,
                "is_dir": false,
                "is_regular_file": true,
                "file_handle": "handle123",
                "current_state": 1
            }),
            serde_json::json!({
                "path": "/test/path/file2.jpg",
                "size": 2048,
                "ext": "jpg",
                "ctime": 1234567891,
                "mtime": 1234567891,
                "atime": 1234567891,
                "perm": 420,
                "is_symlink": false,
                "is_dir": false,
                "is_regular_file": true,
                "file_handle": "handle123",
                "current_state": 1
            }),
            serde_json::json!({
                "path": "/test/path/dir1",
                "size": 0,
                "ext": "",
                "ctime": 1234567892,
                "mtime": 1234567892,
                "atime": 1234567892,
                "perm": 493,
                "is_symlink": false,
                "is_dir": true,
                "is_regular_file": false,
                "file_handle": "handle123",
                "current_state": 1
            }),
        ];

        // 测试批量插入
        let result = db_with_temp.batch_insert_temp_record_sync(test_records.clone()).await;
        assert!(
            result.is_ok(),
            "Failed to batch insert temp records: {:?}",
            result
        );

        // 验证数据已插入
        let temp_table_name = format!("scan_temp_{}", job_id);
        let count_result = db_with_temp.query(&format!("SELECT COUNT(*) as count FROM {}", temp_table_name), &[]).await;
        assert!(count_result.is_ok(), "Failed to count records");

        let count = count_result.unwrap().rows[0]["count"].as_u64().unwrap_or(0);
        assert_eq!(count, 3, "Should have 3 records in temporary table");

        println!(
            "Successfully inserted {} records to temporary table",
            test_records.len()
        );

        // 测试结束后清理
        let _ = db_with_temp.drop_scan_temporary_table().await;
    }

    #[tokio::test]
    async fn test_batch_insert_empty_temp_table() {
        let job_id = generate_unique_job_id("test_empty_batch");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // 创建新的临时表
        let mut db_with_temp = db;
        let _ = db_with_temp.create_scan_temporary_table().await.expect("Failed to create temporary table");

        // 测试空数据批量插入
        let empty_records = vec![];
        let result = db_with_temp.batch_insert_temp_record_sync(empty_records).await;
        assert!(
            result.is_ok(),
            "Empty batch insert should succeed: {:?}",
            result
        );

        println!("Empty batch insert test passed");

        // 测试结束后清理
        let _ = db_with_temp.drop_scan_temporary_table().await;
    }

    #[tokio::test]
    async fn test_batch_insert_large_temp_table() {
        let job_id = generate_unique_job_id("test_large_batch");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // 创建新的临时表
        let mut db_with_temp = db;
        let _ = db_with_temp.create_scan_temporary_table().await.expect("Failed to create temporary table");

        // 准备大量测试数据
        let mut large_records = Vec::new();
        for i in 0..50 {
            // 50条记录
            large_records.push(serde_json::json!({
                "path": format!("/test/path/file_{}.txt", i),
                "size": 1024 + (i * 100),
                "ext": "txt",
                "ctime": 1234567890 + i,
                "mtime": 1234567890 + i,
                "atime": 1234567890 + i,
                "perm": 420,
                "is_symlink": false,
                "is_dir": false,
                "is_regular_file": true,
                "file_handle": "handle123",
                "current_state": 1
            }));
        }

        // 测试大批量插入
        let start_time = std::time::Instant::now();
        let result = db_with_temp
            .batch_insert_temp_record_sync(large_records.clone())
            .await;
        let duration = start_time.elapsed();

        assert!(
            result.is_ok(),
            "Large batch insert should succeed: {:?}",
            result
        );

        // 验证数据已插入
        let temp_table_name = format!("scan_temp_{}", job_id);
        let count_result = db_with_temp.query(&format!("SELECT COUNT(*) as count FROM {}", temp_table_name), &[]).await;
        assert!(count_result.is_ok(), "Failed to count records");

        let count = count_result.unwrap().rows[0]["count"].as_u64().unwrap_or(0);
        assert_eq!(count, 50, "Should have 50 records in temporary table");

        println!(
            "Successfully inserted {} records in {:?}",
            large_records.len(),
            duration
        );

        // 测试结束后清理
        let _ = db_with_temp.drop_scan_temporary_table().await;
    }

    #[tokio::test]
    async fn test_insert_scan_state_sync() {
        let job_id = generate_unique_job_id("test_insert_state");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // 创建新的scan_state表
        let table_name = format!("scan_state_{}", job_id);
        let _ = db.create_table(&db::traits::TableSchema {
            name: table_name.clone(),
            columns: vec![
                db::traits::ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                db::traits::ColumnInfo {
                    name: "origin_state".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: true,
                    default_value: Some("0".to_string()),
                    is_primary_key: false,
                },
            ],
        }).await;

        // 测试插入scan_state记录
        let insert_sql = format!(
            "INSERT INTO {} (id, origin_state) VALUES (?, ?)",
            table_name
        );
        let result = db.execute(&insert_sql, &[serde_json::Value::from(1), serde_json::Value::from(5)]).await;
        assert!(result.is_ok(), "Failed to insert scan state: {:?}", result);

        // 验证数据已插入
        let select_sql = format!("SELECT * FROM {} WHERE id = 1", table_name);
        let result = db.query(&select_sql, &[]).await;
        assert!(result.is_ok(), "Failed to query scan state");

        let query_result = result.unwrap();
        assert_eq!(query_result.rows.len(), 1, "Should have one row");
        assert_eq!(query_result.rows[0]["id"], 1, "ID should be 1");
        assert_eq!(query_result.rows[0]["origin_state"], 5, "Origin state should be 5");

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }

    #[tokio::test]
    async fn test_table_exists() {
        let job_id = generate_unique_job_id("test_exists");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        let table_name = format!("test_exists_{}", job_id);

        // 验证表不存在
        let exists = db.table_exists(&table_name).await.expect("Failed to check table existence");
        assert!(!exists, "Table should not exist initially");

        // 创建表
        let _ = db.create_table(&db::traits::TableSchema {
            name: table_name.clone(),
            columns: vec![
                db::traits::ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
            ],
        }).await.expect("Failed to create table");

        // 验证表存在
        let exists = db.table_exists(&table_name).await.expect("Failed to check table existence");
        assert!(exists, "Table should exist after creation");
    }

    #[tokio::test]
    async fn test_ping() {
        let job_id = generate_unique_job_id("test_ping");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        let result = db.ping().await;
        assert!(result.is_ok(), "Ping should succeed: {:?}", result);
    }

    #[tokio::test]
    async fn test_execute_batch() {
        let job_id = generate_unique_job_id("test_batch");
        let (db, _temp_file) = setup_test_db_with_job_id(&job_id);
        
        db.initialize()
            .await
            .expect("Failed to initialize database");

        // 创建测试表
        let table_name = format!("test_batch_{}", job_id);
        let _ = db.create_table(&db::traits::TableSchema {
            name: table_name.clone(),
            columns: vec![
                db::traits::ColumnInfo {
                    name: "id".to_string(),
                    data_type: "INTEGER".to_string(),
                    nullable: false,
                    default_value: None,
                    is_primary_key: true,
                },
                db::traits::ColumnInfo {
                    name: "name".to_string(),
                    data_type: "TEXT".to_string(),
                    nullable: true,
                    default_value: None,
                    is_primary_key: false,
                },
            ],
        }).await;

        // 准备批量插入数据
        let insert_sql = format!(
            "INSERT INTO {} (id, name) VALUES (?, ?)",
            table_name
        );
        
        let params_batch = vec![
            vec![serde_json::Value::from(1), serde_json::Value::from("Alice")],
            vec![serde_json::Value::from(2), serde_json::Value::from("Bob")],
            vec![serde_json::Value::from(3), serde_json::Value::from("Charlie")],
        ];

        let result = db.execute_batch(&insert_sql, &params_batch).await;
        assert!(result.is_ok(), "Batch execute should succeed: {:?}", result);

        // 验证数据已插入
        let count_result = db.query(&format!("SELECT COUNT(*) as count FROM {}", table_name), &[]).await;
        assert!(count_result.is_ok(), "Failed to count records");

        let count = count_result.unwrap().rows[0]["count"].as_u64().unwrap_or(0);
        assert_eq!(count, 3, "Should have 3 records after batch insert");

        // 测试结束后清理
        let _ = cleanup_test_tables(&db, &job_id).await;
    }
}