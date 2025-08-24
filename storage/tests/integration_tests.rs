mod common;
mod test_walkdir;

use common::create_test_structure;
use storage::{create_storage, StorageType};
use tempfile::TempDir;

/// 集成测试：验证整个存储系统的walkdir功能
#[tokio::test]
async fn test_storage_integration() {
    let temp_dir = create_test_structure();
    let root_path = temp_dir.path().to_string_lossy().to_string();

    // 测试通过create_storage创建LocalStorage
    let storage = create_storage(&root_path).unwrap();

    match storage {
        StorageType::Local(local_storage) => {
            // 测试walkdir功能
            let mut rx = local_storage.walkdir(None, None).await;

            let mut entries = Vec::new();
            while let Some(entry) = rx.recv().await {
                entries.push(entry);
            }

            // 验证基本功能
            assert!(entries.len() > 6, "应该找到多个文件和目录");

            // 验证文件和目录分类
            let files: Vec<_> = entries.iter().filter(|e| !e.is_dir).collect();
            let dirs: Vec<_> = entries.iter().filter(|e| e.is_dir).collect();

            assert!(!files.is_empty(), "应该找到文件");
            assert!(!dirs.is_empty(), "应该找到目录");

            // 验证特定文件存在
            let file_names: Vec<_> = files.iter().map(|e| e.name.as_str()).collect();
            assert!(file_names.contains(&"file1.txt"));
            assert!(file_names.contains(&"file2.txt"));
            assert!(file_names.contains(&"file3.txt"));
            assert!(file_names.contains(&"file4.txt"));
            assert!(file_names.contains(&"file5.txt"));
            assert!(file_names.contains(&"file6.txt"));

            println!(
                "集成测试通过：找到 {} 个文件和 {} 个目录",
                files.len(),
                dirs.len()
            );
        }
        _ => panic!("应该创建LocalStorage"),
    }
}

/// 性能测试：大目录遍历
#[tokio::test]
async fn test_walkdir_performance() {
    use std::time::Instant;

    let temp_dir = common::create_large_test_structure();
    let root_path = temp_dir.path().to_string_lossy().to_string();

    let storage = create_storage(&root_path).unwrap();

    let start_time = Instant::now();

    match storage {
        StorageType::Local(local_storage) => {
            let mut rx = local_storage.walkdir(None, None).await;

            let mut entry_count = 0;
            while let Some(_entry) = rx.recv().await {
                entry_count += 1;
            }

            let duration = start_time.elapsed();

            assert!(entry_count >= 120, "应该找到至少120个文件和目录");
            assert!(duration.as_secs() < 5, "遍历大目录应该很快完成");

            println!(
                "性能测试通过：遍历 {} 个条目耗时 {:?}",
                entry_count, duration
            );
        }
        _ => panic!("应该创建LocalStorage"),
    }
}

/// 边界测试：特殊路径和空目录
#[tokio::test]
async fn test_walkdir_edge_cases() {
    // 测试空目录
    let empty_temp = TempDir::new().unwrap();
    let empty_path = empty_temp.path().to_string_lossy().to_string();

    let storage = create_storage(&empty_path).unwrap();

    match storage {
        StorageType::Local(local_storage) => {
            let mut rx = local_storage.walkdir(None, None).await;

            let mut entries = Vec::new();
            while let Some(entry) = rx.recv().await {
                entries.push(entry);
            }

            // 空目录应该至少包含根目录
            assert!(!entries.is_empty(), "空目录应该至少包含根目录");

            let root_entry = entries.iter().find(|e| e.is_dir).unwrap();
            assert!(
                root_entry.name.is_empty()
                    || root_entry.name
                        == empty_temp
                            .path()
                            .file_name()
                            .unwrap()
                            .to_string_lossy()
                            .to_string()
            );
        }
        _ => panic!("应该创建LocalStorage"),
    }
}
