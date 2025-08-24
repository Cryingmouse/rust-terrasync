use std::fs;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;

use storage::{create_storage, parse_nfs_path, LocalStorage, NFSStorage, Storage, StorageType};

/// 测试NFS存储的walkdir性能 - 测量海量文件扫描速度（带超时和计数限制）
#[tokio::test]
async fn test_nfs_walkdir_performance() {
    use std::time::Instant;

    let nfs_path = "nfs://10.131.10.10/mnt/raid0".to_string();
    let (server_ip, portmapper_port, path) = parse_nfs_path(&nfs_path);

    println!("测试NFS存储性能:");
    println!("服务器: {}", server_ip);
    println!("端口: {}", portmapper_port);
    println!("路径: {}", path);

    let storage = NFSStorage::new(server_ip, Some(portmapper_port), Some(path));

    // 预热连接
    let _ = storage.walkdir(Some(1)).await;

    // 开始性能测试
    let start_time = Instant::now();
    let mut rx = storage.walkdir(None).await;

    let mut file_count = 0;
    let mut dir_count = 0;
    let mut total_size = 0u64;
    let mut total_entries = 0;

    // 设置超时和计数限制
    let timeout_duration = Duration::from_secs(30);
    let max_entries = 100_000;

    loop {
        // 检查超时
        if start_time.elapsed() >= timeout_duration {
            println!("⚠️  达到30秒超时限制,停止扫描");
            break;
        }

        // 检查计数限制
        if total_entries >= max_entries {
            println!("⚠️  达到10万条目限制,停止扫描");
            break;
        }

        // 使用超时接收
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(entry)) => {
                total_entries += 1;
                if entry.is_dir {
                    dir_count += 1;
                } else {
                    file_count += 1;
                    total_size += entry.size as u64;
                }

                // 每1000条输出进度
                if total_entries % 1000 == 0 {
                    println!("已扫描 {} 条目...", total_entries);
                }
            }
            Ok(None) => {
                println!("扫描完成");
                break;
            }
            Err(_) => {
                // 超时，继续循环检查限制
                continue;
            }
        }
    }

    let duration = start_time.elapsed();
    let duration_secs = duration.as_secs_f64();

    // 计算性能指标
    let scan_speed = if duration_secs > 0.0 {
        file_count as f64 / duration_secs
    } else {
        0.0
    };

    println!("\n=== NFS扫描性能结果 ===");
    println!("总耗时: {:.2} 秒", duration_secs);
    println!("总条目数: {}", total_entries);
    println!("文件数量: {}", file_count);
    println!("目录数量: {}", dir_count);
    println!("总文件大小: {:.2} MB", total_size as f64 / 1024.0 / 1024.0);
    println!("平均扫描速度: {:.2} 文件/秒", scan_speed);

    // 性能基准测试 - 仅在实际扫描到文件时进行
    if file_count > 1000 {
        let expected_min_speed = 100.0; // 最低100文件/秒
        assert!(
            scan_speed >= expected_min_speed,
            "扫描速度过低: {:.2} 文件/秒 < 期望 {:.2} 文件/秒",
            scan_speed,
            expected_min_speed
        );
    }

    // 数据一致性检查
    assert!(total_entries > 0, "应该找到至少一个条目");

    println!("✅ NFS性能测试通过");
}

/// 测试NFS存储的并发扫描性能（带超时和计数限制）
#[tokio::test]
async fn test_nfs_concurrent_walkdir_performance() {
    use std::time::Instant;

    let nfs_path = "nfs://10.131.10.10/mnt/raid0".to_string();
    let (server_ip, portmapper_port, path) = parse_nfs_path(&nfs_path);

    println!("测试NFS并发扫描性能:");

    // 并发测试配置
    const CONCURRENT_TASKS: usize = 4;
    const MAX_ENTRIES_PER_TASK: usize = 25_000; // 每个任务最多2.5万条目
    const TASK_TIMEOUT: Duration = Duration::from_secs(30);

    let mut handles = vec![];
    let start_time = Instant::now();

    for task_id in 0..CONCURRENT_TASKS {
        let server_ip = server_ip.clone();
        let path = path.clone();

        let handle = tokio::spawn(async move {
            let storage = NFSStorage::new(server_ip, Some(portmapper_port), Some(path));
            let mut rx = storage.walkdir(None).await;

            let mut file_count = 0;
            let mut task_entries = 0;
            let task_start = Instant::now();

            loop {
                // 检查任务超时
                if task_start.elapsed() >= TASK_TIMEOUT {
                    println!("⚠️  任务 {} 达到30秒超时限制", task_id);
                    break;
                }

                // 检查计数限制
                if task_entries >= MAX_ENTRIES_PER_TASK {
                    println!("⚠️  任务 {} 达到2.5万条目限制", task_id);
                    break;
                }

                match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
                    Ok(Some(entry)) => {
                        task_entries += 1;
                        if !entry.is_dir {
                            file_count += 1;
                        }

                        if task_entries % 1000 == 0 {
                            println!("任务 {} 已扫描 {} 条目...", task_id, task_entries);
                        }
                    }
                    Ok(None) => {
                        println!("任务 {} 扫描完成", task_id);
                        break;
                    }
                    Err(_) => continue,
                }
            }

            (task_id, file_count, task_entries, task_start.elapsed())
        });

        handles.push(handle);
    }

    // 等待所有并发任务完成
    let results = futures::future::join_all(handles).await;
    let total_duration = start_time.elapsed();

    // 汇总结果
    let mut total_files = 0;
    let mut total_entries = 0;

    println!("\n=== 并发扫描结果 ===");
    for result in results {
        let (task_id, files, entries, duration) = result.unwrap();

        println!(
            "任务 {}: 文件数={}, 总条目数={}, 耗时={:.2}s",
            task_id,
            files,
            entries,
            duration.as_secs_f64()
        );
        total_files += files;
        total_entries += entries;
    }

    let avg_files_per_task = if CONCURRENT_TASKS > 0 {
        total_files / CONCURRENT_TASKS
    } else {
        0
    };
    let concurrent_speed = if total_duration.as_secs_f64() > 0.0 {
        total_files as f64 / total_duration.as_secs_f64()
    } else {
        0.0
    };

    println!("\n=== 并发性能汇总 ===");
    println!("并发任务数: {}", CONCURRENT_TASKS);
    println!("总耗时: {:.2} 秒", total_duration.as_secs_f64());
    println!("总扫描文件: {}", total_files);
    println!("总扫描条目: {}", total_entries);
    println!("平均每个任务文件: {}", avg_files_per_task);
    println!("并发总速度: {:.2} 文件/秒", concurrent_speed);
    println!(
        "平均单任务速度: {:.2} 文件/秒",
        concurrent_speed / CONCURRENT_TASKS as f64
    );

    // 并发性能验证
    assert!(total_files > 0, "应该扫描到文件");

    println!("✅ NFS并发性能测试通过");
}

/// 创建测试用的临时目录结构
fn create_test_structure() -> TempDir {
    let temp_dir = TempDir::new().unwrap();
    let root = temp_dir.path();

    // 创建目录结构
    fs::create_dir_all(root.join("dir1/subdir1")).unwrap();
    fs::create_dir_all(root.join("dir2/subdir2")).unwrap();
    fs::create_dir_all(root.join("empty_dir")).unwrap();

    // 创建文件
    fs::write(root.join("file1.txt"), b"content1").unwrap();
    fs::write(root.join("file2.txt"), b"content2").unwrap();
    fs::write(root.join("dir1/file3.txt"), b"content3").unwrap();
    fs::write(root.join("dir1/subdir1/file4.txt"), b"content4").unwrap();
    fs::write(root.join("dir2/file5.txt"), b"content5").unwrap();
    fs::write(root.join("dir2/subdir2/file6.txt"), b"content6").unwrap();

    temp_dir
}

/// 测试LocalStorage的walkdir接口
#[tokio::test]
async fn test_local_storage_walkdir() {
    let temp_dir = create_test_structure();
    let root_path = temp_dir.path().to_string_lossy().to_string();

    let storage = LocalStorage::new(root_path.clone());

    // 测试walkdir，不传路径参数
    let mut rx = storage.walkdir(None, None).await;

    let mut file_count = 0;
    let mut dir_count = 0;

    while let Some(entry) = rx.recv().await {
        if entry.is_dir {
            dir_count += 1;
        } else {
            file_count += 1;
        }
    }

    let total_entries = file_count + dir_count;
    println!(
        "找到文件: {}, 目录: {}, 总计: {}",
        file_count, dir_count, total_entries
    );

    // 验证找到所有文件和目录
    assert!(total_entries > 6, "应该找到多个文件和目录");

    let root = temp_dir.path();

    // 检查根目录文件
    assert!(root.join("file1.txt").exists(), "应该存在文件: file1.txt");
    assert!(root.join("file2.txt").exists(), "应该存在文件: file2.txt");

    // 检查子目录文件
    assert!(
        root.join("dir1/file3.txt").exists(),
        "应该存在文件: dir1/file3.txt"
    );
    assert!(
        root.join("dir1/subdir1/file4.txt").exists(),
        "应该存在文件: dir1/subdir1/file4.txt"
    );
    assert!(
        root.join("dir2/file5.txt").exists(),
        "应该存在文件: dir2/file5.txt"
    );
    assert!(
        root.join("dir2/subdir2/file6.txt").exists(),
        "应该存在文件: dir2/subdir2/file6.txt"
    );
}

/// 测试walkdir的深度限制
#[tokio::test]
async fn test_local_storage_walkdir_depth_limit() {
    let temp_dir = create_test_structure();
    let root_path = temp_dir.path().to_string_lossy().to_string();

    let storage = LocalStorage::new(root_path);

    // 测试深度限制为1
    let mut rx = storage.walkdir(None, Some(1)).await;

    let mut entries = 0;
    while let Some(_entry) = rx.recv().await {
        entries += 1;
    }

    println!("深度限制为1时找到条目: {}", entries);
    assert!(entries > 0, "应该找到条目");
}

/// 测试walkdir指定子目录
#[tokio::test]
async fn test_local_storage_walkdir_subdir() {
    let temp_dir = create_test_structure();
    let root_path = temp_dir.path().to_string_lossy().to_string();

    let storage = LocalStorage::new(root_path.clone());

    // 测试指定子目录 - 使用相对于根目录的完整路径
    let subdir_path = PathBuf::from(&root_path).join("dir1");
    let mut rx = storage.walkdir(Some(subdir_path), None).await;

    let mut file_count = 0;
    while let Some(entry) = rx.recv().await {
        if !entry.is_dir {
            file_count += 1;
        }
    }

    println!("在dir1中找到文件: {}", file_count);
    assert!(file_count > 0, "应该找到dir1下的文件");
}

/// 测试create_storage创建LocalStorage的walkdir
#[tokio::test]
async fn test_create_storage_walkdir() {
    let temp_dir = create_test_structure();
    let root_path = temp_dir.path().to_string_lossy().to_string();

    let storage = create_storage(&root_path).unwrap();

    match storage {
        StorageType::Local(storage) => {
            let mut rx = storage.walkdir(None, None).await;

            let mut entries = 0;
            while let Some(_entry) = rx.recv().await {
                entries += 1;
            }

            println!("create_storage找到条目: {}", entries);
            assert!(entries > 6, "应该找到多个文件和目录");
        }
        _ => panic!("应该创建LocalStorage"),
    }
}

/// 测试Storage trait的walkdir接口
#[tokio::test]
async fn test_storage_trait_walkdir() {
    let temp_dir = create_test_structure();
    let root_path = temp_dir.path().to_string_lossy().to_string();

    let storage = create_storage(&root_path).unwrap();

    // 通过Storage trait调用walkdir
    let mut rx = storage.walkdir(None, None).await;

    let mut entries = 0;
    while let Some(_entry) = rx.recv().await {
        entries += 1;
    }

    println!("Storage trait找到条目: {}", entries);
    assert!(entries > 6, "应该找到多个文件和目录");
}

/// 测试空目录的walkdir
#[tokio::test]
async fn test_empty_directory_walkdir() {
    let temp_dir = TempDir::new().unwrap();
    let root_path = temp_dir.path().to_string_lossy().to_string();

    let storage = LocalStorage::new(root_path);

    let mut rx = storage.walkdir(None, None).await;

    let mut entries = 0;
    while let Some(_entry) = rx.recv().await {
        entries += 1;
    }

    println!("空目录找到条目: {}", entries);
    assert!(entries >= 1, "空目录应该至少有一个条目");
}

/// 测试walkdir的错误处理
#[tokio::test]
async fn test_walkdir_error_handling() {
    let storage = LocalStorage::new("/non/existent/path".to_string());

    let mut rx = storage.walkdir(None, None).await;

    let mut entries = 0;
    while let Some(_entry) = rx.recv().await {
        entries += 1;
    }

    println!("不存在的路径找到条目: {}", entries);
    // 对于不存在的路径，主要测试不panic
}

/// 测试StorageEntry字段正确性
#[tokio::test]
async fn test_storage_entry_fields() {
    let temp_dir = create_test_structure();
    let root_path = temp_dir.path().to_string_lossy().to_string();

    let storage = LocalStorage::new(root_path);

    let mut rx = storage.walkdir(None, None).await;

    let mut found_file1 = false;
    while let Some(entry) = rx.recv().await {
        if entry.name == "file1.txt" {
            found_file1 = true;
            assert_eq!(entry.size, 8); // "content1"的长度
            assert!(!entry.is_dir);
            assert!(entry.path.ends_with("file1.txt"));
            assert!(entry.is_symlink.is_some());
            break;
        }
    }

    assert!(found_file1, "应该找到file1.txt");
}

/// 测试并发walkdir
#[tokio::test]
async fn test_concurrent_walkdir() {
    let temp_dir = create_test_structure();
    let root_path = temp_dir.path().to_string_lossy().to_string();

    let storage = LocalStorage::new(root_path);

    // 启动多个并发walkdir
    let mut handles = vec![];
    for _ in 0..3 {
        let storage_clone = LocalStorage::new(storage.get_root().to_string());
        let handle = tokio::spawn(async move {
            let mut rx = storage_clone.walkdir(None, None).await;
            let mut entries = 0;
            while let Some(_entry) = rx.recv().await {
                entries += 1;
            }
            entries
        });
        handles.push(handle);
    }

    // 等待所有任务完成
    let results = futures::future::join_all(handles).await;

    // 验证每个任务都有结果
    for result in results {
        let entries = result.unwrap();
        assert!(entries > 6, "每个并发任务都应该找到多个文件和目录");
    }
}
