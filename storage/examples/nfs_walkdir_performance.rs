use std::time::Duration;
use std::time::SystemTime;
use storage::nfs::{NFSStorage, parse_nfs_path};

/// 将Unix权限位格式化为 rwxrwxrwx 字符串
fn format_permissions(mode: u32) -> String {
    let mut perms = String::with_capacity(9);
    let bit = |m, s| if m != 0 { s } else { "-" };
    perms.push_str(bit(mode & 0o400, "r"));
    perms.push_str(bit(mode & 0o200, "w"));
    perms.push_str(bit(mode & 0o100, "x"));
    perms.push_str(bit(mode & 0o040, "r"));
    perms.push_str(bit(mode & 0o020, "w"));
    perms.push_str(bit(mode & 0o010, "x"));
    perms.push_str(bit(mode & 0o004, "r"));
    perms.push_str(bit(mode & 0o002, "w"));
    perms.push_str(bit(mode & 0o001, "x"));
    perms
}

/// NFS存储walkdir性能测试示例 - 测量海量文件扫描速度（带超时和计数限制）
///
/// 运行示例：
/// cargo run --example nfs_walkdir_performance
#[tokio::main]
async fn main() {
    test_nfs_walkdir_performance().await;
}

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
    let mut symlink_count = 0;
    let mut total_size = 0u64;
    let mut total_entries = 0;

    // 设置超时和计数限制
    let timeout_duration = Duration::from_secs(30);
    let max_entries = 100_000;

    loop {
        // 检查超时
        if start_time.elapsed() >= timeout_duration {
            if total_entries > 0 {
                println!(
                    "└──────┴────────────────────────┴──────────┴────────┴────────────┴─────────┴─────────────┘"
                );
            }
            println!("⚠️  达到30秒超时限制,停止扫描");
            break;
        }

        // 检查计数限制
        if total_entries >= max_entries {
            if total_entries > 0 {
                println!(
                    "└──────┴────────────────────────┴──────────┴────────┴────────────┴─────────┴─────────────┘"
                );
            }
            println!("⚠️  达到10万条目限制,停止扫描");
            break;
        }

        // 使用超时接收
        match tokio::time::timeout(Duration::from_millis(100), rx.recv()).await {
            Ok(Some(entry)) => {
                total_entries += 1;

                let mut file_type = String::new();
                let mut hard_links_str = "-".to_string();
                let mut symlink_flag = "-".to_string();

                if entry.is_dir {
                    dir_count += 1;
                    file_type.push_str("📁 DIR");
                } else {
                    file_count += 1;
                    file_type.push_str("📄 FILE");
                }

                // 显示硬链接数
                if let Some(hard_links) = entry.hard_links {
                    hard_links_str = hard_links.to_string();
                }

                // 显示软连接标识
                if let Some(is_symlink) = entry.is_symlink {
                    if is_symlink {
                        symlink_flag = "🔗".to_string();
                        symlink_count += 1;
                    }
                }

                total_size += entry.size;

                // 每100条打印标题
                if total_entries % 100 == 1 {
                    if total_entries > 1 {
                        println!(
                            "└──────┴────────────────────────┴──────────┴────────┴────────────┴─────────┴─────────────┘"
                        );
                        println!();
                    }
                    println!(
                        "┌──────┬────────────────────────┬──────────┬────────┬────────────┬─────────┬─────────────┐"
                    );
                    println!(
                        "│ {:<4} │ {:<24} │ {:<10} │ {:<6} │ {:<10} │ {:<7} │ {:<13} │",
                        "类型", "文件名", "大小", "权限", "硬链接", "软连接", "修改时间"
                    );
                    println!(
                        "├──────┼────────────────────────┼──────────┼────────┼────────────┼─────────┼─────────────┤"
                    );
                }

                let size_str = if entry.size < 1024 {
                    format!("{} B", entry.size)
                } else if entry.size < 1024 * 1024 {
                    format!("{:.1} KB", entry.size as f64 / 1024.0)
                } else if entry.size < 1024 * 1024 * 1024 {
                    format!("{:.1} MB", entry.size as f64 / 1024.0 / 1024.0)
                } else {
                    format!("{:.1} GB", entry.size as f64 / 1024.0 / 1024.0 / 1024.0)
                };

                let format_time = |time: SystemTime| -> String {
                    chrono::DateTime::<chrono::Local>::from(time)
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string()
                };

                let name_display = if entry.name.len() > 24 {
                    format!("{}...", &entry.name[..21])
                } else {
                    entry.name.clone()
                };

                // 格式化权限显示
                let perms_str = entry
                    .mode
                    .map(|mode| format_permissions(mode))
                    .unwrap_or_else(|| "-".to_string());

                println!(
                    "│ {:<4} │ {:<24} │ {:<10} │ {:<6} │ {:<10} │ {:<7} │ {:<13} │",
                    file_type,
                    name_display,
                    size_str,
                    perms_str,
                    hard_links_str,
                    symlink_flag,
                    format_time(entry.modified)
                );

                // 每1000条输出进度
                if total_entries % 1000 == 0 {
                    println!(
                        "├──────┼────────────────────────┼──────────┼────────┼────────────┼─────────┼─────────────┤"
                    );
                    println!("│ 📊 进度: 已扫描 {:<8} 条目... │", total_entries);
                    println!(
                        "├──────┼────────────────────────┼──────────┼────────┼────────────┼─────────┼─────────────┤"
                    );
                }
            }
            Ok(None) => {
                if total_entries > 0 {
                    println!(
                        "├──────┼────────────────────────┼──────────┼────────┼────────────┼─────────┼─────────────┤"
                    );
                    println!(
                        "└──────┴────────────────────────┴──────────┴────────┴────────────┴─────────┴─────────────┘"
                    );
                }
                println!("✅ 扫描完成");
                break;
            }
            Err(_) => {
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
    println!("软连接数量: {}", symlink_count);
    println!("总文件大小: {:.2} MB", total_size as f64 / 1024.0 / 1024.0);
    println!("平均扫描速度: {:.2} 文件/秒", scan_speed);

    // 性能基准测试
    if file_count > 1000 {
        let expected_min_speed = 100.0;
        if scan_speed < expected_min_speed {
            eprintln!(
                "⚠️  扫描速度过低: {:.2} 文件/秒 < 期望 {:.2} 文件/秒",
                scan_speed, expected_min_speed
            );
        } else {
            println!("✅ 性能测试通过 - 扫描速度: {:.2} 文件/秒", scan_speed);
        }
    }

    // 数据一致性检查
    if total_entries == 0 {
        println!("⚠️  未找到任何条目");
    } else {
        println!("✅ NFS性能测试完成");
    }
}
