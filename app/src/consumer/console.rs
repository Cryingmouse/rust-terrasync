use crate::consumer::stats::{ScanStats, StatsCalculator};
use crate::consumer::Consumer;
use crate::scan::ScanMessage;
use std::path::Path;
use std::time::Instant;
use tokio::sync::broadcast;
use utils::error::Result;

/// 控制台消费者 - 将扫描结果输出到控制台并计算统计信息
pub struct ConsoleConsumer;

#[async_trait::async_trait]
impl Consumer for ConsoleConsumer {
    async fn start(
        &mut self, mut receiver: broadcast::Receiver<ScanMessage>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let handle = tokio::spawn(async move {
            let start_time = Instant::now();
            let mut stats = ScanStats::default();
            let mut calculator = None::<StatsCalculator>;
            let mut base_path;
            let mut last_progress_time = Instant::now();
            let mut config_received = false;

            // 处理队列消息并广播给消费者
            println!("terrasync 3.0.0; (c) 2025 LenovoNetapp, Inc.");

            loop {
                match receiver.recv().await {
                    Ok(ScanMessage::Result(result)) => {
                        // 初始化计算器（第一次收到结果时）
                        if calculator.is_none() {
                            // 尝试从文件路径中提取基础目录
                            let path = Path::new(&result.file_path);
                            if let Some(parent) = path.parent() {
                                base_path = parent.to_string_lossy().to_string();
                            } else {
                                base_path = ".".to_string();
                            }
                            calculator = Some(StatsCalculator::new(&base_path));
                        }

                        let calc = calculator.as_ref().unwrap();

                        // 更新基本统计
                        if result.is_dir {
                            stats.total_dirs += 1;
                        } else {
                            stats.total_files += 1;
                            stats.total_size += result.size as i64;
                        }

                        // 使用StatsCalculator更新扩展统计信息
                        if result.is_dir {
                            let depth = calc.calculate_depth(Path::new(&result.file_path));
                            calc.update_dir_stats(&mut stats, &result.file_name, depth);
                        } else {
                            calc.update_file_stats(
                                &mut stats,
                                &result.file_name,
                                result.size,
                                result.is_symlink,
                            );
                        }

                        // 每10秒打印一次进度
                        if last_progress_time.elapsed().as_secs() >= 10 {
                            let now = chrono::Local::now();
                            println!(
                                "[{}] Scan progress: {} total files, {} total dirs",
                                now.format("%Y-%m-%d %H:%M:%S"),
                                stats.total_files,
                                stats.total_dirs
                            );
                            last_progress_time = Instant::now();
                        }

                        log::debug!("[ConsoleConsumer] Processed: {:?}", result);
                    }
                    Ok(ScanMessage::Config(config)) => {
                        // 使用配置信息填充统计信息
                        stats.command = ScanStats::build_command(&config.params);
                        stats.job_id = config
                            .params
                            .id
                            .clone()
                            .unwrap_or_else(|| "unknown".to_string());
                        stats.log_path = ScanStats::build_log_path();
                        config_received = true;
                        log::info!("[ConsoleConsumer] Received scan configuration");
                    }
                    Ok(ScanMessage::Complete) => {
                        log::info!("[ConsoleConsumer] Scan completed");

                        // 计算总执行时间
                        let duration = start_time.elapsed();
                        stats.total_time = format!("{:.2}s", duration.as_secs_f64());

                        // 如果没有收到配置，设置默认值
                        if !config_received {
                            stats.command = "terrasync scan".to_string();
                            stats.job_id = "unknown".to_string();
                            stats.log_path = ScanStats::build_log_path();
                        }

                        // 打印最终统计信息
                        println!("\n{}", stats);
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("[ConsoleConsumer] Channel closed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        log::warn!("[ConsoleConsumer] Channel lagged, skipping messages");
                        continue;
                    }
                }
            }
            Ok(())
        });

        Ok(handle)
    }

    fn name(&self) -> &'static str {
        "console_consumer"
    }
}
