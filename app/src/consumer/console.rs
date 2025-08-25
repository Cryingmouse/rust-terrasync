use crate::consumer::Consumer;
use crate::consumer::stats::{ScanStats, StatsCalculator};
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
            let mut base_path = String::new();
            let mut total_processed = 0;
            let mut last_progress_time = Instant::now();

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

                        // 如果匹配且未被排除，更新匹配统计
                        if result.matched && !result.excluded {
                            if result.is_dir {
                                stats.matched_dirs += 1;
                            } else {
                                stats.matched_files += 1;
                            }
                        }

                        total_processed += 1;

                        // 每1000个文件或每5秒打印一次进度
                        if total_processed % 1000 == 0
                            || last_progress_time.elapsed().as_secs() >= 5
                        {
                            let now = chrono::Local::now();
                            println!("[{}] Scan progress: {} total files, {} total dirs, {} matched files, {} matched dirs",
                                     now.format("%Y-%m-%d %H:%M:%S"),
                                     stats.total_files, stats.total_dirs, stats.matched_files, stats.matched_dirs);
                            last_progress_time = Instant::now();
                        }

                        log::debug!("[ConsoleConsumer] Processed: {:?}", result);
                    }
                    Ok(ScanMessage::Complete) => {
                        log::info!("[ConsoleConsumer] Scan completed");

                        // 计算总执行时间
                        let duration = start_time.elapsed();
                        stats.total_time = format!("{:.2}s", duration.as_secs_f64());

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
