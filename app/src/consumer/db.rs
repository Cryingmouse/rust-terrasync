use crate::consumer::Consumer;
use crate::scan::ScanMessage;
use chrono::Local;
use db::config::DatabaseConfig;
use db::factory::create_database;
use db::traits::Database;
use db::traits::FileScanRecord;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::broadcast;
use utils::app_config::AppConfig;
use utils::error::Result;

/// 数据库消费者 - 将扫描结果存储到数据库
pub struct DatabaseConsumer;

/// 将作业ID转换为文件系统安全的标识符
/// 将特殊字符转换为下划线，确保可用于目录和文件名
fn sanitize_job_id(job_id: &str) -> String {
    job_id
        .replace('-', "_")
        .replace('.', "_")
        .replace(' ', "_")
        .replace('/', "_")
        .replace('\\', "_")
}

#[async_trait::async_trait]
impl Consumer for DatabaseConsumer {
    async fn start(
        &mut self, mut receiver: broadcast::Receiver<ScanMessage>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let handle = tokio::spawn(async move {
            let mut database: Option<Arc<dyn Database>> = None;
            let mut batch_size: Option<u32> = None;
            let mut batch_records = Vec::with_capacity(batch_size.unwrap_or(200_000) as usize);

            loop {
                match receiver.recv().await {
                    Ok(ScanMessage::Result(entity)) => {
                        if let Some(db) = &database {
                            // Convert SystemTime to u64 timestamp
                            let ctime = entity
                                .ctime
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            let mtime = entity
                                .mtime
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);
                            let atime = entity
                                .atime
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .map(|d| d.as_secs())
                                .unwrap_or(0);

                            // Convert permissions string to u32
                            let perm = entity
                                .permissions
                                .as_ref()
                                .and_then(|p| u32::from_str_radix(p, 8).ok())
                                .unwrap_or(0);

                            let record = FileScanRecord {
                                path: entity.file_path,
                                size: entity.size,
                                ext: entity.extension,
                                ctime,
                                mtime,
                                atime,
                                perm,
                                is_symlink: entity.is_symlink,
                                is_dir: entity.is_dir,
                                is_regular_file: !entity.is_dir,
                                file_handle: None,
                                current_state: 0,
                            };
                            batch_records.push(record);

                            // 达到批量大小则插入数据库
                            if batch_records.len() >= batch_size.unwrap_or(200_000) as usize {
                                log::info!(
                                    "[DatabaseConsumer] Inserting batch of {} records",
                                    batch_records.len()
                                );
                                let _ = db
                                    .batch_insert_base_record_sync(batch_records.clone())
                                    .await;
                                batch_records.clear();
                            }
                        }
                    }
                    Ok(ScanMessage::Complete) => {
                        log::info!(
                            "[DatabaseConsumer] Scan completed, flushing remaining records..."
                        );

                        // 如果有剩余记录，插入数据库
                        if let Some(db) = &database {
                            if !batch_records.is_empty() {
                                log::info!(
                                    "[DatabaseConsumer] Inserting final batch of {} records",
                                    batch_records.len()
                                );
                                let _ = db
                                    .batch_insert_base_record_sync(batch_records.clone())
                                    .await;
                                batch_records.clear();
                            }
                        }

                        log::info!("[DatabaseConsumer] Scan completed, shutting down...");
                        break;
                    }
                    Ok(ScanMessage::Config(config)) => {
                        // 从应用配置中获取数据库配置
                        let app_config = AppConfig::fetch().map_err(|e| {
                            utils::error::Error::with_source(
                                "Failed to load application configuration",
                                Box::new(e),
                            )
                        })?;

                        // 生成或处理扫描ID，使用与CLI相同的逻辑
                        let current_job_id = config.params.id.clone().unwrap_or_else(|| {
                            let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
                            timestamp
                        });
                        let current_job_id = sanitize_job_id(&current_job_id);

                        log::info!(
                            "[DatabaseConsumer] Initializing database for job: {}",
                            current_job_id
                        );

                        // 构建数据库配置
                        let db_config = DatabaseConfig {
                            enabled: app_config.database.enabled,
                            db_type: app_config.database.r#type.clone(),
                            batch_size: app_config.database.batch_size,
                            clickhouse: Some(db::config::ClickHouseConfig {
                                dsn: app_config.database.clickhouse.dsn.clone(),
                                dial_timeout: app_config.database.clickhouse.dial_timeout,
                                read_timeout: app_config.database.clickhouse.read_timeout,
                                database: Some("default".to_string()),
                                username: Some("default".to_string()),
                                password: None,
                            }),
                        };

                        batch_size = Some(db_config.batch_size);

                        // 通过DatabaseFactory创建数据库实例
                        match create_database(&db_config, current_job_id.clone()) {
                            Ok(db_instance) => {
                                // 初始化数据库连接
                                if let Err(e) = db_instance.ping().await {
                                    log::error!(
                                        "[DatabaseConsumer] Failed to connect to database: {}",
                                        e
                                    );
                                    continue;
                                }

                                // 创建必要的表
                                if let Err(e) = db_instance
                                    .create_table(db::SCAN_BASE_TABLE_BASE_NAME)
                                    .await
                                {
                                    log::error!(
                                        "[DatabaseConsumer] Failed to create scan_base table: {}",
                                        e
                                    );
                                    continue;
                                }

                                if let Err(e) = db_instance
                                    .create_table(db::SCAN_STATE_TABLE_BASE_NAME)
                                    .await
                                {
                                    log::error!(
                                        "[DatabaseConsumer] Failed to create scan_state table: {}",
                                        e
                                    );
                                    continue;
                                }

                                if let Err(e) = db_instance.insert_scan_state_sync(0).await {
                                    log::error!(
                                        "[DatabaseConsumer] Failed to create scan_state table: {}",
                                        e
                                    );
                                    continue;
                                }

                                log::info!(
                                    "[DatabaseConsumer] Database initialized successfully for job: {}",
                                    current_job_id
                                );

                                database = Some(db_instance);
                            }
                            Err(e) => {
                                log::error!(
                                    "[DatabaseConsumer] Failed to create database instance: {}",
                                    e
                                );
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::info!("[DatabaseConsumer] Broadcast channel closed, shutting down...");
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        log::warn!(
                            "[DatabaseConsumer] Broadcast lag detected, skipping messages..."
                        );
                        continue;
                    }
                }
            }

            Ok(())
        });

        Ok(handle)
    }

    fn name(&self) -> &'static str {
        "database_consumer"
    }
}
