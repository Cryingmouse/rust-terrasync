use crate::consumer::Consumer;
use crate::scan::ScanMessage;
use db::config::DatabaseConfig;
use db::factory::create_database;
use db::traits::Database;
use db::traits::FileScanRecord;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::broadcast;
use utils::error::Result;

/// 数据库消费者 - 将扫描结果存储到数据库
pub struct DatabaseConsumer;

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
                        // 生成或处理扫描ID，使用与CLI相同的逻辑
                        let current_job_id = config.job_id.clone();

                        log::info!(
                            "[DatabaseConsumer] Initializing database for job: {}",
                            current_job_id
                        );

                        // 构建数据库配置
                        let db_config = DatabaseConfig {
                            enabled: config.app_config.database.enabled,
                            db_type: config.app_config.database.r#type.clone(),
                            batch_size: config.app_config.database.batch_size,
                            clickhouse: Some(db::config::ClickHouseConfig {
                                dsn: config.app_config.database.clickhouse.dsn.clone(),
                                dial_timeout: config.app_config.database.clickhouse.dial_timeout,
                                read_timeout: config.app_config.database.clickhouse.read_timeout,
                                database: config.app_config.database.clickhouse.database.clone(),
                                username: config.app_config.database.clickhouse.username.clone(),
                                password: config.app_config.database.clickhouse.password.clone(),
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
