use tokio::sync::broadcast;
use crate::scan::ScanMessage;
use crate::consumer::config::ConsumerConfig;
use utils::error::Result;

pub mod config;

/// 消费者管理器 - 管理多个消费者
pub struct ConsumerManager {
    /// 广播发送器
    broadcaster: broadcast::Sender<ScanMessage>,
    /// 消费者列表
    consumers: Vec<Box<dyn Consumer>>,
}

impl ConsumerManager {
    /// 创建新的消费者管理器
    pub fn new() -> Self {
        Self::with_config(&ConsumerConfig::default())
    }

    /// 根据配置创建消费者管理器
    pub fn with_config(config: &ConsumerConfig) -> Self {
        let (broadcaster, _) = broadcast::channel(config.channel_capacity);
        let mut manager = Self {
            broadcaster,
            consumers: Vec::new(),
        };

        // 根据配置添加消费者
        if config.enable_log_consumer {
            manager.add_consumer(Box::new(LogConsumer));
        }
        if config.enable_database_consumer {
            manager.add_consumer(Box::new(DatabaseConsumer));
        }
        if config.enable_notification_consumer {
            manager.add_consumer(Box::new(NotificationConsumer));
        }

        manager
    }

    /// 添加消费者
    pub fn add_consumer(&mut self, consumer: Box<dyn Consumer>) {
        self.consumers.push(consumer);
    }

    /// 启动所有消费者
    pub async fn start_consumers(&mut self) -> Result<Vec<tokio::task::JoinHandle<Result<()>>>> {
        let mut handles = Vec::new();
        
        for consumer in &mut self.consumers {
            let receiver = self.broadcaster.subscribe();
            let consumer_handle = consumer.start(receiver).await?;
            handles.push(consumer_handle);
        }

        Ok(handles)
    }

    /// 获取广播发送器
    pub fn get_broadcaster(&self) -> broadcast::Sender<ScanMessage> {
        self.broadcaster.clone()
    }

    /// 获取消费者数量
    pub fn get_consumer_count(&self) -> usize {
        self.consumers.len()
    }

    /// 广播消息到所有消费者
    pub fn broadcast(&self, message: ScanMessage) -> Result<()> {
        self.broadcaster
            .send(message)
            .map_err(|e| utils::error::Error::with_source("Failed to broadcast message", Box::new(e)))?;
        Ok(())
    }

    /// 关闭所有消费者
    pub async fn shutdown(&self) -> Result<()> {
        // 发送完成消息，忽略错误（可能没有消费者监听）
        let _ = self.broadcaster.send(ScanMessage::Complete);
        Ok(())
    }
}

/// 消费者 trait - 定义消费者接口
#[async_trait::async_trait]
pub trait Consumer: Send + Sync {
    /// 启动消费者
    async fn start(&mut self, receiver: broadcast::Receiver<ScanMessage>) -> Result<tokio::task::JoinHandle<Result<()>>>;
    
    /// 获取消费者名称
    fn name(&self) -> &'static str;
}

/// 日志消费者 - 将扫描结果记录到日志
pub struct LogConsumer;

#[async_trait::async_trait]
impl Consumer for LogConsumer {
    async fn start(&mut self, mut receiver: broadcast::Receiver<ScanMessage>) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let handle = tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(ScanMessage::Result(result)) => {
                        log::info!("[LogConsumer] Scan result: {:?}", result);
                    }
                    Ok(ScanMessage::Stats(stats)) => {
                        log::info!("[LogConsumer] Scan stats: {:?}", stats);
                    }
                    Ok(ScanMessage::Complete) => {
                        log::info!("[LogConsumer] Scan completed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("[LogConsumer] Channel closed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        log::warn!("[LogConsumer] Channel lagged, skipping messages");
                        continue;
                    }
                }
            }
            Ok(())
        });

        Ok(handle)
    }

    fn name(&self) -> &'static str {
        "log_consumer"
    }
}

/// 数据库消费者 - 将扫描结果保存到数据库
pub struct DatabaseConsumer;

#[async_trait::async_trait]
impl Consumer for DatabaseConsumer {
    async fn start(&mut self, mut receiver: broadcast::Receiver<ScanMessage>) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let handle = tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(ScanMessage::Result(result)) => {
                        // TODO: 实现数据库保存逻辑
                        log::info!("[DatabaseConsumer] Saving result to database: {:?}", result);
                    }
                    Ok(ScanMessage::Stats(stats)) => {
                        log::info!("[DatabaseConsumer] Processing stats: {:?}", stats);
                    }
                    Ok(ScanMessage::Complete) => {
                        log::info!("[DatabaseConsumer] Scan completed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("[DatabaseConsumer] Channel closed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        log::warn!("[DatabaseConsumer] Channel lagged, skipping messages");
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

/// 通知消费者 - 发送通知到其他系统
pub struct NotificationConsumer;

#[async_trait::async_trait]
impl Consumer for NotificationConsumer {
    async fn start(&mut self, mut receiver: broadcast::Receiver<ScanMessage>) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let handle = tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(ScanMessage::Result(result)) => {
                        // TODO: 实现通知逻辑
                        log::info!("[NotificationConsumer] Sending notification for: {:?}", result);
                    }
                    Ok(ScanMessage::Stats(stats)) => {
                        log::info!("[NotificationConsumer] Processing stats: {:?}", stats);
                    }
                    Ok(ScanMessage::Complete) => {
                        log::info!("[NotificationConsumer] Scan completed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("[NotificationConsumer] Channel closed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        log::warn!("[NotificationConsumer] Channel lagged, skipping messages");
                        continue;
                    }
                }
            }
            Ok(())
        });

        Ok(handle)
    }

    fn name(&self) -> &'static str {
        "notification_consumer"
    }
}