use tokio::sync::broadcast;
use utils::error::Result;

use crate::consumer::config::ConsumerConfig;
use crate::consumer::{ConsoleConsumer, Consumer, DatabaseConsumer, KafkaConsumer, LogConsumer};
use crate::scan::ScanMessage;

/// 消费者管理器 - 管理多个消费者
pub struct ConsumerManager {
    /// 广播发送器
    broadcaster: broadcast::Sender<ScanMessage>,
    /// 消费者列表
    consumers: Vec<Box<dyn Consumer>>,
}

impl ConsumerManager {
    /// 创建新的消费者管理器
    pub fn new( enable_database_consumer: bool, enable_kafka_consumer: bool) -> Self {
        Self::with_config(&ConsumerConfig::enable_consumer(enable_database_consumer, enable_kafka_consumer))
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
        if config.enable_kafka_consumer {
            manager.add_consumer(Box::new(KafkaConsumer));
        }
        // 始终添加控制台消费者
        manager.add_consumer(Box::new(ConsoleConsumer));

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
        self.broadcaster.send(message).map_err(|e| {
            utils::error::Error::with_source("Failed to broadcast message", Box::new(e))
        })?;
        Ok(())
    }

    /// 关闭所有消费者
    pub async fn shutdown(&self) -> Result<()> {
        // 发送完成消息，忽略错误（可能没有消费者监听）
        let _ = self.broadcaster.send(ScanMessage::Complete);
        Ok(())
    }
}
