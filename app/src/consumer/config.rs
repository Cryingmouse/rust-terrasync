use serde::{Deserialize, Serialize};

/// 消费者配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerConfig {
    /// 是否启用控制台消费者
    pub enable_console_consumer: bool,
    /// 是否启用日志消费者
    pub enable_log_consumer: bool,
    /// 是否启用数据库消费者
    pub enable_database_consumer: bool,
    /// 是否启用通知消费者
    pub enable_kafka_consumer: bool,
    /// 消费者通道容量
    pub channel_capacity: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            enable_console_consumer: false,
            enable_log_consumer: false,
            enable_database_consumer: false,
            enable_kafka_consumer: false,
            channel_capacity: 10000,
        }
    }
}

impl ConsumerConfig {
    /// 创建仅启用日志消费者的配置
    pub fn log_only() -> Self {
        Self {
            enable_console_consumer: true,
            enable_log_consumer: true,
            enable_database_consumer: false,
            enable_kafka_consumer: false,
            ..Default::default()
        }
    }

    /// 创建启用所有消费者的配置
    pub fn enable_consumer(enable_database_consumer: bool, enable_kafka_consumer: bool) -> Self {
        Self {
            enable_console_consumer: false,
            enable_log_consumer: false,
            enable_database_consumer: enable_database_consumer,
            enable_kafka_consumer: enable_kafka_consumer,
            ..Default::default()
        }
    }

    /// 创建启用所有消费者的配置
    pub fn all_enabled() -> Self {
        Self {
            enable_console_consumer: true,
            enable_log_consumer: true,
            enable_database_consumer: true,
            enable_kafka_consumer: true,
            ..Default::default()
        }
    }

    /// 创建自定义配置
    pub fn new(
        enable_console_consumer: bool, enable_log_consumer: bool, enable_database_consumer: bool,
        enable_kafka_consumer: bool, channel_capacity: usize,
    ) -> Self {
        Self {
            enable_console_consumer,
            enable_log_consumer,
            enable_database_consumer,
            enable_kafka_consumer,
            channel_capacity,
        }
    }
}
