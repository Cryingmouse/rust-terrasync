use tokio::sync::broadcast;
use utils::error::Result;

use crate::scan::ScanMessage;

// 子模块声明
mod console;
mod db;
mod kafka;
mod log;
mod manager;

// 公共模块
pub mod config;

// 重新导出重要的类型，方便用户从crate根导入
pub use console::ConsoleConsumer;
pub use db::DatabaseConsumer;
pub use kafka::KafkaConsumer;
pub use log::LogConsumer;
pub use manager::ConsumerManager;

/// 消费者 trait - 定义消费者接口
#[async_trait::async_trait]
pub trait Consumer: Send + Sync {
    /// 启动消费者
    async fn start(
        &mut self, receiver: broadcast::Receiver<ScanMessage>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>>;

    /// 获取消费者名称
    fn name(&self) -> &'static str;
}
