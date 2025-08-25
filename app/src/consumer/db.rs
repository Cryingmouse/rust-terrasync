use crate::consumer::Consumer;
use crate::scan::ScanMessage;
use tokio::sync::broadcast;
use utils::error::Result;

/// 数据库消费者 - 将扫描结果保存到数据库
pub struct DatabaseConsumer;

#[async_trait::async_trait]
impl Consumer for DatabaseConsumer {
    async fn start(
        &mut self, mut receiver: broadcast::Receiver<ScanMessage>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let handle = tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(ScanMessage::Result(_result)) => {
                        // TODO: 实现数据库保存逻辑
                    }
                    Ok(ScanMessage::Config(_)) => {
                        // Database consumer can ignore config messages
                    }
                    Ok(ScanMessage::Complete) => {
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
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
