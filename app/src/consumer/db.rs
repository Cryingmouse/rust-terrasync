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
                    Ok(ScanMessage::Result(result)) => {
                        // TODO: 实现数据库保存逻辑
                        log::info!("[DatabaseConsumer] Saving result to database: {:?}", result);
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
