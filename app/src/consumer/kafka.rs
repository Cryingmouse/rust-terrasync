use crate::consumer::Consumer;
use crate::scan::ScanMessage;
use tokio::sync::broadcast;
use utils::error::Result;

/// 通知消费者 - 发送通知到其他系统
pub struct KafkaConsumer;

#[async_trait::async_trait]
impl Consumer for KafkaConsumer {
    async fn start(
        &mut self, mut receiver: broadcast::Receiver<ScanMessage>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let handle = tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(ScanMessage::Result(result)) => {
                        // TODO: 实现通知逻辑
                        log::info!("[KafkaConsumer] Sending notification for: {:?}", result);
                    }
                    Ok(ScanMessage::Complete) => {
                        log::info!("[KafkaConsumer] Scan completed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("[KafkaConsumer] Channel closed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        log::warn!("[KafkaConsumer] Channel lagged, skipping messages");
                        continue;
                    }
                }
            }
            Ok(())
        });

        Ok(handle)
    }

    fn name(&self) -> &'static str {
        "kafka_consumer"
    }
}
