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
                    Ok(ScanMessage::Result(_result)) => {
                        // TODO: 实现通知逻辑
                    }
                    Ok(ScanMessage::Complete) => {
                        break;
                    }
                    Ok(ScanMessage::Config(_)) => {}
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
        "kafka_consumer"
    }
}
