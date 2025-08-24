use crate::scan::ScanMessage;
use crate::consumer::Consumer;
use tokio::sync::broadcast;
use utils::error::Result;

/// 日志消费者 - 将扫描结果记录到日志
pub struct LogConsumer;

#[async_trait::async_trait]
impl Consumer for LogConsumer {
    async fn start(
        &mut self, mut receiver: broadcast::Receiver<ScanMessage>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
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
