use crate::scan::ScanMessage;
use crate::consumer::Consumer;
use tokio::sync::broadcast;
use utils::error::Result;

/// 控制台消费者 - 将扫描结果输出到控制台
pub struct ConsoleConsumer;

#[async_trait::async_trait]
impl Consumer for ConsoleConsumer {
    async fn start(
        &mut self, mut receiver: broadcast::Receiver<ScanMessage>,
    ) -> Result<tokio::task::JoinHandle<Result<()>>> {
        let handle = tokio::spawn(async move {
            loop {
                match receiver.recv().await {
                    Ok(ScanMessage::Result(result)) => {
                        log::info!("[ConsoleConsumer] Scan result: {:?}", result);
                    }
                    Ok(ScanMessage::Stats(stats)) => {
                        log::info!("[ConsoleConsumer] Scan stats: {:?}", stats);
                    }
                    Ok(ScanMessage::Complete) => {
                        log::info!("[ConsoleConsumer] Scan completed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        log::warn!("[ConsoleConsumer] Channel closed");
                        break;
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        log::warn!("[ConsoleConsumer] Channel lagged, skipping messages");
                        continue;
                    }
                }
            }
            Ok(())
        });

        Ok(handle)
    }

    fn name(&self) -> &'static str {
        "console_consumer"
    }
}
