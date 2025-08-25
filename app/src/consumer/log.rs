use crate::consumer::Consumer;
use crate::scan::ScanMessage;
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
                    Ok(ScanMessage::Result(_result)) => {
                    }
                    Ok(ScanMessage::Complete) => {
                        break;
                    }
                    Ok(ScanMessage::Config(_)) => {
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
        "log_consumer"
    }
}
