pub mod consumer;
pub mod scan;
pub mod sync;

/// 公共API的prelude模块
/// 用户可以通过 `use app::prelude::*` 来导入最常用的类型
pub mod prelude {
    pub use crate::consumer::config::ConsumerConfig;
    pub use crate::consumer::ConsoleConsumer;
    pub use crate::consumer::Consumer;
    pub use crate::consumer::ConsumerManager;
    pub use crate::consumer::DatabaseConsumer;
    pub use crate::consumer::KafkaConsumer;
    pub use crate::consumer::LogConsumer;
    pub use crate::scan::ScanMessage;
}

use utils::error::Result;

pub fn start() -> Result<()> {
    // does nothing

    Ok(())
}
