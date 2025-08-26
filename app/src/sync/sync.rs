use storage::create_storage;
use crate::scan::{
    FilterExpression, ScanConfig, ScanMessage, ScanParams, parse_expressions, walkdir,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use utils::error::Result;

/// 扫描参数结构体 - 来自CLI的输入参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncParams {
    /// 扫描ID，用于跟踪
    pub id: Option<String>,

    pub scan_params: ScanParams,

    /// 扫描路径（要扫描的目录）
    pub src_path: String,

    /// 扫描路径（要扫描的目录）
    pub dest_path: String,

    /// 检查sum
    pub enable_md5: bool,
}

impl Default for SyncParams {
    fn default() -> Self {
        Self {
            id: None,
            src_path: String::from("."),
            dest_path: String::from("."),
            enable_md5: false,
            scan_params: ScanParams::default(),
        }
    }
}

/// 扫描配置结构体 - 内部使用的完整配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    pub params: SyncParams,
    pub expressions: Vec<FilterExpression>,
    pub exclude_expressions: Vec<FilterExpression>,
}

/// 主扫描函数 - 入口点
pub async fn sync(params: SyncParams) -> Result<()> {
    log::info!("Starting sync with params: {:?}", params);

    let scan_config = ScanConfig {
        params: params.scan_params.clone(),
        expressions: parse_expressions(&params.scan_params.match_expressions)?,
        exclude_expressions: parse_expressions(&params.scan_params.exclude_expressions)?,
    };

    // 创建队列通道
    let (tx, mut rx) = mpsc::channel::<ScanMessage>(1000);

    // 启动walkdir任务（仅生成ScanResults）
    let walkdir_handle = tokio::spawn(async move { walkdir(scan_config, tx).await });

    // 1. 从src_path读取文件内容，写到dest_path里去
    // 1.1 根据传入的src_path 创建storage
    let src_storage = create_storage(&params.src_path)?;
    // 1.2 根据传入的dest_path 创建storage
    let dest_storage = create_storage(&params.dest_path)?;

    loop {
        match rx.recv().await {
            Some(ScanMessage::Result(_result)) => {
                println!("{:?}", _result.file_name);

                // 2. 将_result写入CH数据库
                // 3. broadcast _result 给消费者
            }
            Some(ScanMessage::Complete) => {
                break;
            }
            Some(ScanMessage::Config(_)) => {
                // 忽略配置消息，已在前面的步骤处理
            }
            None => {
                log::warn!("Channel closed unexpectedly");
                break;
            }
        }
    }

    // 等待walkdir任务完成
    let _ = walkdir_handle
        .await
        .map_err(|e| utils::error::Error::with_source("Walkdir task failed", Box::new(e)))?;

    // Stats are now calculated and displayed by ConsoleConsumer
    Ok(())
}
