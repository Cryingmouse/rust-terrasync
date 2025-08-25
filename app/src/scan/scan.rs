use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use storage::Storage;
use tokio::sync::mpsc;

/// 将Unix权限位格式化为 rwxrwxrwx 字符串
fn format_permissions(mode: u32) -> String {
    let mut perms = String::with_capacity(9);
    let bit = |m, s| if m != 0 { s } else { "-" };
    perms.push_str(bit(mode & 0o400, "r"));
    perms.push_str(bit(mode & 0o200, "w"));
    perms.push_str(bit(mode & 0o100, "x"));
    perms.push_str(bit(mode & 0o040, "r"));
    perms.push_str(bit(mode & 0o020, "w"));
    perms.push_str(bit(mode & 0o010, "x"));
    perms.push_str(bit(mode & 0o004, "r"));
    perms.push_str(bit(mode & 0o002, "w"));
    perms.push_str(bit(mode & 0o001, "x"));
    perms
}
use utils::error::Result;

use crate::consumer::ConsumerManager;
use crate::scan::filter::{evaluate_filter, parse_filter_expression, FilterExpression};

/// 辅助函数：解析表达式列表
fn parse_expressions(expressions: &[String]) -> Result<Vec<FilterExpression>> {
    expressions
        .iter()
        .map(|expr| {
            let parsed = parse_filter_expression(expr)?;
            log::debug!("Parsed expression: {:?}", parsed);
            Ok(parsed)
        })
        .collect()
}

/// 辅助函数：检查文件是否应该被跳过
fn should_skip_file(
    expressions: &[FilterExpression], exclude_expressions: &[FilterExpression], file_name: &str,
    file_path: &str, file_type: &str, modified_days: f64, size: u64, extension: &str,
) -> bool {
    /// 内部辅助函数：检查表达式列表中是否有匹配的表达式
    fn has_matching_expression(
        expressions: &[FilterExpression], file_name: &str, file_path: &str, file_type: &str,
        modified_days: f64, size: u64, extension: &str,
    ) -> bool {
        expressions.iter().any(|expr| {
            evaluate_filter(
                expr,
                file_name,
                file_path,
                file_type,
                modified_days,
                size,
                extension,
            )
        })
    }

    // 首先检查排除条件：如果有任何排除表达式匹配，则跳过
    if !exclude_expressions.is_empty()
        && has_matching_expression(
            exclude_expressions,
            file_name,
            file_path,
            file_type,
            modified_days,
            size,
            extension,
        )
    {
        return true;
    }

    // 然后检查匹配条件：如果定义了匹配表达式但没有匹配任何，则跳过
    if !expressions.is_empty()
        && !has_matching_expression(
            expressions,
            file_name,
            file_path,
            file_type,
            modified_days,
            size,
            extension,
        )
    {
        return true;
    }

    // 文件不应该被跳过
    false
}

/// 辅助函数：发送消息到队列
async fn send_message(tx: &mpsc::Sender<ScanMessage>, message: ScanMessage) -> Result<()> {
    tx.send(message)
        .await
        .map_err(|e| utils::error::Error::with_source("Failed to send message", Box::new(e)))
}

/// 扫描参数结构体 - 来自CLI的输入参数
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanParams {
    /// 扫描ID，用于跟踪
    pub id: Option<String>,

    /// 扫描深度
    pub depth: u32,

    /// 扫描路径（要扫描的目录）
    pub path: String,

    /// 匹配表达式
    pub match_expressions: Vec<String>,

    /// 排除表达式
    pub exclude_expressions: Vec<String>,

    /// 扫描类型
    pub scan_type: ScanType,
}

impl Default for ScanParams {
    fn default() -> Self {
        Self {
            id: None,
            depth: 1,
            path: String::from("."),
            match_expressions: Vec::new(),
            exclude_expressions: Vec::new(),
            scan_type: ScanType::default(),
        }
    }
}

/// 扫描类型枚举
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ScanType {
    Full,
    Incremental,
}

impl Default for ScanType {
    fn default() -> Self {
        ScanType::Full
    }
}

impl std::fmt::Display for ScanType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScanType::Full => write!(f, "full"),
            ScanType::Incremental => write!(f, "incremental"),
        }
    }
}

/// 扫描配置结构体 - 内部使用的完整配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanConfig {
    pub params: ScanParams,
    pub expressions: Vec<FilterExpression>,
    pub exclude_expressions: Vec<FilterExpression>,
}

/// 扫描结果结构体 - 单个文件/目录的信息
#[derive(Debug, Clone, Serialize)]
pub struct StorageEntity {
    pub file_name: String,
    pub file_path: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub size: u64,
    pub atime: SystemTime,
    pub ctime: SystemTime,
    pub mtime: SystemTime,
    pub mode: Option<u32>,
    pub permissions: Option<String>,
    pub hard_links: Option<u64>,
}

/// 扫描消息枚举 - 用于队列通信的消息类型
#[derive(Debug, Clone)]
pub enum ScanMessage {
    Result(StorageEntity),
    Complete,
    /// 扫描配置信息
    Config(ScanConfig),
}

/// 主扫描函数 - 入口点
pub async fn scan(params: ScanParams) -> Result<()> {
    log::info!("Starting scan with params: {:?}", params);

    let config = ScanConfig {
        params: params.clone(),
        expressions: parse_expressions(&params.match_expressions)?,
        exclude_expressions: parse_expressions(&params.exclude_expressions)?,
    };

    log::info!("Scan configuration: {:?}", config);

    // 创建消费者管理器（使用默认配置）
    let mut consumer_manager = ConsumerManager::new();

    // 启动所有消费者
    let consumer_handles = consumer_manager.start_consumers().await?;

    // 创建队列通道
    let (tx, mut rx) = mpsc::channel::<ScanMessage>(1000);

    // 获取广播发送器
    let broadcaster = consumer_manager.get_broadcaster();

    // 发送配置信息给所有消费者
    if let Err(e) = broadcaster.send(ScanMessage::Config(config.clone())) {
        log::error!("Failed to broadcast scan config: {}", e);
    }

    // 启动walkdir任务（仅生成ScanResults）
    let walkdir_handle = tokio::spawn(async move { walkdir(config, tx).await });

    loop {
        match rx.recv().await {
            Some(ScanMessage::Result(result)) => {
                // 广播扫描结果给所有消费者
                if let Err(e) = broadcaster.send(ScanMessage::Result(result.clone())) {
                    log::error!("Failed to broadcast scan result: {}", e);
                }
            }
            Some(ScanMessage::Complete) => {
                // 广播完成消息给所有消费者，忽略错误
                let _ = broadcaster.send(ScanMessage::Complete);

                break;
            }
            Some(ScanMessage::Config(_)) => {
                // 忽略配置消息，已在前面的步骤处理
            }
            None => {
                log::warn!("Channel closed unexpectedly");
                // 广播完成消息给所有消费者
                let _ = broadcaster.send(ScanMessage::Complete);
                break;
            }
        }
    }

    // 等待walkdir任务完成
    let _ = walkdir_handle
        .await
        .map_err(|e| utils::error::Error::with_source("Walkdir task failed", Box::new(e)))?;

    // 等待所有消费者完成
    for handle in consumer_handles {
        let _ = handle.await;
    }

    // 关闭消费者管理器
    consumer_manager.shutdown().await?;

    // Stats are now calculated and displayed by ConsoleConsumer

    Ok(())
}

/// 目录遍历函数 - 遍历目录并发送结果到队列（简化版本，直接处理）
pub async fn walkdir(config: ScanConfig, tx: mpsc::Sender<ScanMessage>) -> Result<()> {
    let scan_path = &config.params.path;
    let depth = if config.params.depth > 0 {
        Some(config.params.depth as usize)
    } else {
        None
    };

    // 使用storage库的create_storage接口根据路径创建对应的存储类型
    let storage_type = storage::create_storage(scan_path).map_err(|e| {
        utils::error::Error::with_source(
            "Failed to create storage",
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)),
        )
    })?;

    // 使用Storage trait的统一接口获取遍历器
    let mut rx = storage_type.walkdir(None, depth).await;

    // 直接处理每个StorageEntry
    while let Some(entry) = rx.recv().await {
        let file_name = entry.name;
        let mut file_path = entry.path;

        // 标准化路径分隔符，使用正斜杠跨平台兼容
        file_path = file_path.replace('\\', "/");

        // 直接从StorageEntry获取文件信息
        let is_dir = entry.is_dir;
        let is_symlink = entry.is_symlink.unwrap_or(false);
        let size = entry.size;

        // 获取文件时间信息
        let atime = entry.accessed.unwrap_or(SystemTime::UNIX_EPOCH);
        let ctime = entry.created.unwrap_or(SystemTime::UNIX_EPOCH);
        let mtime = entry.modified;

        // 格式化Unix权限
        let permissions_str = entry.mode.map(format_permissions);

        // 计算修改时间（天数）
        let modified_days = {
            let now = SystemTime::now();
            now.duration_since(mtime)
                .map(|duration| duration.as_secs_f64() / 86400.0)
                .unwrap_or(0.0)
        };

        // 获取文件扩展名
        let extension = std::path::Path::new(&file_path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_lowercase();

        let file_type = if is_dir { "dir" } else { "file" };

        // 使用辅助函数检查是否应该跳过该文件
        if should_skip_file(
            &config.expressions,
            &config.exclude_expressions,
            &file_name,
            &file_path,
            file_type,
            modified_days,
            size,
            &extension,
        ) {
            continue;
        }

        // 创建扫描结果
        let scan_result = StorageEntity {
            file_name,
            file_path,
            is_dir,
            is_symlink,
            size,
            atime,
            ctime,
            mtime,
            mode: entry.mode,
            permissions: permissions_str,
            hard_links: entry.hard_links,
        };

        // 直接发送结果到队列
        if let Err(e) = tx.send(ScanMessage::Result(scan_result)).await {
            log::error!("Failed to send scan result: {}", e);
            break;
        }
    }

    // 通道关闭，发送完成消息
    send_message(&tx, ScanMessage::Complete).await?;
    Ok(())
}
