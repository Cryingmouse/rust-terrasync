use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::SystemTime;
use storage::StorageType;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use utils::error::Result;

mod filter;
mod stats;

pub use filter::{evaluate_filter, parse_filter_expression, FilterCondition, FilterExpression};
pub use stats::{ScanStats, StatsCalculator};

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

/// 辅助函数：评估过滤条件
fn evaluate_filter_conditions(
    expressions: &[FilterExpression], exclude_expressions: &[FilterExpression], file_name: &str,
    file_path: &str, file_type: &str, modified_days: f64, size: u64, extension: &str,
) -> (bool, bool) {
    let matched = expressions.is_empty()
        || expressions.iter().any(|expr| {
            evaluate_filter(
                expr,
                file_name,
                file_path,
                file_type,
                modified_days,
                size,
                extension,
            )
        });

    let excluded = exclude_expressions.iter().any(|expr| {
        evaluate_filter(
            expr,
            file_name,
            file_path,
            file_type,
            modified_days,
            size,
            extension,
        )
    });

    (matched, excluded)
}

/// 辅助函数：发送消息到队列
async fn send_message(tx: &mpsc::Sender<ScanMessage>, message: ScanMessage) -> Result<()> {
    tx.send(message)
        .await
        .map_err(|e| utils::error::Error::with_source("Failed to send message", Box::new(e)))
}

// ============================================================================
// 类型定义 - 枚举、结构体、消息类型
// ============================================================================

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

/// 扫描配置结构体 - 内部使用的完整配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanConfig {
    pub params: ScanParams,
    pub expressions: Vec<FilterExpression>,
    pub exclude_expressions: Vec<FilterExpression>,
}

/// 扫描结果结构体 - 单个文件/目录的信息
#[derive(Debug, Clone, Serialize)]
pub struct ScanResult {
    pub file_name: String,
    pub file_path: String,
    pub is_dir: bool,
    pub is_symlink: bool,
    pub size: u64,
    pub matched: bool,
    pub excluded: bool,
    pub atime: SystemTime,
    pub ctime: SystemTime,
    pub mtime: SystemTime,
}

/// 扫描消息枚举 - 用于队列通信的消息类型
pub enum ScanMessage {
    Result(ScanResult),
    Stats(ScanStats),
    Complete,
}

// ============================================================================
// 核心函数实现
// ============================================================================

/// 主扫描函数 - 入口点
pub async fn scan(params: ScanParams) -> Result<()> {
    log::info!("Starting scan with params: {:?}", params);

    let config = ScanConfig {
        params: params.clone(),
        expressions: parse_expressions(&params.match_expressions)?,
        exclude_expressions: parse_expressions(&params.exclude_expressions)?,
    };

    log::info!("Scan configuration: {:?}", config);

    // 创建队列通道
    let (tx, mut rx) = mpsc::channel::<ScanMessage>(1000);

    // 启动walkdir任务
    let walkdir_handle = tokio::spawn(async move { walkdir(config, tx).await });

    // 处理队列消息
    let start_time = std::time::Instant::now();
    let mut stats = ScanStats::default();

    // 设置显示相关元数据
    stats.command = ScanStats::build_command(&params);
    stats.job_id = params.id.clone().unwrap_or_else(|| "default".to_string());
    stats.log_path = ScanStats::build_log_path();

    // 创建定时器，每10秒输出一次进度
    let mut timer = interval(Duration::from_secs(10));
    let mut _last_files = 0;
    let mut _last_dirs = 0;

    // 等待第一个10秒间隔再输出
    println!("terrasync 3.0.0; (c) 2025 LenovoNetapp, Inc.");

    loop {
        tokio::select! {
            message = rx.recv() => {
                match message {
                    Some(ScanMessage::Result(_)) => {
                        // 结果处理由walkdir内部完成，这里只接收消息
                    }
                    Some(ScanMessage::Stats(s)) => {
                        stats.merge_from(&s);
                        log::info!("Scan progress: {} total files, {} total dirs, {} matched files, {} matched dirs",
                                 stats.total_files, stats.total_dirs, stats.matched_files, stats.matched_dirs);
                    }
                    Some(ScanMessage::Complete) => {
                        log::info!("Scan completed. Stats: {:?}", stats);
                        break;
                    }
                    None => {
                        log::warn!("Channel closed unexpectedly");
                        break;
                    }
                }
            }
            _ = timer.tick() => {
                // 每10秒输出当前进度，即使没有变化也输出
                println!(
                    "[Progress] Scanned: {} files, {} directories (matched: {} files, {} directories)",
                    stats.total_files, stats.total_dirs, stats.matched_files, stats.matched_dirs
                );
                _last_files = stats.total_files;
                _last_dirs = stats.total_dirs;
            }
        }
    }

    // 等待walkdir任务完成
    let _ = walkdir_handle
        .await
        .map_err(|e| utils::error::Error::with_source("Walkdir task failed", Box::new(e)))?;

    // 计算总执行时间
    let duration = start_time.elapsed();
    stats.total_time = format!("{:.2}s", duration.as_secs_f64());

    // 打印ScanStats到console
    println!("\n{}", stats);

    Ok(())
}

/// 目录遍历函数 - 遍历目录并发送结果到队列
pub async fn walkdir(config: ScanConfig, tx: mpsc::Sender<ScanMessage>) -> Result<ScanStats> {
    let scan_path = &config.params.path;
    let calculator = StatsCalculator::new(&config.params.path);
    let mut stats = ScanStats::default();
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

    // 根据存储类型获取对应的遍历器
    let mut rx = match storage_type {
        StorageType::Local(local_storage) => local_storage.walkdir(None, depth).await,
        StorageType::NFS(nfsstorage) => nfsstorage.walkdir(depth).await,
        StorageType::S3(s3_storage) => s3_storage.walkdir(depth).await,
    };

    // 处理每个StorageEntry
    while let Some(entry) = rx.recv().await {
        let file_name = entry.name;
        let file_path = entry.path;

        // 标准化路径分隔符，使用正斜杠跨平台兼容
        let file_path = file_path.replace('\\', "/");

        // 直接从StorageEntry获取文件信息
        let is_dir = entry.is_dir;
        let is_symlink = entry.is_symlink.unwrap_or(false);
        let size = entry.size;

        // 更新基本统计
        if is_dir {
            stats.total_dirs += 1;
        } else {
            stats.total_files += 1;
        }

        // 使用StatsCalculator更新扩展统计信息
        if is_dir {
            let depth = calculator.calculate_depth(Path::new(&file_path));
            calculator.update_dir_stats(&mut stats, &file_name, depth);
        } else {
            calculator.update_file_stats(&mut stats, &file_name, size, is_symlink);
        }

        // 获取文件时间信息
        let atime = entry.accessed.unwrap_or(SystemTime::UNIX_EPOCH);
        let ctime = entry.created.unwrap_or(SystemTime::UNIX_EPOCH);
        let mtime = entry.modified;

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

        // 应用过滤条件
        let (matched, excluded) = evaluate_filter_conditions(
            &config.expressions,
            &config.exclude_expressions,
            &file_name,
            &file_path,
            file_type,
            modified_days,
            size,
            &extension,
        );

        // 创建扫描结果
        let scan_result = ScanResult {
            file_name,
            file_path,
            is_dir,
            is_symlink,
            size,
            matched,
            excluded,
            atime,
            ctime,
            mtime,
        };

        // 如果匹配且未被排除，更新匹配统计
        if matched && !excluded {
            if is_dir {
                stats.matched_dirs += 1;
            } else {
                stats.matched_files += 1;
            }
            log::debug!("Matched file: {:?}", scan_result);
        } else if !config.expressions.is_empty() {
            log::debug!("Filtered file: {:?}", scan_result);
        }

        // 发送ScanResult到队列
        send_message(&tx, ScanMessage::Result(scan_result)).await?;
    }

    // 发送统计信息
    send_message(&tx, ScanMessage::Stats(stats.clone())).await?;

    // 发送完成信号
    send_message(&tx, ScanMessage::Complete).await?;

    Ok(stats)
}
