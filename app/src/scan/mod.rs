use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;
use storage::file::LocalStorage;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use utils::error::Result;

mod filter;
mod stats;

#[cfg(test)]
mod tests;

pub use filter::{evaluate_filter, parse_filter_expression, FilterCondition, FilterExpression};
pub use stats::{ScanStats, StatsCalculator};

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

    // 解析匹配表达式
    let mut expressions = Vec::new();
    for expr in &params.match_expressions {
        let parsed = parse_filter_expression(expr)?;
        log::debug!("Parsed match expression: {:?}", parsed);
        expressions.push(parsed);
    }

    // 解析排除表达式
    let mut exclude_expressions = Vec::new();
    for expr in &params.exclude_expressions {
        let parsed = parse_filter_expression(expr)?;
        log::debug!("Parsed exclude expression: {:?}", parsed);
        exclude_expressions.push(parsed);
    }

    let config = ScanConfig {
        params: params.clone(),
        expressions,
        exclude_expressions,
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

    // 立即输出初始进度
    println!("[Progress] Starting scan...");

    loop {
        tokio::select! {
            message = rx.recv() => {
                match message {
                    Some(ScanMessage::Result(_)) => {
                        // 结果处理由walkdir内部完成，这里只接收消息
                    }
                    Some(ScanMessage::Stats(s)) => {
                        // 合并统计信息，保留显示元数据
                        stats.total_files = s.total_files;
                        stats.total_dirs = s.total_dirs;
                        stats.matched_files = s.matched_files;
                        stats.matched_dirs = s.matched_dirs;
                        stats.total_size = s.total_size;
                        stats.total_symlink = s.total_symlink;
                        stats.total_regular_file = s.total_regular_file;
                        stats.total_name_length = s.total_name_length;
                        stats.max_name_length = s.max_name_length;
                        stats.total_dir_depth = s.total_dir_depth;
                        stats.max_dir_depth = s.max_dir_depth;
                        
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
    walkdir_handle
        .await
        .map_err(|e| utils::error::Error::with_source("Walkdir task failed", Box::new(e)))??;

    // 计算总执行时间
    let duration = start_time.elapsed();
    stats.total_time = format!("{:.2}s", duration.as_secs_f64());

    // 打印ScanStats到console
    println!("\n{}", stats);

    Ok(())
}

/// 目录遍历函数 - 遍历目录并发送结果到队列
pub async fn walkdir(config: ScanConfig, tx: mpsc::Sender<ScanMessage>) -> Result<ScanStats> {
    let scan_path = PathBuf::from(&config.params.path);
    let calculator = StatsCalculator::new(&config.params.path);
    let mut stats = ScanStats::default();
    let depth = if config.params.depth > 0 {
        Some(config.params.depth as usize)
    } else {
        None
    };
    let mut rx = LocalStorage::walkdir_ref(scan_path.clone(), depth).await;

    // 处理每个FileObjectRef
    while let Some(file_ref) = rx.recv().await {
        let file_name = file_ref.name();
        let file_path = file_ref.path().to_string_lossy();

        // 标准化路径分隔符，使用正斜杠跨平台兼容
        let file_path = file_path.replace('\\', "/");

        // 直接从FileObjectRef获取文件信息
        let is_dir = file_ref.is_dir();
        let is_symlink = file_ref.is_symlink();
        let size = file_ref.size();

        // 更新基本统计
        if is_dir {
            stats.total_dirs += 1;
        } else {
            stats.total_files += 1;
        }

        // 使用StatsCalculator更新扩展统计信息
        if is_dir {
            let depth = calculator.calculate_depth(file_ref.path());
            calculator.update_dir_stats(&mut stats, file_name, depth);
        } else {
            calculator.update_file_stats(&mut stats, file_name, size, is_symlink);
        }

        // 获取文件时间信息
        let atime = file_ref.atime();
        let ctime = file_ref.ctime();
        let mtime = file_ref.mtime();

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
        let mut matched = config.expressions.is_empty();
        let mut excluded = false;

        // 检查匹配表达式
        for expr in &config.expressions {
            if evaluate_filter(
                expr,
                file_name,
                file_path.as_ref(),
                file_type,
                modified_days,
                size,
                &extension,
            ) {
                matched = true;
                break;
            }
        }

        // 检查排除表达式
        for expr in &config.exclude_expressions {
            if evaluate_filter(
                expr,
                file_name,
                file_path.as_ref(),
                file_type,
                modified_days,
                size,
                &extension,
            ) {
                excluded = true;
                break;
            }
        }

        // 创建扫描结果
        let scan_result = ScanResult {
            file_name: file_name.to_string(),
            file_path: file_path.to_string(),
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
        tx.send(ScanMessage::Result(scan_result))
            .await
            .map_err(|e| {
                utils::error::Error::with_source("Failed to send scan result", Box::new(e))
            })?;
    }

    // 发送统计信息
    tx.send(ScanMessage::Stats(stats.clone()))
        .await
        .map_err(|e| utils::error::Error::with_source("Failed to send scan stats", Box::new(e)))?;

    // 发送完成信号
    tx.send(ScanMessage::Complete).await.map_err(|e| {
        utils::error::Error::with_source("Failed to send completion signal", Box::new(e))
    })?;

    Ok(stats)
}
