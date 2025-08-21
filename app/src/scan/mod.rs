use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;
use storage::file::LocalStorage;
use tokio::sync::mpsc;
use utils::error::Result;

mod filter;

#[cfg(test)]
mod tests;

pub use filter::{evaluate_filter, parse_filter_expression, FilterCondition, FilterExpression};

/// Scan parameters from CLI
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanParams {
    /// Scan ID for tracking
    pub id: Option<String>,

    /// Scan depth
    pub depth: u32,

    /// Scan path (directory to scan)
    pub path: String,

    /// Match expressions
    pub match_expressions: Vec<String>,

    /// Exclude expressions
    pub exclude_expressions: Vec<String>,
}

impl Default for ScanParams {
    fn default() -> Self {
        Self {
            id: None,
            depth: 1,
            path: String::from("."),
            match_expressions: Vec::new(),
            exclude_expressions: Vec::new(),
        }
    }
}

/// Scan configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanConfig {
    pub params: ScanParams,
    pub expressions: Vec<FilterExpression>,
    pub exclude_expressions: Vec<FilterExpression>,
}

/// Scan result
#[derive(Debug, Clone, Serialize)]
pub struct ScanResult {
    pub file_name: String,
    pub file_path: String,
    pub is_dir: bool,
    pub size: u64,
    pub matched: bool,
    pub excluded: bool,
    pub owner: String,
    pub group: String,
    pub has_acl: bool,
}

/// Scan statistics
#[derive(Debug, Clone, Serialize)]
pub struct ScanStats {
    pub total_files: usize,
    pub total_dirs: usize,
    pub matched_files: usize,
    pub matched_dirs: usize,
}

/// Scan message for queue communication
pub enum ScanMessage {
    Result(ScanResult),
    Stats(ScanStats),
    Complete,
}

/// Walk directory and send FileObject and statistics to queue
pub async fn walkdir(config: ScanConfig, tx: mpsc::Sender<ScanMessage>) -> Result<()> {
    let scan_path = PathBuf::from(&config.params.path);
    let mut total_files = 0;
    let mut total_dirs = 0;
    let mut matched_files = 0;
    let mut matched_dirs = 0;

    // 使用storage的walkdir获取FileObject流
    let mut rx = LocalStorage::walkdir(scan_path).await;

    // 处理每个FileObject
    while let Some(file_object) = rx.recv().await {
        let path = file_object.key();
        let file_name = file_object.name().to_string();

        // 标准化路径分隔符，使用正斜杠跨平台兼容
        let file_path = path.replace('\\', "/");

        // 直接从FileObject获取文件信息
        let is_dir = file_object.is_dir();
        let size = file_object.size();

        // 更新总数统计
        if is_dir {
            total_dirs += 1;
        } else {
            total_files += 1;
        }

        // 计算修改时间（天数）
        let modified_days = {
            let now = SystemTime::now();
            now.duration_since(file_object.mtime())
                .map(|duration| duration.as_secs_f64() / 86400.0)
                .unwrap_or(0.0)
        };

        // 获取文件扩展名
        let extension = std::path::Path::new(&path)
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
                &file_name,
                &file_path,
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
                &file_name,
                &file_path,
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
            file_name,
            file_path,
            is_dir,
            size,
            matched,
            excluded,
            owner: file_object.owner(),
            group: file_object.group(),
            has_acl: file_object.acl_info().is_some(),
        };

        // 如果匹配且未被排除，更新匹配统计
        if matched && !excluded {
            if is_dir {
                matched_dirs += 1;
            } else {
                matched_files += 1;
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
    let stats = ScanStats {
        total_files,
        total_dirs,
        matched_files,
        matched_dirs,
    };
    tx.send(ScanMessage::Stats(stats))
        .await
        .map_err(|e| utils::error::Error::with_source("Failed to send scan stats", Box::new(e)))?;

    // 发送完成信号
    tx.send(ScanMessage::Complete).await.map_err(|e| {
        utils::error::Error::with_source("Failed to send completion signal", Box::new(e))
    })?;

    log::info!(
        "Walkdir completed. Found {} matched files and {} matched dirs",
        matched_files,
        matched_dirs
    );
    Ok(())
}

/// Main scan function
pub async fn scan(params: ScanParams) -> Result<usize> {
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
        expressions: expressions.clone(),
        exclude_expressions: exclude_expressions.clone(),
    };

    log::info!("Scan configuration: {:?}", config);

    // 创建队列通道
    let (tx, mut rx) = mpsc::channel::<ScanMessage>(1000);

    // 启动walkdir任务
    let walkdir_handle = tokio::spawn(async move { walkdir(config, tx).await });

    // 处理队列消息
    let mut total_matched = 0;
    let mut stats = ScanStats {
        total_files: 0,
        total_dirs: 0,
        matched_files: 0,
        matched_dirs: 0,
    };

    while let Some(message) = rx.recv().await {
        match message {
            ScanMessage::Result(result) => {
                if result.matched && !result.excluded {
                    total_matched += 1;
                    log::debug!(
                        "Matched file: {} (path: {})",
                        result.file_name,
                        result.file_path
                    );
                }
            }
            ScanMessage::Stats(s) => {
                stats = s;
                log::info!("Scan progress: {} total files, {} total dirs, {} matched files, {} matched dirs",
                         stats.total_files, stats.total_dirs, stats.matched_files, stats.matched_dirs);
            }
            ScanMessage::Complete => {
                log::info!(
                    "Scan completed. Total matched: {}, Stats: {:?}",
                    total_matched,
                    stats
                );
                break;
            }
        }
    }

    // 等待walkdir任务完成
    walkdir_handle
        .await
        .map_err(|e| utils::error::Error::with_source("Walkdir task failed", Box::new(e)))??;

    Ok(total_matched)
}
