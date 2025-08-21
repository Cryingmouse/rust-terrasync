use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;
use storage::file::LocalStorage;
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
}

/// Walk directory and filter entries based on ScanConfig
pub async fn walkdir(config: ScanConfig) -> Result<Vec<ScanResult>> {
    let scan_path = PathBuf::from(&config.params.path);
    let mut results = Vec::new();

    // 使用storage的walkdir获取entry流
    let mut rx = LocalStorage::walkdir(scan_path).await;

    // 处理每个entry
    while let Some(entry) = rx.recv().await {
        let path = entry.path();
        let file_name = entry.file_name().to_string_lossy().into_owned();

        // 标准化路径分隔符，使用正斜杠跨平台兼容
        let file_path = path.to_string_lossy().replace('\\', "/");

        // 获取文件元数据
        let metadata = match entry.metadata() {
            Ok(meta) => meta,
            Err(e) => {
                log::warn!("Failed to get metadata for {}: {}", file_path, e);
                continue;
            }
        };

        let is_dir = metadata.is_dir();
        let size = metadata.len();

        // 计算修改时间（天数）
        let modified_days = match metadata.modified() {
            Ok(modified) => {
                let now = SystemTime::now();
                now.duration_since(modified)
                    .map(|duration| duration.as_secs_f64() / 86400.0)
                    .unwrap_or(0.0)
            }
            Err(_) => 0.0,
        };

        // 获取文件扩展名
        let extension = path
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

        // 如果匹配且未被排除，添加到结果
        if matched && !excluded {
            // 对于目录，确保路径以斜杠结尾
            let final_path = if is_dir && !file_path.ends_with('/') {
                format!("{}/", file_path)
            } else {
                file_path
            };

            results.push(ScanResult {
                file_name,
                file_path: final_path,
                is_dir,
                size,
                matched: true,
                excluded: false,
            });
        } else if !config.expressions.is_empty() {
            // 记录未匹配或被排除的文件（用于调试）
            log::debug!(
                "File {}: matched={}, excluded={}",
                file_path,
                matched,
                excluded
            );
        }
    }

    log::info!("Walkdir completed. Found {} matching files", results.len());
    Ok(results)
}

/// Main scan function
pub async fn scan(params: ScanParams) -> Result<Vec<ScanResult>> {
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

    // 使用walkdir进行流式处理
    walkdir(config).await
}
