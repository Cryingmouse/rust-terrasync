use crate::sanitize_job_id;
use app::scan::{scan, ScanParams};
use chrono::Local;
use std::fs;
use std::path::Path;
use utils::app_config::AppConfig;

pub async fn scan_cmd(
    id: Option<String>, depth: u32, path: String, r#match: Vec<String>, exclude: Vec<String>,
) -> utils::error::Result<()> {
    // 创建jobs目录（如果不存在）
    let jobs_dir = "jobs";
    if !Path::new(jobs_dir).exists() {
        fs::create_dir_all(jobs_dir)?;
    }

    // 生成或处理扫描ID
    let scan_id = id.unwrap_or_else(|| {
        let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
        timestamp
    });
    let scan_id = sanitize_job_id(&scan_id);

    // 构建扫描目录路径
    let scan_dir = format!("{}/scan_{}", jobs_dir, scan_id);
    let scan_path_exists = Path::new(&scan_dir).exists();

    // 确定扫描类型
    let scan_type = if scan_path_exists {
        app::scan::ScanType::Incremental
    } else {
        app::scan::ScanType::Full
    };

    // 如果是全量扫描，创建扫描目录
    if !scan_path_exists {
        fs::create_dir_all(&scan_dir)?;
        log::info!("Created scan directory for full scan: {}", scan_dir);
    }

    let params = ScanParams {
        id: Some(scan_id),
        depth,
        path,
        match_expressions: r#match,
        exclude_expressions: exclude,
        scan_type,
    };

    scan(params).await?;
    Ok(())
}

pub async fn sync_cmd(verbose: bool, _config: Option<String>) -> utils::error::Result<()> {
    if verbose {
        std::env::set_var("RUST_LOG", "debug");
    }

    let _config = AppConfig::fetch()?;
    log::info!("Starting sync operation...");

    // TODO: 实现同步逻辑
    log::info!("Sync operation completed");
    Ok(())
}
