use crate::sanitize_job_id;
use app::scan::{ScanParams, ScanType, scan};
use app::sync::{SyncParams, sync};
use chrono::Local;
use log::info;
use std::fs;
use std::path::Path;

/// 准备job目录和ID
fn prepare_job(job_type: &str, id: Option<String>) -> utils::error::Result<(String, bool)> {
    // 创建jobs目录（如果不存在）
    let jobs_dir = "jobs";
    if !Path::new(jobs_dir).exists() {
        fs::create_dir_all(jobs_dir)?;
    }

    // 生成或处理job ID
    let job_id = id.unwrap_or_else(|| {
        let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
        timestamp
    });
    let job_id = sanitize_job_id(&job_id);

    // 构建job目录路径
    let job_dir = format!("{}/{}_{}", jobs_dir, job_type, job_id);
    let job_path_exists = Path::new(&job_dir).exists();

    // 如果是全量操作，创建目录
    if !job_path_exists {
        fs::create_dir_all(&job_dir)?;
        info!(
            "Created {} directory for full {}: {}",
            job_type, job_type, job_dir
        );
    }

    Ok((job_id, job_path_exists))
}

pub async fn scan_cmd(
    id: Option<String>, depth: u32, path: String, r#match: Vec<String>, exclude: Vec<String>,
) -> utils::error::Result<()> {
    let (job_id, job_path_exists) = prepare_job("scan", id)?;

    // 确定扫描类型
    let scan_type = if job_path_exists {
        ScanType::Incremental
    } else {
        ScanType::Full
    };

    let params = ScanParams {
        id: Some(job_id.clone()),
        scan_type,
        depth,
        path,
        match_expressions: r#match,
        exclude_expressions: exclude,
    };

    scan(params).await?;
    Ok(())
}

pub async fn sync_cmd(
    id: Option<String>, src_path: String, dest_path: String, enable_md5: bool,
    r#match: Vec<String>, exclude: Vec<String>,
) -> utils::error::Result<()> {
    let (job_id, job_path_exists) = prepare_job("sync", id)?;

    // 确定同步类型
    let scan_type = if job_path_exists {
        ScanType::Incremental
    } else {
        ScanType::Full
    };

    let params = SyncParams {
        id: Some(job_id.clone()),
        scan_params: ScanParams {
            id: Some(job_id),
            scan_type,
            depth: 0,
            path: src_path.clone(),
            match_expressions: r#match,
            exclude_expressions: exclude,
        },
        src_path,
        dest_path,
        enable_md5,
    };

    sync(params).await?;
    Ok(())
}
