use app::scan::{scan, ScanParams};
use utils::app_config::AppConfig;

pub async fn scan_cmd(
    id: Option<String>, depth: u32, path: String, r#match: Vec<String>, exclude: Vec<String>,
) -> utils::error::Result<()> {
    let params = ScanParams {
        id,
        depth,
        path,
        match_expressions: r#match,
        exclude_expressions: exclude,
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
