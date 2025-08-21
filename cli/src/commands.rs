use app::scan;
use app::sync;

use utils::app_config::AppConfig;
use utils::error::Result;

/// Perform a scan operation
pub fn scan() -> Result<()> {
    // Log this sync operation
    log::info!("Performing scan operation");

    scan::scan()?;

    Ok(())
}

/// Show the configuration file
pub fn config() -> Result<()> {
    let config = AppConfig::fetch()?;
    println!("{:#?}", config);

    Ok(())
}

/// Perform a sync operation
pub fn sync() -> Result<()> {
    // Log this sync operation
    log::info!("Performing sync operation");

    // Perform sync
    sync::sync()?;

    Ok(())
}
