use std::path::PathBuf;
use clap::{Parser, Subcommand, CommandFactory};

mod commands;

use utils::app_config::AppConfig;
use utils::error::Result;
use utils::types::LogLevel;


#[derive(Parser, Debug)]
#[command(
    name = "rust-terrasync",
    author,
    about,
    long_about = "Rust Terrasync CLI",
    version
)]
//TODO: #[clap(setting = AppSettings::SubcommandRequired)]
//TODO: #[clap(global_setting(AppSettings::DeriveDisplayOrder))]
pub struct Cli {
    /// Set a custom config file
    /// TODO: parse(from_os_str)
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<PathBuf>,

    /// Set a custom config file
    #[arg(name="debug", short, long="debug", value_name = "DEBUG")]
    pub debug: Option<bool>,

    /// Set Log Level 
    #[arg(name="log_level", short, long="log-level", value_name = "LOG_LEVEL")]
    pub log_level: Option<LogLevel>,

    /// Subcommands
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[clap(
        name = "scan",
        about = "Perform a scan operation",
        long_about = None, 
    )]
    Scan,
    #[clap(
        name = "sync",
        about = "Perform a sync operation",
        long_about = None, 
    )]
    Sync,
    #[clap(
        name = "config",
        about = "Show Configuration",
        long_about = None,
    )]
    Config,
}

pub fn cli_match() -> Result<()> {
    // Parse the command line arguments
    let cli = Cli::parse();

    // Merge clap config file if the value is set
    AppConfig::merge_config(cli.config.as_deref())?;

    let app = Cli::command();
    let matches = app.get_matches();
    
    AppConfig::merge_args(matches)?;

    // Execute the subcommand
    match &cli.command {
        Commands::Scan => commands::scan()?,        
        Commands::Sync => commands::sync()?,        
        Commands::Config => commands::config()?,    }

    Ok(())
}
