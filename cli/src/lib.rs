use clap::{Parser, Subcommand};

mod commands;

#[derive(Parser)]
#[command(name = "rust-terrasync")]
#[command(about = "A Rust-based terrasync application", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Set the logging level (debug, info)
    #[arg(short, long, global = true)]
    pub log_level: Option<String>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run the sync operation
    Sync {
        /// Enable verbose logging
        #[arg(short, long)]
        verbose: bool,

        /// Configuration file path
        #[arg(short, long)]
        config: Option<String>,
    },

    /// Run the scan operation
    Scan {
        /// Scan ID for tracking
        #[arg(short, long)]
        id: Option<String>,

        /// Scan depth level
        #[arg(short, long, default_value = "0")]
        depth: u32,

        /// Directory path to scan
        #[arg(default_value = ".")]
        path: String,

        /// Filter expression to match files/directories
        /// Examples: 'modified<0.5 and "ntap" in name and type==file'
        #[arg(short, long, value_name = "EXPRESSION")]
        r#match: Vec<String>,

        /// Filter expression to exclude files/directories
        /// Examples: 'name=="target" or name==".git"'
        #[arg(short, long, value_name = "EXPRESSION")]
        exclude: Vec<String>,
    },
}

/// 将作业ID转换为文件系统安全的标识符
/// 将特殊字符转换为下划线，确保可用于目录和文件名
pub fn sanitize_job_id(job_id: &str) -> String {
    job_id
        .replace('-', "_")
        .replace('.', "_")
        .replace(' ', "_")
        .replace('/', "_")
        .replace('\\', "_")
}

pub async fn cli_match() -> utils::error::Result<()> {
    let cli = Cli::parse();

    // Execute the subcommand
    match &cli.command {
        Commands::Scan {
            id,
            depth,
            path,
            r#match,
            exclude,
        } => {
            commands::scan_cmd(
                id.clone(),
                *depth,
                path.clone(),
                r#match.clone(),
                exclude.clone(),
            )
            .await?
        }
        Commands::Sync { verbose, config } => commands::sync_cmd(*verbose, config.clone()).await?,
    }

    Ok(())
}
