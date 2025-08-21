use slog::o;
use slog::Drain;
#[cfg(all(target_os = "linux", feature = "journald"))]
use slog_journald::JournaldDrain;
#[cfg(feature = "syslog")]
use slog_syslog::Facility;

use std::fs::OpenOptions;

use super::error::Result;

pub fn setup_logging() -> Result<slog_scope::GlobalLoggerGuard> {
    // Setup Logging
    let guard = slog_scope::set_global_logger(default_root_logger()?);
    slog_stdlog::init()?;

    Ok(guard)
}

pub fn default_root_logger() -> Result<slog::Logger> {
    // Create terminal drain for stdout output
    let term_drain = default_term_drain().unwrap_or(default_discard()?);
    
    // Create file drain for file output
    let file_drain = default_file_drain().unwrap_or(default_discard()?);
    
    // Combine terminal and file drains
    let drain = slog::Duplicate(term_drain, file_drain).fuse();

    // Merge additional drains based on features
    #[cfg(feature = "syslog")]
    let drain = slog::Duplicate(default_syslog_drain().unwrap_or(default_discard()?), drain).fuse();
    #[cfg(feature = "journald")]
    #[cfg(target_os = "linux")]
    let drain = slog::Duplicate(
        default_journald_drain().unwrap_or(default_discard()?),
        drain,
    )
    .fuse();

    // Create Logger
    let logger = slog::Logger::root(drain, o!());

    // Return Logger
    Ok(logger)
}

fn default_discard() -> Result<slog_async::Async> {
    let drain = slog_async::Async::default(slog::Discard);

    Ok(drain)
}

// term drain: Log to Terminal
#[cfg(not(feature = "termlog"))]
fn default_term_drain() -> Result<slog_async::Async> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let term = slog_term::FullFormat::new(plain)
        .use_file_location()  // 添加文件路径和行号
        .use_custom_timestamp(slog_term::timestamp_local);

    let drain = slog_async::Async::default(term.build().fuse());

    Ok(drain)
}

// term drain: Log to Terminal
#[cfg(feature = "termlog")]
fn default_term_drain() -> Result<slog_async::Async> {
    let plain = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let term = slog_term::FullFormat::new(plain)
        .use_file_location()  // 添加文件路径和行号
        .use_custom_timestamp(slog_term::timestamp_local);

    let drain = slog_async::Async::default(term.build().fuse());

    Ok(drain)
}

// file drain: Log to file
fn default_file_drain() -> Result<slog_async::Async> {
    // 获取当前可执行文件所在目录
    let current_exe = std::env::current_exe()?;
    let mut exe_dir = current_exe;
    exe_dir.pop(); // 移除可执行文件名，得到目录
    
    // 如果无法获取可执行文件目录，使用当前工作目录
    if !exe_dir.exists() {
        exe_dir = std::env::current_dir()?;
    }
    
    // 创建logs子目录
    let log_dir = exe_dir.join("logs");
    
    // Create log directory if it doesn't exist
    std::fs::create_dir_all(&log_dir)?;
    
    let log_file = log_dir.join("app.log");
    
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(log_file)?;
    
    let decorator = slog_term::PlainSyncDecorator::new(file);
    let formatter = slog_term::FullFormat::new(decorator)
        .use_file_location()  // 添加文件路径和行号
        .use_custom_timestamp(slog_term::timestamp_local)
        .build()
        .fuse();
    
    let drain = slog_async::Async::default(formatter);
    
    Ok(drain)
}

// syslog drain: Log to syslog
#[cfg(feature = "syslog")]
fn default_syslog_drain() -> Result<slog_async::Async> {
    let syslog = slog_syslog::unix_3164(Facility::LOG_USER)?;

    let drain = slog_async::Async::default(syslog.fuse());

    Ok(drain)
}

#[cfg(all(target_os = "linux", feature = "journald"))]
fn default_journald_drain() -> Result<slog_async::Async> {
    let journald = JournaldDrain.ignore_res();
    let drain = slog_async::Async::default(journald);

    Ok(drain)
}
