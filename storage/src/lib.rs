pub mod common;
pub mod file;
pub mod nfs;
pub mod s3;

pub use common::*;
pub use file::*;
pub use nfs::*;
use nfs3_client::nfs3_types::portmap::PMAP_PORT;
pub use s3::*;

use std::path::{Path, PathBuf};

/// 存储类型枚举
pub enum StorageType {
    Local(LocalStorage),
    NFS(NFSStorage),
    S3(S3Storage),
}

/// 根据路径前缀创建对应的存储实例
pub fn create_storage(path: &str) -> Result<StorageType, String> {
    match path {
        p if p.starts_with("nfs://") => create_nfs_storage(&p[6..]),
        p if p.starts_with("s3://") => create_s3_storage(&p[5..]),
        _ => create_local_storage(path),
    }
}

/// 创建NFS存储实例
#[inline]
fn create_nfs_storage(nfs_path: &str) -> Result<StorageType, String> {
    let (server_ip, port, mount_path) = parse_nfs_path(nfs_path);
    let nfs_storage = NFSStorage::new(server_ip, Some(port), Some(mount_path));
    Ok(StorageType::NFS(nfs_storage))
}

/// 创建S3存储实例
#[inline]
fn create_s3_storage(s3_path: &str) -> Result<StorageType, String> {
    let (bucket, region, access_key, secret_key) = parse_s3_config(s3_path)?;
    let s3_storage = S3Storage::new(bucket, region, access_key, secret_key);
    Ok(StorageType::S3(s3_storage))
}

/// 创建本地存储实例
#[inline]
fn create_local_storage(path: &str) -> Result<StorageType, String> {
    let local_path = resolve_local_path(path)?;
    let local_storage = LocalStorage::new(local_path);
    Ok(StorageType::Local(local_storage))
}

/// 解析NFS路径，返回服务器IP和挂载路径
// 增强的NFS路径解析函数
pub fn parse_nfs_path(nfs_path: &str) -> (String, u16, String) {
    // 移除空白字符
    let nfs_path = nfs_path.trim();

    // 检查是否为URL格式 (nfs://server/path)
    if nfs_path.starts_with("nfs://") {
        let without_prefix = &nfs_path[6..]; // 移除 "nfs://"

        // 查找第一个斜杠位置来分离服务器和路径
        if let Some(slash_pos) = without_prefix.find('/') {
            let server_part = &without_prefix[..slash_pos];
            let path_part = &without_prefix[slash_pos..]; // 包含开头的斜杠

            // 解析服务器部分，可能包含端口 (server:port)
            let (server_ip, port) = if let Some(colon_pos) = server_part.find(':') {
                let ip = &server_part[..colon_pos];
                let port_str = &server_part[colon_pos + 1..];
                let port = port_str.parse::<u16>().unwrap_or(PMAP_PORT);
                (ip.to_string(), port)
            } else {
                (server_part.to_string(), PMAP_PORT)
            };

            return (server_ip, port, path_part.to_string());
        }
    }

    // 检查是否为传统格式 (server:port:path)
    let parts: Vec<&str> = nfs_path.split(':').collect();
    if parts.len() >= 3 {
        let server_ip = parts[0].to_string();
        let port = parts[1].parse::<u16>().unwrap_or(PMAP_PORT);
        let path = parts[2..].join(":"); // 处理路径中可能包含的冒号
        return (server_ip, port, path);
    }

    // 检查是否为简写格式 (server:path) - 使用默认端口
    if parts.len() == 2 {
        let server_ip = parts[0].to_string();
        let path = parts[1].to_string();
        return (server_ip, PMAP_PORT, path);
    }

    // 如果只有服务器，使用默认路径和端口
    if !parts.is_empty() && !parts[0].is_empty() {
        return (parts[0].to_string(), PMAP_PORT, "/".to_string());
    }

    panic!(
        "无效的NFS路径格式: {}。支持的格式:\n  - nfs://server/path\n  - nfs://server:port/path\n  - server:port:path\n  - server:path",
        nfs_path
    );
}

/// 解析S3配置，返回bucket和认证信息
fn parse_s3_config(s3_path: &str) -> Result<(String, String, String, String), String> {
    let separator_pos = s3_path.find('/').unwrap_or(s3_path.len());

    let bucket = s3_path[..separator_pos].to_string();

    let region = std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into());
    let access_key = std::env::var("AWS_ACCESS_KEY_ID")
        .map_err(|_| "AWS_ACCESS_KEY_ID environment variable not set")?;
    let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
        .map_err(|_| "AWS_SECRET_ACCESS_KEY environment variable not set")?;

    Ok((bucket, region, access_key, secret_key))
}

/// 解析本地路径，支持相对路径和绝对路径
fn resolve_local_path(path: &str) -> Result<String, String> {
    let path_obj = Path::new(path);

    if path_obj.is_absolute() {
        Ok(path.to_string())
    } else {
        std::env::current_dir()
            .map_err(|e| format!("Failed to get current directory: {}", e))
            .map(|dir| dir.join(path).to_string_lossy().into_owned())
    }
}

/// 存储操作trait
#[async_trait::async_trait]
pub trait Storage {
    async fn list_files(&self, path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>>;
    async fn read_file(&self, path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>>;
    async fn write_file(&self, path: &str, data: &[u8]) -> Result<(), Box<dyn std::error::Error>>;

    /// 递归遍历目录树，返回所有文件路径的异步通道
    async fn walkdir(
        &self, path: Option<PathBuf>, depth: Option<usize>,
    ) -> tokio::sync::mpsc::Receiver<crate::StorageEntry>;
}

// 为StorageType实现统一的接口
#[async_trait::async_trait]
impl Storage for StorageType {
    async fn list_files(&self, path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        match self {
            StorageType::Local(storage) => list_local_files(storage.get_root(), path).await,
            StorageType::NFS(storage) => list_nfs_files(storage, path).await,
            StorageType::S3(_storage) => Ok(Vec::new()),
        }
    }

    async fn read_file(&self, path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        match self {
            StorageType::Local(storage) => read_local_file(storage.get_root(), path).await,
            StorageType::NFS(_storage) => Ok(Vec::new()),
            StorageType::S3(_storage) => Ok(Vec::new()),
        }
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            StorageType::Local(storage) => write_local_file(storage.get_root(), path, data).await,
            StorageType::NFS(_storage) => Ok(()),
            StorageType::S3(_storage) => Ok(()),
        }
    }

    async fn walkdir(
        &self, path: Option<PathBuf>, depth: Option<usize>,
    ) -> tokio::sync::mpsc::Receiver<crate::StorageEntry> {
        match self {
            StorageType::Local(storage) => storage.walkdir(path, depth).await,
            StorageType::NFS(storage) => storage.walkdir(depth).await,
            StorageType::S3(storage) => storage.walkdir(depth).await,
        }
    }
}

/// 本地文件操作：列出文件
#[inline]
async fn list_local_files(
    root: &str, path: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let full_path = PathBuf::from(root).join(path);

    if !full_path.is_dir() {
        return Ok(Vec::new());
    }

    let mut files = Vec::new();
    let mut entries = tokio::fs::read_dir(&full_path).await?;

    while let Some(entry) = entries.next_entry().await? {
        files.push(entry.path().to_string_lossy().into_owned());
    }

    Ok(files)
}

/// NFS文件操作：列出文件
#[inline]
async fn list_nfs_files(
    storage: &NFSStorage, path: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    let mut rx = storage.list_directory(path).await?;

    while let Some(entry) = rx.recv().await {
        files.push(entry.path);
    }

    Ok(files)
}

/// 本地文件操作：读取文件
#[inline]
async fn read_local_file(root: &str, path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let full_path = PathBuf::from(root).join(path);
    tokio::fs::read(&full_path).await.map_err(Into::into)
}

/// 本地文件操作：写入文件
#[inline]
async fn write_local_file(
    root: &str, path: &str, data: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let full_path = PathBuf::from(root).join(path);

    if let Some(parent) = full_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    tokio::fs::write(&full_path, data).await.map_err(Into::into)
}
