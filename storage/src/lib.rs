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
        p if p.starts_with("nfs://") => create_nfs_storage(&p),
        p if p.starts_with("s3://") => create_s3_storage(&p),
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

/// 解析NFS路径，返回服务器IP、端口和挂载路径
///
/// 支持以下格式：
/// - nfs://server/path
/// - nfs://server:port/path
/// - server:port:path (传统格式)
/// - server:path (简写格式，使用默认端口)
/// - server (仅服务器，使用默认端口和根路径)
///
/// # Arguments
/// * `nfs_path` - NFS路径字符串
///
/// # Returns
/// 返回一个三元组：(服务器IP, 端口, 路径)
///
/// # Panics
/// 如果路径格式无效，将panic并显示支持的格式
pub fn parse_nfs_path(nfs_path: &str) -> (String, u16, String) {
    let nfs_path = nfs_path.trim();

    if nfs_path.is_empty() {
        panic!("无效的NFS路径: 空字符串");
    }

    // 处理nfs://格式的路径
    if let Some(stripped) = nfs_path.strip_prefix("nfs://") {
        return parse_nfs_url_format(stripped);
    }

    // 处理传统格式 (server:port:path 或 server:path)
    parse_nfs_traditional_format(nfs_path)
}

/// 解析nfs://server/path格式的路径
fn parse_nfs_url_format(path_without_prefix: &str) -> (String, u16, String) {
    // 查找第一个斜杠来分离服务器和路径
    let slash_pos = path_without_prefix
        .find('/')
        .unwrap_or_else(|| panic!("无效的NFS URL格式: 缺少路径部分"));

    let server_part = &path_without_prefix[..slash_pos];
    let path_part = &path_without_prefix[slash_pos..];

    // 确保路径以斜杠开头
    if !path_part.starts_with('/') {
        panic!("无效的NFS路径: 路径必须以斜杠开头");
    }

    // 解析服务器和端口
    let (server, port) = parse_server_and_port(server_part);

    (server, port, path_part.to_string())
}

/// 解析传统格式的NFS路径
fn parse_nfs_traditional_format(nfs_path: &str) -> (String, u16, String) {
    let parts: Vec<&str> = nfs_path.split(':').collect();

    match parts.len() {
        0 => panic!("无效的NFS路径: 空字符串"),
        1 => {
            // 只有服务器名，使用默认端口和根路径
            let server = parts[0].trim();
            if server.is_empty() {
                panic!("无效的NFS路径: 服务器名不能为空");
            }
            (server.to_string(), PMAP_PORT, "/".to_string())
        }
        2 => {
            // server:path 格式
            let server = parts[0].trim();
            let path = parts[1].trim();

            if server.is_empty() {
                panic!("无效的NFS路径: 服务器名不能为空");
            }
            if path.is_empty() {
                panic!("无效的NFS路径: 路径不能为空");
            }

            // 确保路径以斜杠开头
            let normalized_path = if path.starts_with('/') {
                path.to_string()
            } else {
                format!("/{}", path)
            };

            (server.to_string(), PMAP_PORT, normalized_path)
        }
        _ => {
            // server:port:path 格式
            let server = parts[0].trim();
            let port_str = parts[1].trim();
            let path = parts[2..].join(":");
            let path = path.trim();

            if server.is_empty() {
                panic!("无效的NFS路径: 服务器名不能为空");
            }
            if port_str.is_empty() {
                panic!("无效的NFS路径: 端口号不能为空");
            }
            if path.is_empty() {
                panic!("无效的NFS路径: 路径不能为空");
            }

            let port = port_str
                .parse::<u16>()
                .unwrap_or_else(|_| panic!("无效的端口号: {}", port_str));

            // 确保路径以斜杠开头
            let normalized_path = if path.starts_with('/') {
                path.to_string()
            } else {
                format!("/{}", path)
            };

            (server.to_string(), port, normalized_path)
        }
    }
}

/// 解析服务器地址和端口
fn parse_server_and_port(server_part: &str) -> (String, u16) {
    let server_part = server_part.trim();
    if server_part.is_empty() {
        panic!("无效的NFS路径: 服务器名不能为空");
    }

    if let Some(colon_pos) = server_part.find(':') {
        let server = server_part[..colon_pos].trim();
        let port_str = server_part[colon_pos + 1..].trim();

        if server.is_empty() {
            panic!("无效的NFS路径: 服务器名不能为空");
        }
        if port_str.is_empty() {
            panic!("无效的NFS路径: 端口号不能为空");
        }

        let port = port_str
            .parse::<u16>()
            .unwrap_or_else(|_| panic!("无效的端口号: {}", port_str));

        (server.to_string(), port)
    } else {
        (server_part.to_string(), PMAP_PORT)
    }
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
    async fn list_dir(&self, path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>>;
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
    async fn list_dir(&self, path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        match self {
            StorageType::Local(storage) => list_local_dir(storage.get_root(), path).await,
            StorageType::NFS(storage) => list_nfs_dir(storage, path).await,
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
async fn list_local_dir(root: &str, path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
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
async fn list_nfs_dir(
    storage: &NFSStorage, path: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    let mut rx = storage.list_dir(path).await?;

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
