pub mod common;
pub mod file;
pub mod nfs;
pub mod s3;

pub use common::*;
pub use file::*;
pub use nfs::*;
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
    let (server_ip, mount_path) = parse_nfs_path(nfs_path)?;
    let nfs_storage = NFSStorage::new(server_ip, None, mount_path);
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
fn parse_nfs_path(nfs_path: &str) -> Result<(String, Option<String>), String> {
    let separator_pos = nfs_path
        .find('/')
        .ok_or("Invalid NFS path format. Expected: nfs://server_ip/path")?;

    let server_ip = &nfs_path[..separator_pos];
    let mount_path = Some(format!("/{}", &nfs_path[separator_pos + 1..]));

    Ok((server_ip.to_string(), mount_path))
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
pub trait StorageOperations {
    async fn list_files(&self, path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>>;
    async fn read_file(&self, path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>>;
    async fn write_file(&self, path: &str, data: &[u8]) -> Result<(), Box<dyn std::error::Error>>;
}

// 为StorageType实现统一的接口
#[async_trait::async_trait]
impl StorageOperations for StorageType {
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
