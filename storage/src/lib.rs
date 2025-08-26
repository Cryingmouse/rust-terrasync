pub mod common;
pub mod file;
pub mod nfs;
pub mod s3;
use common::StorageEntry;

use file::LocalStorage;
use nfs::NFSStorage;
use nfs::parse_nfs_path;
use s3::S3Storage;
use s3::parse_s3_config;

use std::path::PathBuf;

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
    let local_path = std::fs::canonicalize(path)
        .unwrap()
        .to_string_lossy()
        .replace("\\\\?\\", "");
    let local_storage = LocalStorage::new(local_path);
    Ok(StorageType::Local(local_storage))
}

/// 存储操作trait
#[async_trait::async_trait]
pub trait Storage {
    fn get_root(&self) -> &str;
    fn is_local(&self) -> bool;
    /// 递归遍历目录树，返回所有文件路径的异步通道
    async fn walkdir(
        &self, path: Option<PathBuf>, depth: Option<usize>,
    ) -> tokio::sync::mpsc::Receiver<crate::StorageEntry>;
}

// 为StorageType实现统一的接口
#[async_trait::async_trait]
impl Storage for StorageType {
    fn get_root(&self) -> &str {
        match self {
            StorageType::Local(storage) => storage.get_root(),
            StorageType::NFS(_storage) => "/",
            StorageType::S3(_storage) => "bucketname",
        }
    }

    fn is_local(&self) -> bool {
        matches!(self, StorageType::Local(_))
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
