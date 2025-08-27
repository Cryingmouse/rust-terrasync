use nfs3_client::nfs3_types::nfs3;
use std::{path::PathBuf, time::SystemTime};

/// 统一的文件系统条目类型，兼容LocalStorage和NFSStorage
#[derive(Debug, Clone)]
pub struct StorageEntry {
    /// 文件或目录的名称
    pub name: String,
    /// 完整路径
    pub path: String,

    pub relative_path: String,

    /// 是否为目录
    pub is_dir: bool,
    /// 文件大小（字节）
    pub size: u64,
    pub modified: SystemTime,
    pub accessed: SystemTime,
    pub created: SystemTime,
    /// NFS文件句柄（仅NFS使用）
    pub nfs_fh3: Option<nfs3::nfs_fh3>,
    /// 文件权限模式原始值（Unix权限位）
    pub mode: Option<u32>,
    /// 硬链接数（仅NFS使用）
    pub hard_links: Option<u8>,
    /// 是否为符号链接（仅NFS使用）
    pub is_symlink: Option<bool>,
}

impl StorageEntry {
    /// 转换为路径
    pub fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.path)
    }
}

pub fn get_relative_path(target: &PathBuf, base: &PathBuf) -> String {
    target
        .strip_prefix(&base)
        .ok()
        .map(|p| p.to_string_lossy().to_string())
        .unwrap()
}
