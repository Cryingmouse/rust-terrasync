use nfs3_client::nfs3_types::nfs3;
use std::path::PathBuf;
use std::time::SystemTime;

/// 统一的文件系统条目类型，兼容LocalStorage和NFSStorage
#[derive(Debug, Clone)]
pub struct StorageEntry {
    /// 文件或目录的名称
    pub name: String,
    /// 完整路径
    pub path: String,
    /// 是否为目录
    pub is_dir: bool,
    /// 文件大小（字节）
    pub size: u64,
    /// 最后修改时间（统一使用SystemTime）
    pub modified: SystemTime,
    /// 是否为符号链接（仅本地文件系统）
    pub is_symlink: Option<bool>,
    /// 最后访问时间（仅本地文件系统）
    pub accessed: Option<SystemTime>,
    /// 创建时间（仅本地文件系统）
    pub created: Option<SystemTime>,
    /// NFS文件句柄（仅NFS使用）
    pub nfs_fh3: Option<nfs3::nfs_fh3>,
    /// 文件权限模式字符串（如rwxr-xr-x）
    pub mode: Option<String>,
    /// 硬链接数（仅本地文件系统和NFS）
    pub hard_links: Option<u64>,
}

impl StorageEntry {
    /// 转换为路径
    pub fn to_path_buf(&self) -> PathBuf {
        PathBuf::from(&self.path)
    }
}
