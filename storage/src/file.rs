use crate::common::get_relative_path;
use std::io;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::time::UNIX_EPOCH;

#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// Async section reader for efficient file reading
pub struct AsyncSectionReader {
    file: tokio::fs::File,
    limit: u64,
    current_pos: u64,
}

impl AsyncSectionReader {
    pub async fn new(path: PathBuf, offset: u64, limit: u64) -> io::Result<Self> {
        let mut file = tokio::fs::File::open(path).await?;
        file.seek(SeekFrom::Start(offset)).await?;

        Ok(Self {
            file,
            limit,
            current_pos: 0,
        })
    }

    pub async fn read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.current_pos >= self.limit {
            return Ok(0);
        }

        let remaining = self.limit - self.current_pos;
        let to_read = std::cmp::min(buf.len() as u64, remaining) as usize;

        let bytes_read = self.file.read(&mut buf[..to_read]).await?;
        self.current_pos += bytes_read as u64;

        Ok(bytes_read)
    }
}

impl Drop for AsyncSectionReader {
    fn drop(&mut self) {
        // 文件会通过tokio::fs::File的Drop自动关闭
        // 可以在这里添加额外的清理逻辑
    }
}

pub struct AsyncSectionWriter {
    file: tokio::fs::File,
    limit: u64,
    current_pos: u64,
}

impl AsyncSectionWriter {
    pub async fn new(path: PathBuf, offset: u64, limit: u64) -> io::Result<Self> {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .open(path)
            .await?;
        file.seek(SeekFrom::Start(offset)).await?;

        Ok(Self {
            file,
            limit,
            current_pos: 0,
        })
    }

    pub async fn write_chunk(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.current_pos >= self.limit {
            return Ok(0);
        }

        let remaining = self.limit - self.current_pos;
        let to_write = std::cmp::min(buf.len() as u64, remaining) as usize;

        let bytes_written = self.file.write(&buf[..to_write]).await?;
        self.current_pos += bytes_written as u64;

        Ok(bytes_written)
    }
}

impl Drop for AsyncSectionWriter {
    fn drop(&mut self) {
        // 文件会通过tokio::fs::File的Drop自动关闭
        // 可以在这里添加额外的清理逻辑
    }
}

/// Local storage implementation with async support
pub struct LocalStorage {
    root: String,
}

impl LocalStorage {
    /// Create new local storage instance
    pub fn new(root: String) -> Self {
        Self { root }
    }

    /// Get the root path
    pub fn get_root(&self) -> &str {
        &self.root
    }

    /// 使用统一StorageEntry类型的walkdir版本
    pub async fn walkdir(
        &self, path: Option<PathBuf>, depth: Option<usize>,
    ) -> tokio::sync::mpsc::Receiver<crate::StorageEntry> {
        use walkdir::WalkDir;
        let (tx, rx) = tokio::sync::mpsc::channel(1000); // 缓冲区大小1000

        // 确定要遍历的路径：优先使用传入的path，否则使用self.root
        let target_path = match path {
            Some(p) => p,
            None => PathBuf::from(&self.root),
        };

        tokio::task::spawn_blocking(move || {
            let mut walker = WalkDir::new(&target_path)
                .follow_links(false) // 不跟随符号链接，避免循环
                .max_open(100); // 限制同时打开的文件句柄数

            // 设置遍历深度
            if let Some(max_depth) = depth {
                walker = walker.max_depth(max_depth);
            }

            for entry in walker.into_iter().filter_map(|e| e.ok()) {
                let path_buf = PathBuf::from(entry.path());

                let name = path_buf
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned(); // 转换为String

                let path = path_buf.to_string_lossy().into_owned(); // 转换为String

                if let Ok(info) = entry.metadata() {
                    #[cfg(unix)]
                    let hard_links = info.nlink() as u8;
                    #[cfg(windows)]
                    let hard_links = 1;

                    let storage_entry = crate::StorageEntry {
                        name,
                        path,
                        relative_path: get_relative_path(&path_buf, &target_path),
                        is_dir: info.is_dir(),
                        size: info.len(),
                        is_symlink: Some(info.file_type().is_symlink()),
                        modified: info
                            .modified()
                            .unwrap_or(UNIX_EPOCH)
                            .duration_since(UNIX_EPOCH)
                            .ok()
                            .map(|duration| {
                                duration.as_secs() as i64 * 1000 + duration.subsec_millis() as i64
                            }),
                        accessed: info
                            .accessed()
                            .unwrap_or(UNIX_EPOCH)
                            .duration_since(UNIX_EPOCH)
                            .ok()
                            .map(|duration| {
                                duration.as_secs() as i64 * 1000 + duration.subsec_millis() as i64
                            }),
                        created: info
                            .created()
                            .unwrap_or(UNIX_EPOCH)
                            .duration_since(UNIX_EPOCH)
                            .ok()
                            .map(|duration| {
                                duration.as_secs() as i64 * 1000 + duration.subsec_millis() as i64
                            }),
                        nfs_fh3: None,
                        mode: {
                            #[cfg(unix)]
                            {
                                use std::os::unix::fs::PermissionsExt;
                                Some(info.permissions().mode())
                            }
                            #[cfg(windows)]
                            {
                                Some(if info.permissions().readonly() {
                                    0o444
                                } else {
                                    0o666
                                })
                            }
                        },
                        hard_links: Some(hard_links),
                    };

                    if tx.blocking_send(storage_entry).is_err() {
                        // 如果接收端已关闭，退出循环
                        break;
                    }
                } else {
                    // 忽略无法获取元数据的文件
                    continue;
                }
            }
        });

        rx
    }
}
