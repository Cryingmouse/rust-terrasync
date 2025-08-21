use std::fs::Metadata;
use std::io;
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tokio::fs as tokio_fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

const DIR_SUFFIX: &str = "/";

/// Local storage implementation with async support
pub struct LocalStorage {
    root: String,
}

/// File object representation with async capabilities
#[derive(Clone)]
pub struct FileObject {
    info: Metadata,
    path: PathBuf,
    name: String,
}

/// Async section reader for efficient file reading
pub struct AsyncSectionReader {
    file: tokio_fs::File,
    offset: u64,
    limit: u64,
    current_pos: u64,
}

impl AsyncSectionReader {
    pub async fn new(path: PathBuf, offset: u64, limit: u64) -> io::Result<Self> {
        let mut file = tokio_fs::File::open(path).await?;
        file.seek(SeekFrom::Start(offset)).await?;

        Ok(Self {
            file,
            offset,
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

impl FileObject {
    /// Get file name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get full key path
    pub fn key(&self) -> String {
        let path = &self.path;
        if self.is_dir() {
            format!("{}{}", path.display(), DIR_SUFFIX)
        } else {
            path.display().to_string()
        }
    }

    /// Get file size
    pub fn size(&self) -> u64 {
        self.info.len()
    }

    /// Get modification time
    pub fn mtime(&self) -> SystemTime {
        self.info.modified().unwrap_or(SystemTime::UNIX_EPOCH)
    }

    /// Check if it's a directory
    pub fn is_dir(&self) -> bool {
        self.info.is_dir()
    }

    /// Check if it's a symbolic link
    pub fn is_symlink(&self) -> bool {
        self.info.file_type().is_symlink()
    }

    /// Check if it's a regular file
    pub fn is_regular(&self) -> bool {
        self.info.is_file()
    }

    /// Delete the file/directory asynchronously
    pub async fn delete(&self) -> io::Result<()> {
        match tokio_fs::remove_file(&self.path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(_) => tokio_fs::remove_dir_all(&self.path).await.or_else(|e| {
                if e.kind() == io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(e)
                }
            }),
        }
    }

    /// Get full file system path
    pub fn full_path(&self) -> PathBuf {
        self.path.clone()
    }

    /// Get file content asynchronously with offset and limit
    pub async fn get_async(&self, offset: u64, limit: u64) -> io::Result<Vec<u8>> {
        if self.is_dir() || offset > self.size() || self.size() == 0 {
            return Ok(Vec::new());
        }

        let mut file = tokio_fs::File::open(&self.path).await?;
        file.seek(SeekFrom::Start(offset)).await?;
        let actual_limit = std::cmp::min(limit, self.size() - offset);

        let mut buffer = vec![0; actual_limit as usize];
        file.read_exact(&mut buffer).await?;

        Ok(buffer)
    }

    /// Get file access time (cross-platform)
    pub fn atime(&self) -> SystemTime {
        SystemTime::UNIX_EPOCH // Simplified for cross-platform compatibility
    }

    /// Get file mode/permissions
    pub fn mode(&self) -> u32 {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            self.info.permissions().mode()
        }
        #[cfg(windows)]
        {
            self.info.permissions().readonly() as u32
        }
    }

    /// Get file owner UID (cross-platform)
    pub fn uid(&self) -> u32 {
        0 // Simplified for cross-platform compatibility
    }

    /// Get file owner GID (cross-platform)
    pub fn gid(&self) -> u32 {
        0 // Simplified for cross-platform compatibility
    }
}

impl LocalStorage {
    /// Create new local storage instance
    pub fn new(root: String) -> Self {
        Self { root }
    }

    /// Get full path for a key
    pub fn full_path(&self, key: &str) -> PathBuf {
        PathBuf::from(&self.root).join(key)
    }

    /// 使用walkdir的流式版本 - 通过队列返回entry作为生产者
    pub async fn walkdir(path: PathBuf) -> tokio::sync::mpsc::Receiver<walkdir::DirEntry> {
        use walkdir::WalkDir;
        let (tx, rx) = tokio::sync::mpsc::channel(1000); // 缓冲区大小1000

        tokio::task::spawn_blocking(move || {
            let walker = WalkDir::new(&path)
                .follow_links(false) // 不跟随符号链接，避免循环
                .max_open(100); // 限制同时打开的文件句柄数

            for entry in walker.into_iter().filter_map(|e| e.ok()) {
                if let Err(_) = tx.blocking_send(entry) {
                    // 如果接收端已关闭，退出循环
                    break;
                }
            }
        });

        rx
    }

    /// Get file metadata asynchronously
    pub async fn head(&self, key: &str) -> io::Result<FileObject> {
        let path = self.full_path(key);
        let metadata = tokio_fs::metadata(&path).await?;

        let name = Path::new(key)
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();

        Ok(FileObject {
            info: metadata,
            path,
            name,
        })
    }

    /// Get file content asynchronously
    pub async fn get(&self, key: &str) -> io::Result<Vec<u8>> {
        let obj = self.head(key).await?;
        if obj.is_dir() || obj.size() == 0 {
            return Ok(Vec::new());
        }

        let mut file = tokio_fs::File::open(&obj.path).await?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        Ok(buffer)
    }

    /// Put file content asynchronously
    pub async fn put(&self, key: &str, content: &[u8], src: Option<&FileObject>) -> io::Result<()> {
        let p = self.full_path(key);

        let _perm = if let Some(src_obj) = src {
            src_obj.mode()
        } else {
            0o644
        };

        if key.ends_with(DIR_SUFFIX) || key.is_empty() {
            tokio_fs::create_dir_all(&p).await?;
            return Ok(());
        }

        // Create parent directories if needed
        if let Some(parent) = p.parent() {
            if !tokio_fs::try_exists(parent).await? {
                tokio_fs::create_dir_all(parent).await?;
            }
        }

        let mut file = tokio_fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&p)
            .await?;

        file.write_all(content).await?;

        Ok(())
    }

    /// Delete file/directory asynchronously
    pub async fn delete(&self, key: &str) -> io::Result<()> {
        let path = self.full_path(key);
        match tokio_fs::remove_file(&path).await {
            Ok(_) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(_) => tokio_fs::remove_dir_all(&path).await.or_else(|e| {
                if e.kind() == io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(e)
                }
            }),
        }
    }

    /// Copy file asynchronously
    pub async fn copy(&self, src: &FileObject, dest_key: &str) -> io::Result<()> {
        let src_path = &src.path;
        let dest_path = self.full_path(dest_key);

        // Create parent directories if needed
        if let Some(parent) = dest_path.parent() {
            if !tokio_fs::try_exists(parent).await? {
                tokio_fs::create_dir_all(parent).await?;
            }
        }

        tokio_fs::copy(src_path, dest_path).await?;
        Ok(())
    }

    /// Get root object asynchronously
    pub async fn root(&self) -> io::Result<FileObject> {
        let path = PathBuf::from(&self.root);
        let metadata = tokio_fs::metadata(&path).await?;

        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();

        Ok(FileObject {
            info: metadata,
            path,
            name,
        })
    }

    /// Check if path exists asynchronously
    pub async fn exists(&self, key: &str) -> io::Result<bool> {
        let path = self.full_path(key);
        tokio_fs::try_exists(&path).await
    }

    /// Get file size asynchronously
    pub async fn size(&self, key: &str) -> io::Result<u64> {
        let obj = self.head(key).await?;
        Ok(obj.size())
    }
}
