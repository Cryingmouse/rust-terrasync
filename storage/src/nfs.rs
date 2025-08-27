use std::path::PathBuf;

use std::future::Future;
use std::pin::Pin;

use std::time::SystemTime;
use tokio::sync::mpsc;

use nfs3_client::Nfs3ConnectionBuilder;
use nfs3_client::nfs3_types::nfs3::{self, Nfs3Option};
use nfs3_client::nfs3_types::portmap::PMAP_PORT;
use nfs3_client::nfs3_types::rpc::{auth_unix, opaque_auth};
use nfs3_client::nfs3_types::xdr_codec::Opaque;
use nfs3_client::tokio::TokioConnector;

use crate::common::get_relative_path;
use crate::seconds_nanos_to_systemtime;

// 类型别名，简化复杂类型
pub type NfsConnection =
    nfs3_client::Nfs3Connection<nfs3_client::tokio::TokioIo<tokio::net::TcpStream>>;
pub type NfsResult<T> = Result<T, Box<dyn std::error::Error>>;
pub type RecursiveFuture<'a> = Pin<Box<dyn Future<Output = NfsResult<()>> + Send + 'a>>;

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

pub fn from_secs_nsecs_to_nsecs(seconds: u32, nseconds: u32) -> Option<i64> {
    // 1. 先将 seconds 安全地转换为 i64
    let secs_i64: i64 = seconds.into();

    // 2. 计算 seconds 代表的纳秒数，检查乘法溢出
    let secs_as_nanos = secs_i64.checked_mul(1_000_000_000)?;

    // 3. 加上 nseconds，检查加法溢出
    secs_as_nanos.checked_add(nseconds.into())
}

/// 将纳秒数(i64)转换回秒和纳秒
///
/// # Arguments
/// * `nanos` - 纳秒数
///
/// # Returns
/// 成功时返回(seconds, nseconds)元组，失败时返回None
pub fn from_nanos_to_secs_nsecs(nanos: i64) -> Option<(u32, u32)> {
    // 检查纳秒数是否为负数
    if nanos < 0 {
        return None;
    }

    // 计算秒数：纳秒数除以1_000_000_000
    let seconds = nanos / 1_000_000_000;
    // 计算剩余纳秒数：纳秒数对1_000_000_000取模
    let remaining_nanos = nanos % 1_000_000_000;

    // 安全地将结果转换为u32类型
    match (seconds.try_into().ok(), remaining_nanos.try_into().ok()) {
        (Some(secs), Some(nsecs)) => Some((secs, nsecs)),
        _ => None,
    }
}

pub struct NFSStorage {
    server_ip: String,
    portmapper_port: u16,
    path: Option<String>,
}

impl NFSStorage {
    pub fn new(server_ip: String, portmapper_port: Option<u16>, path: Option<String>) -> Self {
        let portmapper_port = portmapper_port.unwrap_or(PMAP_PORT);

        Self {
            server_ip,
            portmapper_port,
            path,
        }
    }

    pub async fn list_dir(
        &self, dir_path: &str,
    ) -> NfsResult<mpsc::UnboundedReceiver<crate::StorageEntry>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let dir_path = dir_path.to_string();
        let server_ip = self.server_ip.clone();
        let portmapper_port = self.portmapper_port;

        tokio::spawn(async move {
            if let Err(e) =
                Self::list_dir_internal(&server_ip, portmapper_port, &dir_path, tx).await
            {
                eprintln!("Error in list_directory: {}", e);
            }
        });

        Ok(rx)
    }

    async fn list_dir_internal(
        server_ip: &str, portmapper_port: u16, dir_path: &str,
        tx: mpsc::UnboundedSender<crate::StorageEntry>,
    ) -> NfsResult<()> {
        let auth_unix = auth_unix {
            stamp: 0xaaaa_aaaa,
            machinename: Opaque::borrowed(b"unknown"),
            uid: 0xffff_fffe,
            gid: 0xffff_fffe,
            gids: vec![],
        };
        let credential = opaque_auth::auth_unix(&auth_unix);

        let mut connection = Nfs3ConnectionBuilder::new(TokioConnector, server_ip, dir_path)
            .portmapper_port(portmapper_port)
            .credential(credential)
            .mount()
            .await?;

        let dir_handle = connection.root_nfs_fh3();

        let mut cookie = nfs3::cookie3::default();
        let mut cookieverf = nfs3::cookieverf3::default();

        loop {
            let readdirplus = connection
                .readdirplus(&nfs3::READDIRPLUS3args {
                    dir: dir_handle.clone(),
                    cookie,
                    cookieverf,
                    maxcount: 128 * 1024,
                    dircount: 128 * 1024,
                })
                .await?
                .unwrap();

            let dir_entries = readdirplus.reply.entries.into_inner();
            for entry in &dir_entries {
                let storage_entry = Self::build_storage_entry_detailed(entry, dir_path)?;
                if tx.send(storage_entry).is_err() {
                    break;
                }
            }

            if readdirplus.reply.eof {
                break;
            }

            cookie = dir_entries.last().expect("entries list is empty").cookie;
            cookieverf = readdirplus.cookieverf;
        }

        Ok(())
    }

    pub async fn list_root(&self) -> NfsResult<mpsc::UnboundedReceiver<crate::StorageEntry>> {
        self.list_dir("/").await
    }

    pub async fn walkdir(
        &self, depth: Option<usize>,
    ) -> tokio::sync::mpsc::Receiver<crate::StorageEntry> {
        let (tx, rx) = tokio::sync::mpsc::channel(1000);
        let dir_path = self.path.clone().unwrap_or_else(|| "/".to_string());

        let server_ip = self.server_ip.clone();
        let portmapper_port = self.portmapper_port;
        let max_depth = depth.unwrap_or(0); // 0 means scan all depths

        tokio::spawn(async move {
            let auth_unix = auth_unix {
                stamp: 0xaaaa_aaaa,
                machinename: Opaque::borrowed(b"unknown"),
                uid: 0xffff_fffe,
                gid: 0xffff_fffe,
                gids: vec![],
            };
            let credential = opaque_auth::auth_unix(&auth_unix);

            let mut connection =
                match Nfs3ConnectionBuilder::new(TokioConnector, &server_ip, &dir_path)
                    .portmapper_port(portmapper_port)
                    .credential(credential)
                    .mount()
                    .await
                {
                    Ok(conn) => conn,
                    Err(e) => {
                        eprintln!("Error mounting NFS connection: {}", e);
                        return;
                    }
                };

            let dir_handler = connection.root_nfs_fh3();

            if let Err(e) = Self::list_dir_recursive_internal(
                &mut connection,
                &dir_path,
                &dir_handler,
                tx,
                0, // current depth starts at 0
                max_depth,
            )
            .await
            {
                eprintln!("Error in recursive directory listing: {}", e);
            }

            connection.unmount().await.ok();
        });

        rx
    }

    fn list_dir_recursive_internal<'a>(
        connection: &'a mut NfsConnection, dir_path: &'a str, dir_handle: &'a nfs3::nfs_fh3,
        tx: tokio::sync::mpsc::Sender<crate::StorageEntry>, current_depth: usize, max_depth: usize,
    ) -> RecursiveFuture<'a> {
        Box::pin(async move {
            let mut cookie = nfs3::cookie3::default();
            let mut cookieverf = nfs3::cookieverf3::default();

            loop {
                let readdirplus = connection
                    .readdirplus(&nfs3::READDIRPLUS3args {
                        dir: dir_handle.clone(),
                        cookie,
                        cookieverf,
                        maxcount: 128 * 1024,
                        dircount: 128 * 1024,
                    })
                    .await?
                    .unwrap();

                let dir_entries = readdirplus.reply.entries.into_inner();
                for entry in &dir_entries {
                    // Skip . and .. entries
                    let name = String::from_utf8_lossy(&entry.name.0).to_string();
                    if name == "." || name == ".." {
                        continue;
                    }

                    let storage_entry = Self::build_storage_entry_detailed(entry, dir_path)?;
                    let is_dir = storage_entry.is_dir;
                    let full_path = storage_entry.path.clone();

                    if tx.send(storage_entry).await.is_err() {
                        return Ok(());
                    }

                    // If it's a directory, recurse only if max_depth allows
                    if is_dir && (max_depth == 0 || current_depth < max_depth - 1) {
                        if let Nfs3Option::Some(child_handle) = entry.name_handle.clone() {
                            if let Err(e) = Self::list_dir_recursive_internal(
                                connection,
                                &full_path,
                                &child_handle,
                                tx.clone(),
                                current_depth + 1,
                                max_depth,
                            )
                            .await
                            {
                                eprintln!("Error listing directory {}: {}", full_path, e);
                            }
                        }
                    }
                }

                if readdirplus.reply.eof {
                    break;
                }

                cookie = dir_entries.last().expect("entries list is empty").cookie;
                cookieverf = readdirplus.cookieverf;
            }

            Ok(())
        })
    }

    pub fn server_ip(&self) -> &str {
        &self.server_ip
    }

    pub fn portmapper_port(&self) -> u16 {
        self.portmapper_port
    }

    pub fn path(&self) -> Option<&str> {
        self.path.as_deref()
    }

    /// 统一的StorageEntry构建函数，用于list_dir_internal和list_dir_recursive_internal
    /// 保留必要的时间转换和路径处理，但移除Unix权限格式化
    fn build_storage_entry_detailed(
        entry: &nfs3::entryplus3, dir_path: &str,
    ) -> NfsResult<crate::StorageEntry> {
        let name = String::from_utf8_lossy(&entry.name.0).to_string();
        let attrs = &entry.name_attributes;
        let (
            is_dir,
            is_symlink,
            size,
            modified_time,
            accessed_time,
            created_time,
            mode,
            hard_links,
        ) = if let Nfs3Option::Some(attrs) = attrs {
            let file_type = &attrs.type_;
            let is_dir = matches!(file_type, nfs3::ftype3::NF3DIR);
            let is_symlink = matches!(file_type, nfs3::ftype3::NF3LNK);

            // 创建时间 (ctime)
            let ctime = &attrs.ctime;
            let created_time = seconds_nanos_to_systemtime(ctime.seconds, ctime.nseconds);

            // 修改时间 (mtime)
            let mtime = &attrs.mtime;
            let modified_time = seconds_nanos_to_systemtime(mtime.seconds, mtime.nseconds);

            // 访问时间 (atime)
            let atime = &attrs.atime;
            let accessed_time = seconds_nanos_to_systemtime(atime.seconds, atime.nseconds);

            // 解析mode字段 - Unix文件权限原始值
            let mode = attrs.mode;
            // 硬链接数
            let hard_links = attrs.nlink as u8;

            (
                is_dir,
                is_symlink,
                attrs.size,
                modified_time,
                accessed_time,
                created_time,
                mode,
                hard_links,
            )
        } else {
            (
                false,
                false,
                0,
                SystemTime::UNIX_EPOCH,
                SystemTime::UNIX_EPOCH,
                SystemTime::UNIX_EPOCH,
                0o644,
                1,
            )
        };

        let nfs_fh3 = match &entry.name_handle {
            Nfs3Option::Some(handle) => handle.clone(),
            Nfs3Option::None => nfs3::nfs_fh3::default(),
        };

        let full_path = if dir_path.ends_with('/') {
            format!("{}{}", dir_path, name)
        } else {
            format!("{}/{}", dir_path, name)
        };

        let storage_entry = crate::StorageEntry {
            name,
            path: full_path.clone(),
            relative_path: get_relative_path(&PathBuf::from(full_path), &PathBuf::from(dir_path)),
            is_dir,
            size,
            is_symlink: Some(is_symlink),
            created: created_time,
            modified: modified_time,
            accessed: accessed_time,
            nfs_fh3: Some(nfs_fh3),
            // Unix权限原始值，格式化移至消费者循环
            mode: Some(mode),
            hard_links: Some(hard_links),
        };

        Ok(storage_entry)
    }
}
