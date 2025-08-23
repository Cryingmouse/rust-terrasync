use std::future::Future;
use std::pin::Pin;
use std::time::{Duration, UNIX_EPOCH};

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;

use nfs3_client::nfs3_types::nfs3::{self, Nfs3Option};
use nfs3_client::nfs3_types::portmap::PMAP_PORT;
use nfs3_client::nfs3_types::rpc::{auth_unix, opaque_auth};
use nfs3_client::nfs3_types::xdr_codec::Opaque;
use nfs3_client::tokio::TokioConnector;
use nfs3_client::Nfs3ConnectionBuilder;

// 类型别名，简化复杂类型
pub type NfsConnection =
    nfs3_client::Nfs3Connection<nfs3_client::tokio::TokioIo<tokio::net::TcpStream>>;
pub type NfsResult<T> = Result<T, Box<dyn std::error::Error>>;
pub type RecursiveFuture<'a> = Pin<Box<dyn Future<Output = NfsResult<()>> + Send + 'a>>;

#[derive(Debug, Clone)]
pub struct NFSEntry {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub size: u64,
    pub modified: DateTime<Utc>,
    pub nfs_fh3: nfs3::nfs_fh3,
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

    pub async fn list_directory(
        &self, dir_path: &str,
    ) -> NfsResult<mpsc::UnboundedReceiver<crate::StorageEntry>> {
        let (tx, rx) = mpsc::unbounded_channel();
        let dir_path = dir_path.to_string();
        let server_ip = self.server_ip.clone();
        let portmapper_port = self.portmapper_port;

        tokio::spawn(async move {
            if let Err(e) =
                Self::list_directory_internal(&server_ip, portmapper_port, &dir_path, tx).await
            {
                eprintln!("Error in list_directory: {}", e);
            }
        });

        Ok(rx)
    }

    async fn list_directory_internal(
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
                let name = String::from_utf8_lossy(&entry.name.0).to_string();
                let attrs = &entry.name_attributes;
                let (is_dir, size, modified_time) = if let Nfs3Option::Some(attrs) = attrs {
                    let is_dir = matches!(attrs.type_, nfs3::ftype3::NF3DIR);
                    let mtime = &attrs.mtime;
                    let duration = Duration::new(u64::from(mtime.seconds), mtime.nseconds);
                    let systime = UNIX_EPOCH.checked_add(duration).unwrap_or(UNIX_EPOCH);
                    let modified: DateTime<Utc> = systime.into();
                    (is_dir, attrs.size, modified)
                } else {
                    (false, 0, Utc::now())
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
                    name: name.clone(),
                    path: full_path,
                    is_dir,
                    size,
                    modified: {
                        use std::time::{Duration, UNIX_EPOCH};
                        let duration = Duration::new(modified_time.timestamp() as u64, 0);
                        UNIX_EPOCH.checked_add(duration).unwrap_or(UNIX_EPOCH)
                    },
                    is_symlink: None,
                    accessed: None,
                    created: None,
                    nfs_fh3: Some(nfs_fh3.clone()),
                };

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
        self.list_directory("/").await
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

            if let Err(e) = Self::list_directory_recursive_internal(
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

    fn list_directory_recursive_internal<'a>(
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
                    let name = String::from_utf8_lossy(&entry.name.0).to_string();

                    // Skip . and .. entries
                    if name == "." || name == ".." {
                        continue;
                    }

                    let attrs = &entry.name_attributes;
                    let (is_dir, is_symlink, size, modified_time, accessed_time, created_time) =
                        if let Nfs3Option::Some(attrs) = attrs {
                            let file_type = &attrs.type_;
                            let is_dir = matches!(file_type, nfs3::ftype3::NF3DIR);
                            let is_symlink = matches!(file_type, nfs3::ftype3::NF3LNK);

                            // 修改时间 (mtime)
                            let mtime = &attrs.mtime;
                            let modified_duration =
                                Duration::new(u64::from(mtime.seconds), mtime.nseconds);
                            let modified_time = UNIX_EPOCH
                                .checked_add(modified_duration)
                                .unwrap_or(UNIX_EPOCH);
                            let modified: DateTime<Utc> = modified_time.into();

                            // 访问时间 (atime)
                            let atime = &attrs.atime;
                            let accessed_duration =
                                Duration::new(u64::from(atime.seconds), atime.nseconds);
                            let accessed_time = UNIX_EPOCH
                                .checked_add(accessed_duration)
                                .unwrap_or(UNIX_EPOCH);
                            let accessed: DateTime<Utc> = accessed_time.into();

                            // 创建/状态变更时间 (ctime)
                            let ctime = &attrs.ctime;
                            let created_duration =
                                Duration::new(u64::from(ctime.seconds), ctime.nseconds);
                            let created_time = UNIX_EPOCH
                                .checked_add(created_duration)
                                .unwrap_or(UNIX_EPOCH);
                            let created: DateTime<Utc> = created_time.into();

                            (is_dir, is_symlink, attrs.size, modified, accessed, created)
                        } else {
                            (false, false, 0, Utc::now(), Utc::now(), Utc::now())
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
                        name: name.clone(),
                        path: full_path.clone(),
                        is_dir,
                        size,
                        modified: {
                            use std::time::{Duration, UNIX_EPOCH};
                            let duration = Duration::new(modified_time.timestamp() as u64, 0);
                            UNIX_EPOCH.checked_add(duration).unwrap_or(UNIX_EPOCH)
                        },
                        is_symlink: Some(is_symlink),
                        accessed: Some({
                            use std::time::{Duration, UNIX_EPOCH};
                            let duration = Duration::new(accessed_time.timestamp() as u64, 0);
                            UNIX_EPOCH.checked_add(duration).unwrap_or(UNIX_EPOCH)
                        }),
                        created: Some({
                            use std::time::{Duration, UNIX_EPOCH};
                            let duration = Duration::new(created_time.timestamp() as u64, 0);
                            UNIX_EPOCH.checked_add(duration).unwrap_or(UNIX_EPOCH)
                        }),
                        nfs_fh3: Some(nfs_fh3.clone()),
                    };

                    if tx.send(storage_entry).await.is_err() {
                        return Ok(());
                    }

                    // If it's a directory, recurse only if max_depth allows
                    if is_dir && (max_depth == 0 || current_depth < max_depth - 1) {
                        if let Nfs3Option::Some(child_handle) = entry.name_handle.clone() {
                            if let Err(e) = Self::list_directory_recursive_internal(
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
}
