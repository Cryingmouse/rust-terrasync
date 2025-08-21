# ACL信息集成到FileObject的说明

## 功能概述

现在`FileObject`结构已经集成了Windows ACL（访问控制列表）信息，通过以下方式提供：

### 新增字段

- `acl_info: Option<FileAclInfo>` - 包含文件的完整ACL信息

### 新增方法

- `acl_info() -> Option<&FileAclInfo>` - 获取ACL信息引用
- `owner() -> String` - 获取文件所有者
- `group() -> String` - 获取文件所属组

### 修改的方法

- `walkdir()` - 现在返回 `tokio::sync::mpsc::Receiver<FileObject>` 而不是 `Receiver<DirEntry>`
- `head()` - 现在返回的 `FileObject` 包含ACL信息
- `root()` - 现在返回的 `FileObject` 包含ACL信息

## 使用示例

### 遍历目录并获取ACL信息

```rust
use storage::LocalStorage;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let storage = LocalStorage::new(".".to_string());
    
    // 使用walkdir获取文件流
    let mut rx = storage.walkdir(PathBuf::from(".")).await;
    
    while let Some(file_object) = rx.recv().await {
        println!("文件: {}", file_object.name());
        println!("路径: {}", file_object.key());
        println!("大小: {} bytes", file_object.size());
        
        // 获取ACL信息
        if let Some(acl) = file_object.acl_info() {
            println!("所有者: {}", acl.owner);
            println!("所属组: {}", acl.group);
            println!("权限条目: {}", acl.permissions.len());
            
            for perm in &acl.permissions {
                println!("  {}: {:?} - {:?}", 
                    perm.account, 
                    perm.access_type, 
                    perm.rights
                );
            }
        }
    }
    
    Ok(())
}
```

### 获取单个文件的ACL信息

```rust
let file_obj = storage.head("Cargo.toml").await?;
println!("所有者: {}", file_obj.owner());
println!("所属组: {}", file_obj.group());

if let Some(acl) = file_obj.acl_info() {
    println!("完整ACL: {:#?}", acl);
}
```

## ACL信息结构

### FileAclInfo

```rust
pub struct FileAclInfo {
    pub owner: String,        // 文件所有者
    pub group: String,        // 文件所属组
    pub permissions: Vec<AclEntry>,  // 权限列表
}
```

### AclEntry

```rust
pub struct AclEntry {
    pub access_type: AclAccessType,  // Allow/Deny/Audit
    pub account: String,             // 用户或组账户
    pub rights: Vec<String>,         // 具体权限列表
    pub is_inherited: bool,          // 是否继承的权限
}
```

### AclAccessType

```rust
pub enum AclAccessType {
    Allow,
    Deny,
    Audit,
}
```

## 平台兼容性

- **Windows**: 使用Windows API获取真实ACL信息
- **非Windows**: 提供占位符实现，返回"Not supported"

## 运行示例

```bash
# 运行walkdir ACL示例
cargo run --example walkdir_acl_example

# 运行简单ACL测试
cargo run --example simple_acl_test
```