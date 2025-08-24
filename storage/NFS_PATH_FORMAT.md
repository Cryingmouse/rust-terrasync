# NFS路径格式指南

## 支持的格式

新的增强NFS路径解析功能支持以下多种格式：

### 1. URL格式（推荐）
```
nfs://server/path
nfs://server:port/path
```

**示例：**
- `nfs://10.131.10.10/mnt/raid0`
- `nfs://192.168.1.100:2049/data/files`

### 2. 传统格式
```
server:port:path
```

**示例：**
- `10.131.10.10:2049:/mnt/raid0`
- `192.168.1.100:2049:/home/user/data`

### 3. 简写格式（使用默认端口2049）
```
server:path
```

**示例：**
- `10.131.10.10:/mnt/raid0`
- `192.168.1.100:/data`

### 4. 仅服务器格式（使用默认端口和根路径）
```
server
```

**示例：**
- `10.131.10.10` → 解析为 `10.131.10.10:2049:/`

## 特殊处理

### 路径中包含冒号
当路径中包含冒号时，使用URL格式或传统格式可以避免解析错误：

```
nfs://server/path:with:colons
server:port:/path:with:colons
```

### 默认端口
如果未指定端口，默认使用 **2049**（NFS标准端口）。

## 使用示例

### 在代码中使用

```rust
use storage::tests::test_walkdir::parse_nfs_path;

// 解析NFS路径
let (server, port, path) = parse_nfs_path("nfs://10.131.10.10/mnt/raid0");
println!("Server: {}, Port: {}, Path: {}", server, port, path);
// 输出: Server: 10.131.10.10, Port: 2049, Path: /mnt/raid0

// 使用解析结果创建NFS存储
let storage = NFSStorage::new(server, Some(port), Some(path));
```

### 环境变量配置

设置环境变量 `NFS_PATH`：

```bash
# Linux/MacOS
export NFS_PATH="nfs://10.131.10.10/mnt/raid0"

# Windows PowerShell
$env:NFS_PATH="nfs://10.131.10.10/mnt/raid0"

# 或者使用传统格式
export NFS_PATH="10.131.10.10:2049:/mnt/raid0"
```

### 支持的格式验证

| 输入格式 | 解析结果 |
|---------|----------|
| `nfs://10.131.10.10/mnt/raid0` | `("10.131.10.10", 2049, "/mnt/raid0")` |
| `nfs://10.131.10.10:2049/mnt/raid0` | `("10.131.10.10", 2049, "/mnt/raid0")` |
| `10.131.10.10:2049:/mnt/raid0` | `("10.131.10.10", 2049, "/mnt/raid0")` |
| `10.131.10.10:/mnt/raid0` | `("10.131.10.10", 2049, "/mnt/raid0")` |
| `10.131.10.10` | `("10.131.10.10", 2049, "/")` |

## 错误处理

如果提供的路径格式无效，函数会返回清晰的错误信息：

```
无效的NFS路径格式: invalid_path。支持的格式:
  - nfs://server/path
  - nfs://server:port/path
  - server:port:path
  - server:path
```

## 向后兼容性

新的解析功能完全向后兼容，原有的格式仍然支持，同时增加了更灵活的URL格式支持。