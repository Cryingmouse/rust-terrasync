# Storage Examples

这个目录包含了 storage crate 的使用示例。

## 示例列表

### nfs_walkdir_performance.rs
NFS存储walkdir性能测试示例，用于测量海量文件扫描速度。

#### 运行方式
```bash
cargo run --example nfs_walkdir_performance
```

#### 功能说明
- 连接到指定的NFS服务器
- 扫描目录结构并统计文件和目录数量
- 测量扫描性能（文件/秒）
- 包含超时和计数限制保护
- 输出详细的性能统计信息

#### 配置说明
默认连接到 `nfs://10.131.10.10/mnt/raid0`，如需修改请编辑源码中的 `nfs_path` 变量。