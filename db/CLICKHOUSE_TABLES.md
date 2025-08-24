# ClickHouse表创建功能文档

本文档描述了`ClickHouseDatabase`中新增的表创建功能，这些功能基于Golang示例实现。

## 表结构

### 1. 主扫描表 (scan_base)
存储完整的文件扫描信息，使用`ReplacingMergeTree`引擎处理重复数据。

**字段定义：**
- `path String` - 文件路径
- `size Int64` - 文件大小
- `ext String` - 文件扩展名
- `ctime DateTime` - 创建时间
- `mtime DateTime` - 修改时间
- `atime DateTime` - 访问时间
- `perm UInt32` - 权限
- `is_symlink Bool` - 是否为符号链接
- `is_dir Bool` - 是否为目录
- `is_regular_file Bool` - 是否为普通文件
- `file_handle String` - 目录句柄
- `current_state UInt8` - 当前状态

### 2. 临时扫描表 (temp_files_{uuid})
结构与主表相同，但使用`MergeTree`引擎，表名包含UUID确保唯一性。

### 3. 状态表 (scan_state)
存储扫描状态信息，使用`ReplacingMergeTree`引擎。

**字段定义：**
- `id UInt8` - 状态ID
- `origin_state UInt8` - 原始状态值

## 使用方法

### 基本用法

```rust
use db::clickhouse::ClickHouseDatabase;
use db::config::ClickHouseConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClickHouseConfig {
        dsn: "tcp://localhost:9000".to_string(),
        ..Default::default()
    };

    let db = ClickHouseDatabase::new(config);
    db.ping().await?;

    // 方法1：一次性创建所有表（不包含临时表）
    db.create_all_scan_tables().await?;

    // 方法2：分别创建各个表
    db.create_scan_base_table().await?;
    db.create_scan_state_table().await?;
    
    // 需要时单独创建临时表（返回表名）
    let mut db_with_temp = ClickHouseDatabase::new(config);
    let temp_name = db_with_temp.create_scan_temporary_table().await?;
    println!("临时表名： {}", temp_name);

    Ok(())
}
```

### API参考

#### 主要方法

- `create_scan_base_table()` - 创建主扫描表
- `create_scan_state_table()` - 创建状态表
- `create_scan_temporary_table()` - 创建临时表，返回表名（需要可变引用）
- `create_all_scan_tables()` - 一次性创建所有表（不包含临时表）
- `get_scan_temp_table_name()` - 获取当前临时表名

#### 表名常量

- `SCAN_BASE_TABLE_NAME` - "scan_base"
- `SCAN_STATE_TABLE_NAME` - "scan_state"
- `SCAN_TEMP_TABLE_BASE_NAME` - "temp_files"

## 注意事项

1. **临时表管理**：临时表需要时单独创建，不会通过`create_all_scan_tables()`创建。每次调用`create_scan_temporary_table()`都会生成新的UUID，旧的临时表需要手动清理。

2. **错误处理**：所有方法都返回`Result<()>`，需要适当的错误处理。

3. **连接管理**：确保在使用前调用`initialize()`建立数据库连接。

4. **测试环境**：集成测试需要实际的ClickHouse服务器，默认被忽略。

5. **方法签名差异**：
   - `create_all_scan_tables()` 只需要不可变引用 `&self`
   - `create_scan_temporary_table()` 需要可变引用 `&mut self`，因为它会更新内部状态

## 示例代码

查看以下文件获取完整示例：
- `examples/create_tables.rs` - 基本用法示例
- `tests/test_clickhouse_tables.rs` - 集成测试