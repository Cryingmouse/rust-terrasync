# Consumer Framework 使用指南

这个consumer框架提供了一个灵活的方式来注册和管理多个消息消费者，支持基于广播的消费者模型，允许多个消费者同时处理扫描结果。

## 新的广播消费者框架（v2.0）

基于tokio::sync::broadcast实现的消费者管理框架，支持实时消息广播到多个消费者。

### 核心组件

#### 1. ConsumerManager
管理所有消费者的核心类：

```rust
use app::consumer::{ConsumerManager, ConsumerConfig};

// 使用默认配置
let mut manager = ConsumerManager::new();

// 使用自定义配置
let config = ConsumerConfig::all_enabled();
let mut manager = ConsumerManager::with_config(&config);
```

#### 2. Consumer trait
定义消费者接口：

```rust
#[async_trait::async_trait]
pub trait Consumer: Send + Sync {
    async fn start(&mut self, receiver: broadcast::Receiver<ScanMessage>) -> Result<tokio::task::JoinHandle<Result<()>>>;
    fn name(&self) -> &'static str;
}
```

#### 3. 内置消费者
- **LogConsumer**: 日志记录消费者
- **DatabaseConsumer**: 数据库消费者（预留接口）
- **NotificationConsumer**: 通知消费者（预留接口）

### 使用方法

#### 在扫描中使用

扫描函数已经集成了消费者管理器，会自动广播所有扫描结果：

```rust
use app::scan::{ScanParams, scan};

let params = ScanParams {
    path: "/path/to/scan".to_string(),
    depth: 2,
    ..Default::default()
};

// 扫描结果会自动广播给所有注册的消费者
scan(params).await?;
```

#### 自定义消费者配置

```rust
use app::consumer::{ConsumerManager, ConsumerConfig};
use app::scan::{ScanParams, scan};

// 创建自定义配置
let config = ConsumerConfig {
    enable_log_consumer: true,
    enable_database_consumer: true,
    enable_kafka_consumer: false,
    channel_capacity: 5000,
};

// 使用配置创建管理器
let mut manager = ConsumerManager::with_config(&config);
```

#### 消息流

```
扫描任务 → MPSC通道 → 主处理循环 → 广播 → 多个消费者
```

### 配置选项

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| enable_log_consumer | bool | true | 启用日志消费者 |
| enable_database_consumer | bool | false | 启用数据库消费者 |
| enable_kafka_consumer | bool | false | 启用通知消费者 |
| channel_capacity | usize | 10000 | 广播通道容量 |

### 预设配置

- `ConsumerConfig::default()`: 仅启用日志消费者
- `ConsumerConfig::log_only()`: 仅启用日志消费者
- `ConsumerConfig::all_enabled()`: 启用所有消费者

---

# 旧的消费者框架（v1.0 - 已弃用）

这个consumer框架提供了一个灵活的方式来注册和管理多个消息消费者，包括一个专门的数据库消费者，用于对接AppDatabaseManager。

## 核心组件

### 1. Consumer Trait
定义了所有消费者必须实现的接口：

```rust
#[async_trait]
pub trait Consumer: Send + Sync {
    fn name(&self) -> &'static str;
    async fn initialize(&self) -> Result<(), ConsumerError>;
    async fn process(&self, message: Value) -> Result<(), ConsumerError>;
    async fn process_batch(&self, messages: Vec<Value>) -> Result<(), ConsumerError>;
    async fn is_ready(&self) -> bool;
    async fn shutdown(&self) -> Result<(), ConsumerError>;
}
```

### 2. DatabaseConsumer
专门的数据库消费者，对接AppDatabaseManager：

```rust
// 创建数据库消费者
let db_consumer = DatabaseConsumer::builder("database")
    .with_sqlite("./data.db")
    .with_batch_size(100)
    .build();

// 或者使用ClickHouse
let db_consumer = DatabaseConsumer::builder("database")
    .with_clickhouse("tcp://localhost:9000")
    .build();
```

### 3. ConsumerRegistry
用于注册和管理多个消费者：

```rust
let registry = ConsumerRegistry::new();
registry.register(Arc::new(db_consumer)).await?;
registry.register(Arc::new(logging_consumer)).await?;
```

### 4. ConsumerManager
协调所有消费者的消息处理：

```rust
let manager = ConsumerManager::new(100); // 批次大小100
manager.start().await?; // 初始化所有消费者

// 处理消息
let messages = vec![json!({...}), json!({...})];
manager.process_batch(messages).await;

manager.stop().await?; // 关闭所有消费者
```

## 内置消费者

### DatabaseConsumer
- 支持ClickHouse和SQLite
- 自动处理数据库连接和配置
- 批量处理优化

### LoggingConsumer
- 简单的日志记录消费者
- 用于调试和监控

### MetricsConsumer
- 收集处理统计信息
- 消息计数和批次数统计

## 使用示例

### 基本使用

```rust
use app::consumer::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建管理器
    let manager = ConsumerManager::new(50);
    
    // 创建消费者
    let db_consumer = Arc::new(DatabaseConsumer::builder("db")
        .with_sqlite("./app.db")
        .build());
    
    let logger = Arc::new(LoggingConsumer::new("logger"));
    
    // 注册消费者
    manager.registry().register(db_consumer).await?;
    manager.registry().register(logger).await?;
    
    // 启动并处理消息
    manager.start().await?;
    
    let messages = vec![
        json!({"type": "event", "data": "test"}),
        json!({"type": "event", "data": "test2"}),
    ];
    
    manager.process_batch(messages).await;
    
    manager.stop().await?;
    Ok(())
}
```

### 自定义消费者

```rust
use app::consumer::{Consumer, ConsumerError};
use serde_json::Value;

pub struct MyConsumer {
    name: &'static str,
}

#[async_trait::async_trait]
impl Consumer for MyConsumer {
    fn name(&self) -> &'static str { self.name }
    
    async fn process(&self, message: Value) -> Result<(), ConsumerError> {
        // 自定义处理逻辑
        println!("Processing: {}", message);
        Ok(())
    }
    
    // 实现其他必需的方法...
}
```

## 配置选项

### DatabaseConsumer配置

```rust
// 使用配置文件
let config = AppDatabaseConfig {
    enabled: true,
    db_type: "clickhouse".to_string(),
    batch_size: 200000,
    clickhouse: Some(ClickHouseConfig {
        dsn: "tcp://localhost:9000".to_string(),
        ..Default::default()
    }),
    sqlite: None,
};

let consumer = DatabaseConsumer::new("db", config);
```

### ConsumerManager配置

```rust
let manager = ConsumerManager::builder()
    .batch_size(1000)
    .build();
```

## 错误处理

框架提供了统一的错误处理机制：

```rust
let results = manager.process_batch(messages).await;
for (consumer_name, result) in results {
    match result {
        Ok(_) => log::info!("Success: {}", consumer_name),
        Err(e) => log::error!("Error from {}: {}", consumer_name, e),
    }
}
```

## 运行示例

运行提供的示例：

```bash
cargo run --example consumer_example
```

这个示例演示了如何：
1. 创建不同类型的消费者
2. 注册到管理器
3. 处理测试消息
4. 查看统计信息