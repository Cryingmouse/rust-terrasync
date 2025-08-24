# Consumer Framework 使用指南

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