use app::consumer::ConsumerManager;
use app::consumer::config::ConsumerConfig;
use app::scan::ScanMessage;
use utils::error::Result;

#[tokio::test]
async fn test_consumer_manager_creation() -> Result<()> {
    // 测试默认配置
    let manager = ConsumerManager::new();
    assert_eq!(manager.get_consumer_count(), 1); // 默认只有日志消费者

    // 测试全启用配置
    let config = ConsumerConfig::all_enabled();
    let manager = ConsumerManager::with_config(&config);
    assert_eq!(manager.get_consumer_count(), 3); // 启用所有消费者

    // 测试仅日志配置
    let config = ConsumerConfig::log_only();
    let manager = ConsumerManager::with_config(&config);
    assert_eq!(manager.get_consumer_count(), 1); // 只有日志消费者

    Ok(())
}

#[tokio::test]
async fn test_scan_with_consumers() -> Result<()> {
    // 初始化日志（使用简单的日志初始化）
    let _ = env_logger::builder().is_test(true).try_init();

    // 创建消费者管理器
    let mut consumer_manager = ConsumerManager::new();
    
    // 启动消费者
    let _handles = consumer_manager.start_consumers().await?;
    
    // 获取广播器
    let broadcaster = consumer_manager.get_broadcaster();
    
    // 测试广播消息
    let message = ScanMessage::Complete;
    let result = broadcaster.send(message);
    assert!(result.is_ok());
    
    // 关闭消费者
    consumer_manager.shutdown().await?;

    Ok(())
}

#[tokio::test]
async fn test_custom_config() -> Result<()> {
    // 初始化日志
    let _ = env_logger::builder().is_test(true).try_init();

    // 测试自定义配置
    let config = ConsumerConfig {
        enable_log_consumer: true,
        enable_database_consumer: false,
        enable_notification_consumer: false,
        channel_capacity: 100,
    };

    let manager = ConsumerManager::with_config(&config);
    assert_eq!(manager.get_consumer_count(), 1);

    // 测试全启用配置
    let config = ConsumerConfig::all_enabled();
    let manager = ConsumerManager::with_config(&config);
    assert_eq!(manager.get_consumer_count(), 3);

    Ok(())
}