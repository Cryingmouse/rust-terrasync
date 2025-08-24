@echo off
echo 启动ClickHouse服务器...

echo 检查Docker是否安装...
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Docker未安装，请先安装Docker Desktop
    pause
    exit /b 1
)

echo 启动ClickHouse容器...
docker run -d --name clickhouse-server-test ^
  -p 9000:9000 -p 8123:8123 ^
  -e CLICKHOUSE_DB=default ^
  -e CLICKHOUSE_USER=default ^
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 ^
  clickhouse/clickhouse-server

echo 等待ClickHouse启动...
timeout /t 10 /nobreak >nul

echo 验证ClickHouse连接...
docker exec clickhouse-server-test clickhouse-client --query "SELECT 1" >nul 2>&1
if %errorlevel% neq 0 (
    echo ClickHouse启动失败，请检查Docker日志
    docker logs clickhouse-server-test
    pause
    exit /b 1
)

echo ClickHouse服务器已成功启动！
echo 地址: localhost:9000 (TCP) / localhost:8123 (HTTP)
echo 用户: default
echo 密码: (空)
echo 数据库: default
pause