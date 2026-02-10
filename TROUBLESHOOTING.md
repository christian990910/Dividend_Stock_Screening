# 故障排查指南

## 常见问题和解决方案

### 1. 网络连接错误

#### 问题表现
```
ProxyError: Unable to connect to proxy
HTTPSConnectionPool: Max retries exceeded
ConnectionError
```

#### 原因
- 网络代理配置问题
- 网络不稳定
- 东方财富API访问受限

#### 解决方案

**方案1：禁用代理（推荐）**
```python
# 在config.py中设置
USE_PROXY = False
```

**方案2：配置正确的代理**
```python
# 在config.py中设置
USE_PROXY = True
PROXY_HTTP = "http://your-proxy:port"
PROXY_HTTPS = "https://your-proxy:port"
```

**方案3：增加重试次数和延迟**
```python
# 在config.py中调整
MAX_RETRIES = 5  # 增加到5次
RETRY_DELAY = 5  # 增加到5秒
```

**方案4：降低请求频率**
```python
# 在config.py中调整
DATA_FETCH_INTERVAL = 120  # 改为2分钟
REQUEST_INTERVAL = 60  # 改为60秒
```

### 2. 定时任务重复执行警告

#### 问题表现
```
WARNING - Execution of job skipped: maximum number of running instances reached (1)
```

#### 原因
上一次任务还没执行完，新任务就要开始了

#### 解决方案

**已自动优化：**
- 设置了 `max_instances=1`
- 添加了 `misfire_grace_time`

**如果仍有问题，调整间隔：**
```python
# 在config.py中
DATA_FETCH_INTERVAL = 120  # 增加到2分钟或更长
```

### 3. 股票数据获取失败

#### 问题表现
```
获取股票 600519 数据失败
股票数据为空
```

#### 可能原因
1. 股票代码错误
2. 股票已退市
3. 网络问题
4. API限流

#### 解决方案

**检查股票代码：**
```bash
# 通过API查看当前列表
curl http://localhost:8000/stocks
```

**删除无效股票：**
```bash
curl -X DELETE http://localhost:8000/stocks/错误代码
```

**查看日志：**
```bash
# 日志会显示具体错误原因
```

### 4. 分析结果为空

#### 问题表现
```
GET /analysis/latest 返回空列表
```

#### 原因
1. 还没有执行过分析
2. 分析失败
3. 数据获取失败

#### 解决方案

**手动触发分析：**
```bash
curl -X POST http://localhost:8000/analyze
```

**等待30秒后查看结果：**
```bash
sleep 30
curl http://localhost:8000/analysis/latest
```

**检查数据库：**
```bash
sqlite3 stock_analysis.db "SELECT * FROM daily_analysis;"
```

### 5. CSV文件导出失败

#### 问题表现
```
404: 暂无分析结果
导出失败
```

#### 解决方案

**确保有分析结果：**
```bash
curl http://localhost:8000/analysis/latest
```

**检查output目录：**
```bash
ls -la output/
```

**手动导出：**
```python
from utils import export_to_csv
from database import get_latest_analysis

results = get_latest_analysis()
if results:
    export_to_csv(results)
```

### 6. 数据库锁定

#### 问题表现
```
database is locked
```

#### 解决方案

**重启服务：**
```bash
# 按Ctrl+C停止
# 重新运行
python main.py
```

**如果问题持续，删除数据库：**
```bash
rm stock_analysis.db
# 重启服务会自动重建
```

### 7. 依赖包问题

#### 问题表现
```
ModuleNotFoundError: No module named 'xxx'
ImportError
```

#### 解决方案

**重新安装依赖：**
```bash
pip install -r requirements.txt --upgrade
```

**单独安装缺失的包：**
```bash
pip install akshare --upgrade
pip install fastapi --upgrade
```

### 8. 端口已被占用

#### 问题表现
```
OSError: [Errno 48] Address already in use
```

#### 解决方案

**查找占用端口的进程：**
```bash
lsof -i :8000
```

**杀死进程：**
```bash
kill -9 <PID>
```

**或修改端口：**
```python
# 在config.py中
PORT = 8001  # 改为其他端口
```

## 调试技巧

### 1. 启用详细日志

```python
# 在config.py中
LOG_LEVEL = "DEBUG"  # 改为DEBUG级别
```

### 2. 查看实时日志

```bash
# 运行服务时会在终端显示日志
python main.py
```

### 3. 检查健康状态

```bash
curl http://localhost:8000/health
```

### 4. 使用测试脚本

```bash
python test.py
```

### 5. 检查数据库内容

```bash
# 安装SQLite浏览器
# 或使用命令行
sqlite3 stock_analysis.db

# 查看股票列表
SELECT * FROM stock_list;

# 查看分析结果
SELECT * FROM daily_analysis ORDER BY analysis_date DESC LIMIT 10;

# 查看历史数据
SELECT * FROM historical_data WHERE code='600519' ORDER BY trade_date DESC LIMIT 10;
```

## 性能优化建议

### 1. 减少请求频率

```python
# config.py
DATA_FETCH_INTERVAL = 300  # 5分钟
REQUEST_INTERVAL = 60  # 1分钟
```

### 2. 限制股票数量

建议监控股票数量不超过50只

### 3. 调整分析窗口

```python
# config.py
VOLATILITY_WINDOW = 30  # 减少到30日
HISTORY_DAYS = 180  # 减少到半年
```

### 4. 使用数据库索引

已自动创建索引，无需额外配置

## 联系支持

如果以上方法都无法解决问题，请：

1. 收集错误日志
2. 记录复现步骤
3. 检查config.py配置
4. 查看数据库状态

## 预防措施

### 1. 定期备份数据库

```bash
cp stock_analysis.db stock_analysis_backup_$(date +%Y%m%d).db
```

### 2. 监控日志文件

```bash
# 定期查看日志，及时发现问题
```

### 3. 合理配置参数

```python
# 根据网络情况和股票数量调整
DATA_FETCH_INTERVAL = 60-300秒
REQUEST_INTERVAL = 30-60秒
MAX_RETRIES = 3-5次
```

### 4. 测试新股票

添加新股票前，先测试代码是否有效：

```bash
# 手动测试
curl -X POST "http://localhost:8000/stocks" \
  -H "Content-Type: application/json" \
  -d '{"code": "测试代码", "name": "测试名称"}'

# 立即触发分析看是否有错
curl -X POST "http://localhost:8000/analyze"
```

## 最佳实践

1. **渐进式添加股票**：先添加几只测试，成功后再批量添加
2. **避开高峰期**：避免在交易时间频繁请求
3. **合理设置间隔**：根据监控股票数量调整请求频率
4. **定期清理数据**：删除不需要的历史数据
5. **监控系统资源**：确保服务器有足够的CPU和内存

---

如有其他问题，请查看：
- README.md - 完整文档
- config.py - 配置说明
- 日志输出 - 实时错误信息
