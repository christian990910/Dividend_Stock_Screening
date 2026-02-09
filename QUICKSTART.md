# 快速使用指南

## 5分钟快速上手

### 第一步：安装依赖

```bash
cd stock_analysis
pip install -r requirements.txt
```

### 第二步：启动服务

**方式1：使用启动脚本（推荐）**
```bash
chmod +x start.sh
./start.sh
```

**方式2：直接运行**
```bash
python main.py
```

启动成功后，你会看到：
```
INFO:     Uvicorn running on http://0.0.0.0:8000
INFO:     Application startup complete.
```

### 第三步：访问API文档

打开浏览器，访问：`http://localhost:8000/docs`

你会看到一个交互式的API文档界面（Swagger UI）

### 第四步：添加股票

在API文档页面，找到 `POST /stocks` 接口：

1. 点击 "Try it out"
2. 在请求体中输入：
```json
{
  "code": "600519",
  "name": "贵州茅台"
}
```
3. 点击 "Execute"

或者使用命令行：
```bash
curl -X POST "http://localhost:8000/stocks" \
  -H "Content-Type: application/json" \
  -d '{"code": "600519", "name": "贵州茅台"}'
```

### 第五步：查看股票列表

访问：`http://localhost:8000/stocks`

或使用API文档中的 `GET /stocks` 接口

### 第六步：手动执行分析

在API文档中找到 `POST /analyze` 接口，点击执行

或使用命令行：
```bash
curl -X POST "http://localhost:8000/analyze"
```

### 第七步：查看分析结果

等待30秒后，访问：`http://localhost:8000/analysis/latest`

或下载CSV报告：`http://localhost:8000/export/csv`

## 常用股票代码

### 白酒板块
- 600519: 贵州茅台
- 000858: 五粮液
- 000568: 泸州老窖
- 603589: 口子窖

### 银行板块
- 600036: 招商银行
- 601166: 兴业银行
- 600000: 浦发银行
- 601328: 交通银行

### 保险板块
- 601318: 中国平安
- 601601: 中国太保
- 601628: 中国人寿

### 医药板块
- 600276: 恒瑞医药
- 000661: 长春高新
- 300347: 泰格医药

## 自动化测试

运行测试脚本来验证所有功能：

```bash
python test.py
```

## 定时任务说明

系统启动后会自动运行两个定时任务：

1. **每日分析任务**：每天早上9:00执行
2. **数据获取任务**：每30秒执行一次

你可以在日志中看到任务执行情况。

## 查看输出

### CSV文件位置
所有分析报告都保存在 `output/` 目录下

### 数据库文件
SQLite数据库文件：`stock_analysis.db`

可以使用任何SQLite客户端查看，例如：
- DB Browser for SQLite
- DBeaver
- DataGrip

## 常见问题

### 1. 如何批量添加股票？

创建一个Python脚本：

```python
import requests

stocks = [
    {"code": "600519", "name": "贵州茅台"},
    {"code": "601318", "name": "中国平安"},
    {"code": "000858", "name": "五粮液"}
]

for stock in stocks:
    response = requests.post(
        "http://localhost:8000/stocks",
        json=stock
    )
    print(f"{stock['name']}: {response.json()}")
```

### 2. 如何修改分析时间？

编辑 `main.py` 文件，找到：

```python
scheduler.add_job(
    daily_analysis_job,
    trigger=CronTrigger(hour=9, minute=0),  # 修改这里
    ...
)
```

### 3. 如何调整请求频率？

编辑 `analysis.py` 文件，修改：

```python
REQUEST_INTERVAL = 30  # 改为你需要的秒数
```

### 4. 分析结果说明

**波动率**：数值越小，股价越稳定
- < 20%: 很稳定
- 20-30%: 较稳定
- > 40%: 波动较大

**股息率**：数值越高，分红越多
- > 4%: 非常好
- 2-4%: 较好
- < 1%: 较低

**PE/PB百分位**：数值越低，估值越便宜
- < 20%: 历史低位
- 20-40%: 较低水平
- > 60%: 较高水平

**买入信号**：评分≥8分时触发

## 停止服务

在终端中按 `Ctrl + C` 停止服务

## 下一步

- 查看 `README.md` 了解详细文档
- 探索 API 文档中的所有接口
- 根据需求定制分析策略
- 添加更多股票到监控列表

## 技术支持

如有问题，请检查：
1. 日志输出
2. 数据库内容
3. output目录下的CSV文件

Happy Investing! 📈
