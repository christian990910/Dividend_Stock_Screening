# 股票分析系统 - 完整项目包

## 🎯 项目概述

这是一个基于 FastAPI 的智能股票筛选分析系统，专门用于筛选**波动小、股息率高、有成长价值**的优质股票。

## ✨ 核心功能

### 1️⃣ 股票管理
- ✅ 添加股票到监控列表
- ✅ 查看所有监控股票
- ✅ 删除不需要的股票

### 2️⃣ 自动化分析
- ✅ 每日定时分析（早上9点）
- ✅ 每30秒自动获取数据
- ✅ 自动保存到SQLite数据库

### 3️⃣ 手动分析
- ✅ 随时触发分析
- ✅ 实时查看结果

### 4️⃣ 数据导出
- ✅ 导出CSV报告
- ✅ 下载最新分析结果

## 📊 分析指标

| 指标 | 说明 | 目标值 |
|------|------|--------|
| **波动率** | 60日年化波动率 | < 30% (越低越稳定) |
| **股息率** | 分红收益率 | > 2% (越高越好) |
| **PE百分位** | 市盈率历史位置 | < 40% (估值低) |
| **PB百分位** | 市净率历史位置 | < 40% (估值低) |
| **买入信号** | 综合评分 | ≥ 8分 触发 |

## 🚀 快速开始

### 方式1：使用启动脚本（推荐）
```bash
cd stock_analysis
chmod +x start.sh
./start.sh
```

### 方式2：手动启动
```bash
cd stock_analysis
pip install -r requirements.txt
python main.py
```

### 访问API文档
浏览器打开：`http://localhost:8000/docs`

## 📁 项目文件

```
stock_analysis/
├── main.py                    # FastAPI主应用
├── database.py                # SQLite数据库操作
├── analysis.py                # 股票分析核心逻辑
├── utils.py                   # 工具函数
├── requirements.txt           # 依赖包列表
├── test.py                    # 自动化测试
├── start.sh                   # 启动脚本
├── README.md                  # 完整文档
├── QUICKSTART.md             # 快速指南
└── PROJECT_STRUCTURE.md      # 结构说明
```

## 📖 文档导航

1. **[QUICKSTART.md](QUICKSTART.md)** - 5分钟快速上手
2. **[README.md](README.md)** - 完整使用文档
3. **[PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md)** - 项目结构详解

## 🎬 使用示例

### 1. 添加股票
```bash
curl -X POST "http://localhost:8000/stocks" \
  -H "Content-Type: application/json" \
  -d '{"code": "600519", "name": "贵州茅台"}'
```

### 2. 手动分析
```bash
curl -X POST "http://localhost:8000/analyze"
```

### 3. 查看结果
```bash
curl "http://localhost:8000/analysis/latest"
```

### 4. 下载报告
```bash
curl "http://localhost:8000/export/csv" -o report.csv
```

## 🔧 技术栈

- **Web框架**: FastAPI
- **数据库**: SQLite
- **数据源**: AKShare
- **定时任务**: APScheduler
- **数据分析**: Pandas + NumPy

## 📝 常用股票代码

### 白酒
- 600519 贵州茅台
- 000858 五粮液
- 000568 泸州老窖

### 银行
- 600036 招商银行
- 601166 兴业银行
- 601328 交通银行

### 保险
- 601318 中国平安
- 601601 中国太保
- 601628 中国人寿

### 医药
- 600276 恒瑞医药
- 000661 长春高新

## ⚙️ 定时任务

| 任务 | 执行时间 | 说明 |
|------|---------|------|
| 每日分析 | 每天 9:00 | 分析所有股票并导出CSV |
| 数据获取 | 每30秒 | 获取最新股票数据 |

## 📈 买入信号规则

采用 **13分制** 评分系统：

- 波动率：最高3分
- 股息率：最高3分  
- PE百分位：最高3分
- PB百分位：最高3分
- 成长性：最高1分

**评分 ≥ 8分** 时触发买入信号！

## 🧪 运行测试

```bash
python test.py
```

测试会自动验证所有功能。

## 📦 输出文件

- **数据库**: `stock_analysis.db`
- **CSV报告**: `output/stock_analysis_YYYYMMDD_HHMMSS.csv`

## 🛠️ API端点

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /stocks | 添加股票 |
| GET | /stocks | 获取列表 |
| DELETE | /stocks/{code} | 删除股票 |
| POST | /analyze | 手动分析 |
| GET | /analysis/latest | 最新结果 |
| GET | /export/csv | 下载CSV |
| GET | /health | 健康检查 |

## ⚠️ 免责声明

本系统仅供学习研究使用，不构成投资建议。投资有风险，入市需谨慎！

## 📧 技术支持

遇到问题？检查：
1. 日志输出
2. 数据库内容
3. output目录下的文件

---

**祝您投资顺利！📊💰**
