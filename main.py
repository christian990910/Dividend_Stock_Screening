import os
import socket
import urllib3

# ============================================================
# 顶级补丁：禁用代理并强制 IPv4 (必须放在所有 import 之前)
# ============================================================
socket.setdefaulttimeout(30) # 强制所有 socket 30秒超时
orig_getaddrinfo = socket.getaddrinfo
def patched_getaddrinfo(*args, **kwargs):
    res = orig_getaddrinfo(*args, **kwargs)
    return [r for r in res if r[0] == socket.AF_INET]
socket.getaddrinfo = patched_getaddrinfo

os.environ['NO_PROXY'] = '*'  # 禁用所有请求的代理
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# ============================================================

import datetime
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
# 2. 导入核心配置与模型
from core.database import engine, Base, SessionLocal
from api import user_router, stock_router, holdings_router

# 3. 导入业务服务
from services.stock_service import stock_service
from services.holding_service import holding_service
from services.email_service import email_service
from services.index_service import index_service

# 初始化数据库表 (如果表不存在则创建)
Base.metadata.create_all(bind=engine)

# 4. FastAPI Lifespan 管理
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- 启动时逻辑 ---
    print("\n" + "="*50)
    print("🚀 价值分析系统 V3.0 正在启动...")
    print("="*50)

    # 确保输出目录存在
    if not os.path.exists("outputs"):
        os.makedirs("outputs")

    # 初始化调度器
    scheduler = AsyncIOScheduler()

    # 任务 A: 每日 15:30 抓取全市场收盘数据
    scheduler.add_job(
        stock_service.fetch_daily_market_data,
        CronTrigger(hour=15, minute=30),
        id="sync_market_data"
    )

    # 任务 B: 每日 16:00 进行全量股票分析评分 (波动率/股息/成长)
    scheduler.add_job(
        stock_service.analyze_all_watched_stocks,
        CronTrigger(hour=16, minute=0),
        id="analyze_stocks"
    )

    # 任务 C: 每日 16:30 更新所有用户的持仓盈亏
    scheduler.add_job(
        lambda: holding_service.update_all_holdings_profit(SessionLocal()),
        CronTrigger(hour=16, minute=30),
        id="update_holdings"
    )

    # 任务 D: 每日 18:00 生成报告并发送邮件
    scheduler.add_job(
        email_service.send_all_daily_reports,
        CronTrigger(hour=18, minute=0),
        id="send_daily_emails"
    )

    # 任务 E: 每周一凌晨 02:00 同步一次指数成分股 (无需频繁同步)
    scheduler.add_job(
        index_service.sync_index_constituents,
        CronTrigger(day_of_week='mon', hour=2, minute=0),
        id="sync_indices"
    )

    scheduler.start()
    print("✅ 定时任务系统已启动 (15:30 行情 / 16:00 分析 / 16:30 盈亏 / 18:00 邮件)")
    
    yield

    # --- 关闭时逻辑 ---
    print("\n🛑 系统正在关闭...")
    scheduler.shutdown()
    print("✅ 任务调度已安全停止\n")

# 5. 创建 FastAPI 应用
app = FastAPI(
    title="价值分析系统",
    description="基于 Python 的多维度股票评分与自动报表系统",
    version="3.0",
    lifespan=lifespan
)

# 6. 注册路由
app.include_router(user_router.router)      # 用户注册、登录、个人中心
app.include_router(stock_router.router)     # 关注股、手动抓取、行情查看
app.include_router(holdings_router.router)  # 买入卖出、盈亏统计

@app.get("/")
async def root():
    return {
        "status": "online",
        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "docs": "/docs"
    }

# 7. 启动入口
if __name__ == "__main__":
    # 在命令行运行: python main.py
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)