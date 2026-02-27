import os
import socket
import urllib3
import asyncio
import datetime
import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# ============================================================
# 顶级补丁：禁用代理并强制 IPv4 (必须放在所有 import 之前)
# ============================================================
socket.setdefaulttimeout(30)  # 强制所有 socket 30秒超时
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

# 导入核心配置与模型
from core.database import engine, Base, SessionLocal
from api import user_router, stock_router, holdings_router

# 导入业务服务
from services.stock_service import stock_service
from services.holding_service import holding_service
from services.email_service import email_service
from services.index_service import index_service

# 导入调度管理器（方案二）
try:
    from services.scheduler_manager import scheduler_manager
    SCHEDULER_MANAGER_AVAILABLE = True
except ImportError:
    SCHEDULER_MANAGER_AVAILABLE = False
    print("⚠️ 调度管理器不可用，仅使用主调度器")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('log/system.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 初始化数据库表 (如果表不存在则创建)
Base.metadata.create_all(bind=engine)

# 全局调度器实例（方案一的核心）
main_scheduler = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_scheduler
    # --- 启动时逻辑 ---
    print("\n" + "="*60)
    print("🚀 价值分析系统 V3.0 正在启动...")
    print("="*60)
    logger.info("系统启动开始")
    
    # 确保输出目录存在
    if not os.path.exists("outputs"):
        os.makedirs("outputs")
        logger.info("创建outputs目录")
    
    # 方案一：初始化主调度器（全局变量）
    main_scheduler = AsyncIOScheduler()
    logger.info("主调度器初始化完成")
    
    # 配置核心业务任务
    setup_business_tasks(main_scheduler)
    
    # 启动主调度器
    main_scheduler.start()
    print("✅ 主调度器已启动")
    logger.info("主调度器启动成功")
    
    # 方案二：启动独立监控调度器（如果可用）
    if SCHEDULER_MANAGER_AVAILABLE:
        try:
            scheduler_manager.start()
            print("✅ 监控调度器已启动")
            logger.info("监控调度器启动成功")
        except Exception as e:
            print(f"⚠️ 监控调度器启动失败: {e}")
            logger.error(f"监控调度器启动失败: {e}")
    else:
        print("ℹ️  监控调度器不可用，仅使用主调度器")
    
    # 显示任务概览
    show_scheduler_status()
    
    yield
    
    # --- 关闭时逻辑 ---
    print("\n🛑 系统正在关闭...")
    logger.info("系统关闭开始")
    
    # 关闭主调度器
    if main_scheduler and main_scheduler.running:
        main_scheduler.shutdown()
        print("✅ 主调度器已关闭")
        logger.info("主调度器关闭完成")
    
    # 关闭监控调度器
    if SCHEDULER_MANAGER_AVAILABLE:
        try:
            scheduler_manager.shutdown()
            print("✅ 监控调度器已关闭")
            logger.info("监控调度器关闭完成")
        except Exception as e:
            logger.error(f"监控调度器关闭异常: {e}")
    
    print("✅ 系统已安全停止\n")
    logger.info("系统关闭完成")

def setup_business_tasks(scheduler):
    """配置核心业务任务"""
    logger.info("配置核心业务任务...")
    
    # 任务 A: 每日 15:30 抓取全市场收盘数据
    scheduler.add_job(
        lambda: asyncio.create_task(stock_service.fetch_daily_market_data()),
        CronTrigger(hour=15, minute=30),
        id="sync_market_data",
        name="市场数据抓取",
        misfire_grace_time=3600,  # 允许1小时内补执行
        coalesce=True,
        max_instances=1
    )
    logger.info("✓ 市场数据抓取任务配置完成")
    
    # 任务 B: 每日 16:00 进行全量股票分析评分
    scheduler.add_job(
        lambda: asyncio.create_task(stock_service.analyze_all_watched_stocks()),
        CronTrigger(hour=16, minute=0),
        id="analyze_stocks",
        name="股票分析",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1
    )
    logger.info("✓ 股票分析任务配置完成")
    
    # 任务 C: 每日 16:30 更新所有用户的持仓盈亏
    scheduler.add_job(
        lambda: update_holdings_wrapper(),
        CronTrigger(hour=16, minute=30),
        id="update_holdings",
        name="持仓更新",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1
    )
    logger.info("✓ 持仓更新任务配置完成")
    
    # 任务 D: 每日 18:00 生成报告并发送邮件
    scheduler.add_job(
        lambda: asyncio.create_task(email_service.send_all_daily_reports()),
        CronTrigger(hour=18, minute=0),
        id="send_daily_emails",
        name="邮件报告",
        misfire_grace_time=3600,
        coalesce=True,
        max_instances=1
    )
    logger.info("✓ 邮件报告任务配置完成")
    
    # 任务 E: 每周一凌晨 02:00 同步指数成分股
    scheduler.add_job(
        lambda: asyncio.create_task(index_service.sync_index_constituents()),
        CronTrigger(day_of_week='mon', hour=2, minute=0),
        id="sync_indices",
        name="指数同步",
        misfire_grace_time=7200,  # 周任务允许2小时内补执行
        coalesce=True,
        max_instances=1
    )
    logger.info("✓ 指数同步任务配置完成")
    
    # 添加系统监控任务
    scheduler.add_job(
        system_monitor_task,
        CronTrigger(minute="*/15"),  # 每15分钟监控一次
        id="system_monitor",
        name="系统监控"
    )
    logger.info("✓ 系统监控任务配置完成")

def update_holdings_wrapper():
    """持仓更新包装函数"""
    try:
        db = SessionLocal()
        holding_service.update_all_holdings_profit(db)
        db.close()
        logger.info("持仓更新执行成功")
    except Exception as e:
        logger.error(f"持仓更新执行失败: {e}")

def system_monitor_task():
    """系统监控任务"""
    if main_scheduler:
        jobs = main_scheduler.get_jobs()
        running_jobs = [job for job in jobs if job.next_run_time]
        logger.info(f"📊 系统状态 - 调度器运行中 | 任务总数: {len(jobs)} | 待执行: {len(running_jobs)}")
        
        # 检查关键任务状态
        critical_tasks = ["sync_market_data", "analyze_stocks", "update_holdings"]
        for task_id in critical_tasks:
            job = main_scheduler.get_job(task_id)
            if job:
                status = "✓" if job.next_run_time else "⚠"
                logger.info(f"   {status} {job.name}: 下次执行 {job.next_run_time}")

def show_scheduler_status():
    """显示调度器状态"""
    print("\n📋 调度器配置概览:")
    print("-" * 50)
    
    if main_scheduler:
        jobs = main_scheduler.get_jobs()
        print(f"主调度器状态: 运行中 | 任务数: {len(jobs)}")
        for job in jobs:
            next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M") if job.next_run_time else "无"
            print(f"  • {job.name} ({job.id})")
            print(f"    下次执行: {next_run}")
            print(f"    触发器: {job.trigger}")
    
    if SCHEDULER_MANAGER_AVAILABLE:
        monitor_jobs = scheduler_manager.scheduler.get_jobs()
        print(f"\n监控调度器状态: 运行中 | 任务数: {len(monitor_jobs)}")
        for job in monitor_jobs:
            next_run = job.next_run_time.strftime("%Y-%m-%d %H:%M") if job.next_run_time else "无"
            print(f"  • {job.name} ({job.id}): {next_run}")
    
    print("-" * 50)

# 创建 FastAPI 应用
app = FastAPI(
    title="价值分析系统",
    description="基于 Python 的多维度股票评分与自动报表系统",
    version="3.0",
    lifespan=lifespan
)

# 注册路由
app.include_router(user_router.router)      # 用户注册、登录、个人中心
app.include_router(stock_router.router)     # 关注股、手动抓取、行情查看
app.include_router(holdings_router.router)  # 买入卖出、盈亏统计

# 健康检查端点
@app.get("/")
async def root():
    return {
        "status": "online",
        "version": "3.0",
        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scheduler_running": main_scheduler is not None and main_scheduler.running if main_scheduler else False,
        "docs": "/docs"
    }

# 调度器状态API
@app.get("/scheduler/status")
async def scheduler_status():
    """获取调度器详细状态"""
    if not main_scheduler:
        return {"status": "error", "message": "调度器未初始化"}
    
    jobs_info = []
    for job in main_scheduler.get_jobs():
        jobs_info.append({
            "id": job.id,
            "name": job.name,
            "next_run": job.next_run_time.isoformat() if job.next_run_time else None,
            "trigger": str(job.trigger)
        })
    
    return {
        "status": "running" if main_scheduler.running else "stopped",
        "job_count": len(jobs_info),
        "jobs": jobs_info
    }

# 手动触发任务API
@app.post("/scheduler/trigger/{task_id}")
async def trigger_task(task_id: str):
    """手动触发指定任务"""
    if not main_scheduler:
        return {"status": "error", "message": "调度器未初始化"}
    
    try:
        job = main_scheduler.get_job(task_id)
        if job:
            # 立即执行任务
            if hasattr(job.func, '__call__'):
                if asyncio.iscoroutinefunction(job.func):
                    asyncio.create_task(job.func())
                else:
                    job.func()
            return {"status": "success", "message": f"任务 {task_id} 已触发"}
        else:
            return {"status": "error", "message": f"任务 {task_id} 不存在"}
    except Exception as e:
        return {"status": "error", "message": f"触发任务失败: {str(e)}"}

# 启动入口
if __name__ == "__main__":
    print("🔧 启动参数:")
    print("  - Host: 0.0.0.0")
    print("  - Port: 8000")
    print("  - Reload: True")
    print("  - 访问地址: http://localhost:8000")
    print("  - API文档: http://localhost:8000/docs")
    print("="*60)
    
    # 在命令行运行: python main.py
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)