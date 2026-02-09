"""
股票分析系统 - FastAPI 主应用
功能：添加股票、定时分析、输出CSV、手动分析
"""
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime, time
import uvicorn
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging

from database import (
    init_db, add_stock_to_list, get_all_stocks, 
    remove_stock_from_list, save_analysis_result,
    get_latest_analysis, save_historical_data
)
from analysis import analyze_stock, fetch_stock_data
from utils import export_to_csv

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="股票分析系统", description="基于FastAPI的股票筛选分析系统")

# 初始化数据库
init_db()

# 创建调度器
scheduler = BackgroundScheduler()


class StockRequest(BaseModel):
    """添加股票请求模型"""
    code: str
    name: Optional[str] = None


class AnalysisResponse(BaseModel):
    """分析结果响应模型"""
    code: str
    name: str
    date: str
    volatility: float
    dividend_yield: float
    pb_percentile: float
    pe_percentile: float
    buy_signal: bool
    reason: str


@app.on_event("startup")
async def startup_event():
    """应用启动时的事件"""
    logger.info("应用启动，初始化定时任务...")
    
    # 每日定时分析（每天早上9点执行）
    scheduler.add_job(
        daily_analysis_job,
        trigger=CronTrigger(hour=9, minute=0),
        id='daily_analysis',
        name='每日股票分析任务',
        replace_existing=True
    )
    
    # 每30秒获取一次数据（用于实时更新）
    scheduler.add_job(
        fetch_data_job,
        trigger='interval',
        seconds=30,
        id='fetch_data',
        name='定时获取股票数据',
        replace_existing=True
    )
    
    scheduler.start()
    logger.info("定时任务已启动")


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时的事件"""
    logger.info("关闭定时任务...")
    scheduler.shutdown()


def daily_analysis_job():
    """每日分析任务"""
    logger.info("开始执行每日分析任务...")
    try:
        stocks = get_all_stocks()
        if not stocks:
            logger.warning("分析列表为空，跳过分析")
            return
        
        results = []
        for stock in stocks:
            try:
                logger.info(f"分析股票: {stock['code']} - {stock['name']}")
                result = analyze_stock(stock['code'], stock['name'])
                if result:
                    save_analysis_result(result)
                    results.append(result)
            except Exception as e:
                logger.error(f"分析股票 {stock['code']} 失败: {str(e)}")
        
        # 导出到CSV
        if results:
            filename = export_to_csv(results)
            logger.info(f"分析完成，结果已导出到: {filename}")
        else:
            logger.warning("没有成功分析的股票")
            
    except Exception as e:
        logger.error(f"每日分析任务执行失败: {str(e)}")


def fetch_data_job():
    """定时获取股票数据任务"""
    try:
        stocks = get_all_stocks()
        for stock in stocks:
            try:
                # 获取并保存历史数据
                data = fetch_stock_data(stock['code'])
                if data is not None and not data.empty:
                    save_historical_data(stock['code'], data)
            except Exception as e:
                logger.error(f"获取股票 {stock['code']} 数据失败: {str(e)}")
    except Exception as e:
        logger.error(f"定时获取数据任务失败: {str(e)}")


@app.get("/")
async def root():
    """根路径"""
    return {
        "message": "股票分析系统API",
        "version": "1.0.0",
        "endpoints": {
            "添加股票": "POST /stocks",
            "获取股票列表": "GET /stocks",
            "删除股票": "DELETE /stocks/{code}",
            "手动分析": "POST /analyze",
            "获取最新分析": "GET /analysis/latest",
            "下载CSV": "GET /export/csv"
        }
    }


@app.post("/stocks", summary="添加股票到分析列表")
async def add_stock(stock: StockRequest):
    """
    添加股票编码到分析列表
    - code: 股票代码（如：600519）
    - name: 股票名称（可选）
    """
    try:
        # 如果没有提供名称，尝试从akshare获取
        name = stock.name
        if not name:
            try:
                data = fetch_stock_data(stock.code)
                if data is not None and not data.empty:
                    name = f"股票_{stock.code}"
                else:
                    name = f"股票_{stock.code}"
            except:
                name = f"股票_{stock.code}"
        
        result = add_stock_to_list(stock.code, name)
        if result:
            logger.info(f"成功添加股票: {stock.code} - {name}")
            return {"message": "添加成功", "code": stock.code, "name": name}
        else:
            raise HTTPException(status_code=400, detail="股票已存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"添加股票失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"添加失败: {str(e)}")


@app.get("/stocks", summary="获取所有股票列表")
async def list_stocks():
    """获取分析列表中的所有股票"""
    try:
        stocks = get_all_stocks()
        return {
            "total": len(stocks),
            "stocks": stocks
        }
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.delete("/stocks/{code}", summary="删除股票")
async def delete_stock(code: str):
    """从分析列表中删除股票"""
    try:
        result = remove_stock_from_list(code)
        if result:
            logger.info(f"成功删除股票: {code}")
            return {"message": "删除成功", "code": code}
        else:
            raise HTTPException(status_code=404, detail="股票不存在")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除股票失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"删除失败: {str(e)}")


@app.post("/analyze", summary="手动执行分析")
async def manual_analysis(background_tasks: BackgroundTasks):
    """
    手动触发分析所有股票
    分析结果会保存到数据库并导出CSV
    """
    try:
        stocks = get_all_stocks()
        if not stocks:
            raise HTTPException(status_code=400, detail="分析列表为空")
        
        # 在后台执行分析任务
        background_tasks.add_task(run_analysis_task)
        
        return {
            "message": "分析任务已启动",
            "total_stocks": len(stocks),
            "status": "processing"
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"启动分析任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"启动失败: {str(e)}")


def run_analysis_task():
    """执行分析任务（后台任务）"""
    logger.info("开始手动分析任务...")
    try:
        stocks = get_all_stocks()
        results = []
        
        for stock in stocks:
            try:
                logger.info(f"分析股票: {stock['code']} - {stock['name']}")
                result = analyze_stock(stock['code'], stock['name'])
                if result:
                    save_analysis_result(result)
                    results.append(result)
            except Exception as e:
                logger.error(f"分析股票 {stock['code']} 失败: {str(e)}")
        
        # 导出到CSV
        if results:
            filename = export_to_csv(results)
            logger.info(f"手动分析完成，结果已导出到: {filename}")
        
    except Exception as e:
        logger.error(f"手动分析任务执行失败: {str(e)}")


@app.get("/analysis/latest", summary="获取最新分析结果")
async def get_analysis():
    """获取最新的分析结果"""
    try:
        results = get_latest_analysis()
        return {
            "total": len(results),
            "date": datetime.now().strftime("%Y-%m-%d"),
            "results": results
        }
    except Exception as e:
        logger.error(f"获取分析结果失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取失败: {str(e)}")


@app.get("/export/csv", summary="下载最新分析结果CSV")
async def download_csv():
    """下载最新的分析结果CSV文件"""
    try:
        results = get_latest_analysis()
        if not results:
            raise HTTPException(status_code=404, detail="暂无分析结果")
        
        filename = export_to_csv(results)
        return FileResponse(
            path=filename,
            filename=f"stock_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
            media_type="text/csv"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"导出CSV失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"导出失败: {str(e)}")


@app.get("/health", summary="健康检查")
async def health_check():
    """系统健康检查"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "scheduler_running": scheduler.running
    }


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
