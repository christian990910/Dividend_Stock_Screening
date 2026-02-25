import os
import re
import pandas as pd
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, desc

from core.database import get_db
from core.auth_dependency import get_current_user
from models.user import User
from models.stock import (
    DailyMarketData, UserStockWatch, StockAnalysisResult, 
    HistoricalData, DividendData
)
from services.stock_service import stock_service
import crud.stock as crud_stock

router = APIRouter(prefix="/stocks", tags=["股票数据与分析"])

# ============================================================
# 1. 关注列表管理 (绑定当前用户)
# ============================================================

@router.post("/watch/add")
def add_watch_stock(stock_codes: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """
    批量添加关注股票
    参数示例: "600036, 000001, 300750"
    """
    codes = re.findall(r'\d{6}', stock_codes)
    added = 0
    for code in set(codes):
        # 使用 crud 层逻辑
        success = crud_stock.add_to_watchlist(db, current_user.user_id, code)
        if success:
            added += 1
    return {"status": "success", "message": f"已为 {current_user.nickname} 添加 {added} 只股票"}

@router.delete("/watch/remove/{stock_code}")
def remove_watch_stock(stock_code: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """移除单只关注股票"""
    result = db.query(UserStockWatch).filter(
        UserStockWatch.user_id == current_user.user_id,
        UserStockWatch.stock_code == stock_code
    ).delete()
    db.commit()
    if result > 0:
        return {"status": "success", "message": f"已移除 {stock_code}"}
    raise HTTPException(status_code=404, detail="关注记录不存在")

@router.get("/watch/list")
def list_watch_stocks(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """获取当前用户的关注列表及基本行情"""
    watches = db.query(UserStockWatch).filter(UserStockWatch.user_id == current_user.user_id).all()
    return {
        "user": current_user.nickname,
        "count": len(watches),
        "stocks": [{"code": w.stock_code, "added_at": w.added_at} for w in watches]
    }

# ============================================================
# 2. 数据抓取与维护 (全局)
# ============================================================

@router.post("/data/fetch/market")
async def fetch_all_market_data(force: bool = False):
    """手动触发：抓取全市场 5000+ 股票实时行情"""
    return await stock_service.fetch_daily_market_data(force=force)

@router.post("/data/fetch/history/{stock_code}")
async def fetch_individual_history(stock_code: str):
    """手动同步特定股票的历史K线数据(用于计算波动率)"""
    await stock_service.fetch_historical_data(stock_code)
    return {"status": "success", "message": f"{stock_code} 历史数据同步完成"}

@router.post("/data/fetch/dividend")
async def fetch_dividend_news():
    """触发分红数据同步任务"""
    await stock_service.fetch_dividend_data()
    return {"status": "success", "message": "分红数据同步任务已启动"}

# ============================================================
# 3. 智能分析逻辑
# ============================================================

@router.post("/analyze/all-watched")
async def analyze_all_watched(background_tasks: BackgroundTasks):
    """后台任务：对系统中所有用户关注的股票进行评分分析"""
    background_tasks.add_task(stock_service.analyze_all_watched_stocks)
    return {"status": "success", "message": "全量分析任务已在后台排队"}

@router.post("/analyze/stock/{stock_code}")
async def analyze_single_stock(stock_code: str, db: Session = Depends(get_db)):
    """立即分析单只股票并返回结果"""
    # 确保有最新K线
    await stock_service.fetch_historical_data(stock_code)
    score = await stock_service.analyze_stock(stock_code, db)
    if score is not None:
        return {"stock_code": stock_code, "total_score": score}
    raise HTTPException(status_code=404, detail="无法获取分析所需的基础数据")

# ============================================================
# 4. 数据导出 (CSV 报告)
# ============================================================

def _build_analysis_df(results: List[StockAnalysisResult]):
    """辅助方法：构建 DataFrame"""
    data = []
    for r in results:
        data.append({
            "股票代码": r.stock_code,
            "股票名称": r.stock_name,
            "分析日期": str(r.analysis_date),
            "评分": r.total_score,
            "建议": r.suggestion,
            "最新价": r.latest_price,
            "市盈率": r.pe_ratio,
            "市净率": r.pb_ratio,
            "波动率%": r.volatility_30d,
            "股息率%": r.dividend_yield,
            "ROE%": r.roe
        })
    return pd.DataFrame(data)

@router.get("/export/global")
def export_global_report(db: Session = Depends(get_db)):
    """【全局分析】导出全数据库中所有股票的最新的分析结果"""
    # 子查询找到每个股票最新的分析日期
    subq = db.query(
        StockAnalysisResult.stock_code,
        func.max(StockAnalysisResult.analysis_date).label('max_date')
    ).group_by(StockAnalysisResult.stock_code).subquery()
    
    results = db.query(StockAnalysisResult).join(
        subq, (StockAnalysisResult.stock_code == subq.c.stock_code) & 
              (StockAnalysisResult.analysis_date == subq.c.max_date)
    ).order_by(desc(StockAnalysisResult.total_score)).all()
    
    if not results:
        raise HTTPException(status_code=404, detail="尚无分析数据")

    file_path = "outputs/global_report.csv"
    os.makedirs("outputs", exist_ok=True)
    df = _build_analysis_df(results)
    df.to_csv(file_path, index=False, encoding="utf_8_sig")
    return FileResponse(file_path, filename=f"全市场分析报告_{datetime.now().date()}.csv")

@router.get("/export/my-report")
def export_user_report(db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    """【个人报告】导出当前用户关注列表中股票的分析结果"""
    # 获取用户关注的代码
    watched = db.query(UserStockWatch.stock_code).filter(UserStockWatch.user_id == current_user.user_id).all()
    codes = [w.stock_code for w in watched]
    
    if not codes:
        raise HTTPException(status_code=400, detail="您的关注列表为空")

    subq = db.query(
        StockAnalysisResult.stock_code,
        func.max(StockAnalysisResult.analysis_date).label('max_date')
    ).filter(StockAnalysisResult.stock_code.in_(codes)).group_by(StockAnalysisResult.stock_code).subquery()
    
    results = db.query(StockAnalysisResult).join(
        subq, (StockAnalysisResult.stock_code == subq.c.stock_code) & 
              (StockAnalysisResult.analysis_date == subq.c.max_date)
    ).order_by(desc(StockAnalysisResult.total_score)).all()

    file_path = f"outputs/user_{current_user.user_id}_report.csv"
    df = _build_analysis_df(results)
    df.to_csv(file_path, index=False, encoding="utf_8_sig")
    return FileResponse(file_path, filename=f"我的价值分析报告_{datetime.now().date()}.csv")

# ============================================================
# 5. 系统状态
# ============================================================

@router.get("/system/status")
def get_system_status(db: Session = Depends(get_db)):
    """查看数据库各表数据量及最新同步时间"""
    return {
        "tables": {
            "users": db.query(User).count(),
            "market_data": db.query(DailyMarketData).count(),
            "dividend_data": db.query(DividendData).count(),
            "analysis_results": db.query(StockAnalysisResult).count(),
            "watch_records": db.query(UserStockWatch).count()
        },
        "latest_dates": {
            "market_sync": db.query(func.max(DailyMarketData.date)).scalar(),
            "analysis_sync": db.query(func.max(StockAnalysisResult.analysis_date)).scalar()
        }
    }

@router.get("/diagnose/{stock_code}")
async def diagnose_stock_issues(stock_code: str, db: Session = Depends(get_db)):
    """诊断特定股票的数据问题"""
    # 检查分析记录
    analysis_records = db.query(StockAnalysisResult).filter(
        StockAnalysisResult.stock_code == stock_code
    ).order_by(desc(StockAnalysisResult.analysis_date)).limit(20).all()
    
    # 检查市场数据
    market_data = db.query(DailyMarketData).filter(
        DailyMarketData.code == stock_code
    ).order_by(desc(DailyMarketData.date)).first()
    
    diagnosis = {
        "stock_code": stock_code,
        "total_analyses": len(analysis_records),
        "recent_analyses": [
            {
                "date": str(record.analysis_date),
                "pe_ratio": record.pe_ratio,
                "total_score": record.total_score,
                "suggestion": record.suggestion
            }
            for record in analysis_records[:10]
        ],
        "market_data": {
            "latest_pe": market_data.pe_dynamic if market_data else None,
            "latest_price": market_data.latest_price if market_data else None
        } if market_data else None,
        "issues": []
    }
    
    # 检测问题
    abnormal_pe_count = sum(1 for r in analysis_records if r.pe_ratio > 1000000)
    if abnormal_pe_count > 0:
        diagnosis["issues"].append(f"发现{abnormal_pe_count}条异常PE数据")
    
    duplicate_count = len(analysis_records) - len(set(r.analysis_date for r in analysis_records))
    if duplicate_count > 0:
        diagnosis["issues"].append(f"发现{duplicate_count}条重复分析记录")
    
    return diagnosis
