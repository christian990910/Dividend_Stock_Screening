from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from models.stock import DailyMarketData, UserStockWatch, StockAnalysisResult, HistoricalData, DividendData,IndexConstituent
import datetime

# --- 关注列表 ---
def get_user_watchlist(db: Session, user_id: int):
    return db.query(UserStockWatch).filter(UserStockWatch.user_id == user_id).all()

def add_to_watchlist(db: Session, user_id: int, stock_code: str):
    existing = db.query(UserStockWatch).filter(
        UserStockWatch.user_id == user_id, 
        UserStockWatch.stock_code == stock_code
    ).first()
    if not existing:
        watch = UserStockWatch(user_id=user_id, stock_code=stock_code)
        db.add(watch)
        db.commit()
        return True
    return False

def remove_from_watchlist(db: Session, user_id: int, stock_code: str):
    db.query(UserStockWatch).filter(
        UserStockWatch.user_id == user_id, 
        UserStockWatch.stock_code == stock_code
    ).delete()
    db.commit()

# --- 市场行情 ---
def save_market_data_batch(db: Session, data_list: list):
    """批量保存每日行情数据"""
    # 采用先删除当日数据再插入的逻辑，保证数据唯一性
    today = datetime.date.today()
    db.query(DailyMarketData).filter(DailyMarketData.date == today).delete()
    db.bulk_save_objects(data_list)
    db.commit()

def get_latest_stock_price(db: Session, stock_code: str):
    return db.query(DailyMarketData).filter(
        DailyMarketData.code == stock_code
    ).order_by(desc(DailyMarketData.date)).first()

# --- 分析结果 ---
def save_analysis_result(db: Session, result_obj: StockAnalysisResult):
    # 使用 merge 处理：如果当日已存在该股票分析，则更新
    db.merge(result_obj)
    db.commit()

def get_analysis_by_user(db: Session, user_id: int):
    """获取用户关注股票的最新分析结果"""
    # 1. 获取关注列表
    watched_codes = [w.stock_code for w in get_user_watchlist(db, user_id)]
    if not watched_codes:
        return []
    
    # 2. 子查询：获取每个股票最新的分析日期
    subquery = db.query(
        StockAnalysisResult.stock_code,
        func.max(StockAnalysisResult.analysis_date).label('max_date')
    ).filter(StockAnalysisResult.stock_code.in_(watched_codes)).group_by(StockAnalysisResult.stock_code).subquery()
    
    # 3. 联表查询详情
    return db.query(StockAnalysisResult).join(
        subquery,
        (StockAnalysisResult.stock_code == subquery.c.stock_code) &
        (StockAnalysisResult.analysis_date == subquery.c.max_date)
    ).order_by(desc(StockAnalysisResult.total_score)).all()

def get_stocks_by_index(db: Session, index_code: str):
    return db.query(IndexConstituent.constituent_code).filter(
        IndexConstituent.index_code == index_code,
        IndexConstituent.is_active == 1
    ).all()