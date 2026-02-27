from sqlalchemy.orm import Session
from models.stock import DailyMarketData, UserStockWatch, StockAnalysisResult
from datetime import date, datetime
from typing import List, Optional, Dict, Any

def save_market_data_batch(db: Session, market_data_list: List[Dict[str, Any]]) -> List[DailyMarketData]:
    """批量保存市场数据"""
    db_objects = []
    for data in market_data_list:
        # 确保必要字段存在
        required_fields = ['date', 'code', 'name']
        if not all(field in data for field in required_fields):
            continue
            
        db_data = DailyMarketData(**data)
        db_objects.append(db_data)
    
    if db_objects:
        db.add_all(db_objects)
        db.commit()
        # 刷新对象获取ID
        for obj in db_objects:
            db.refresh(obj)
    
    return db_objects

def get_market_data_by_date(db: Session, query_date: date) -> List[DailyMarketData]:
    """根据日期获取市场数据"""
    return db.query(DailyMarketData).filter(
        DailyMarketData.date == query_date
    ).all()

def add_user_watch_stock(db: Session, user_id: int, stock_code: str) -> UserStockWatch:
    """添加用户关注股票"""
    # 检查是否已关注
    existing = db.query(UserStockWatch).filter(
        UserStockWatch.user_id == user_id,
        UserStockWatch.stock_code == stock_code
    ).first()
    
    if existing:
        return existing
    
    watch_record = UserStockWatch(
        user_id=user_id,
        stock_code=stock_code
    )
    db.add(watch_record)
    db.commit()
    db.refresh(watch_record)
    return watch_record

def get_user_watch_stocks(db: Session, user_id: int) -> List[UserStockWatch]:
    """获取用户关注的股票"""
    return db.query(UserStockWatch).filter(
        UserStockWatch.user_id == user_id
    ).all()

def remove_user_watch_stock(db: Session, user_id: int, stock_code: str) -> bool:
    """移除用户关注的股票"""
    watch_record = db.query(UserStockWatch).filter(
        UserStockWatch.user_id == user_id,
        UserStockWatch.stock_code == stock_code
    ).first()
    
    if watch_record:
        db.delete(watch_record)
        db.commit()
        return True
    return False

def save_analysis_result(db: Session, analysis_data: Dict[str, Any]) -> StockAnalysisResult:
    """保存分析结果"""
    required_fields = ['stock_code', 'stock_name', 'analysis_date']
    if not all(field in analysis_data for field in required_fields):
        raise ValueError("缺少必要字段")
    
    db_result = StockAnalysisResult(**analysis_data)
    db.add(db_result)
    db.commit()
    db.refresh(db_result)
    return db_result

def get_analysis_results(db: Session, stock_code: str, limit: int = 30) -> List[StockAnalysisResult]:
    """获取股票分析结果"""
    return db.query(StockAnalysisResult).filter(
        StockAnalysisResult.stock_code == stock_code
    ).order_by(StockAnalysisResult.analysis_date.desc()).limit(limit).all()

def get_latest_analysis_result(db: Session, stock_code: str) -> Optional[StockAnalysisResult]:
    """获取最新的分析结果"""
    return db.query(StockAnalysisResult).filter(
        StockAnalysisResult.stock_code == stock_code
    ).order_by(StockAnalysisResult.analysis_date.desc()).first()

def update_stock_price(db: Session, stock_code: str, price: float, update_time: datetime = None) -> int:
    """更新股票价格（用于持仓更新）"""
    if update_time is None:
        update_time = datetime.now()
    
    result = db.query(DailyMarketData).filter(
        DailyMarketData.code == stock_code,
        DailyMarketData.date == date.today()
    ).update({
        DailyMarketData.latest_price: price,
        DailyMarketData.updated_at: update_time
    })
    
    db.commit()
    return result

def get_analysis_by_user(db: Session, user_id: int, limit: int = 50) -> List[StockAnalysisResult]:
    """根据用户关注的股票获取分析结果"""
    # 先获取用户关注的股票
    from crud.user import get_user_watch_stocks
    watch_stocks = get_user_watch_stocks(db, user_id)
    stock_codes = [watch.stock_code for watch in watch_stocks]
    
    if not stock_codes:
        return []
    
    # 查询这些股票的分析结果
    return db.query(StockAnalysisResult).filter(
        StockAnalysisResult.stock_code.in_(stock_codes)
    ).order_by(StockAnalysisResult.analysis_date.desc()).limit(limit).all()

def get_top_stocks_by_score(db: Session, limit: int = 20) -> List[StockAnalysisResult]:
    """获取高分股票"""
    return db.query(StockAnalysisResult).filter(
        StockAnalysisResult.total_score >= 80
    ).order_by(StockAnalysisResult.total_score.desc()).limit(limit).all()

def get_stock_analysis_history(db: Session, stock_code: str, days: int = 30) -> List[StockAnalysisResult]:
    """获取股票历史分析数据"""
    from datetime import datetime, timedelta
    start_date = datetime.now() - timedelta(days=days)
    
    return db.query(StockAnalysisResult).filter(
        StockAnalysisResult.stock_code == stock_code,
        StockAnalysisResult.analysis_date >= start_date.date()
    ).order_by(StockAnalysisResult.analysis_date.desc()).all()


def add_to_watchlist(db: Session, user_id: int, stock_code: str) -> bool:
    """添加股票到关注列表"""
    from models.stock import UserStockWatch
    
    # 检查是否已存在
    existing = db.query(UserStockWatch).filter(
        UserStockWatch.user_id == user_id,
        UserStockWatch.stock_code == stock_code
    ).first()
    
    if existing:
        return True  # 已存在，返回成功
    
    # 创建新的关注记录
    watch_record = UserStockWatch(
        user_id=user_id,
        stock_code=stock_code
    )
    db.add(watch_record)
    db.commit()
    return True

def add_multiple_to_watchlist(db: Session, user_id: int, stock_codes: list) -> dict:
    """批量添加股票到关注列表"""
    success_count = 0
    failed_codes = []
    
    for code in stock_codes:
        try:
            if add_to_watchlist(db, user_id, code):
                success_count += 1
            else:
                failed_codes.append(code)
        except Exception as e:
            failed_codes.append(f"{code}({str(e)})")
    
    return {
        "success_count": success_count,
        "failed_count": len(failed_codes),
        "failed_codes": failed_codes
    }

def get_user_watchlist(db: Session, user_id: int) -> list:
    """获取用户关注列表"""
    from models.stock import UserStockWatch
    return db.query(UserStockWatch).filter(
        UserStockWatch.user_id == user_id
    ).all()

def remove_from_watchlist(db: Session, user_id: int, stock_code: str) -> bool:
    """从关注列表移除股票"""
    from models.stock import UserStockWatch
    
    record = db.query(UserStockWatch).filter(
        UserStockWatch.user_id == user_id,
        UserStockWatch.stock_code == stock_code
    ).first()
    
    if record:
        db.delete(record)
        db.commit()
        return True
    return False