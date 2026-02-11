import os
import time
import datetime
import asyncio
import re
import json
import pandas as pd
import numpy as np
import akshare as ak
from typing import Optional, List
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, desc, func, Boolean, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from starlette.background import BackgroundTasks
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import FileResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# --- 数据库配置 ---
SQLALCHEMY_DATABASE_URL = "sqlite:///./stock_advanced_system.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- 数据模型 ---

class User(Base):
    """用户表"""
    __tablename__ = "users"
    user_id = Column(String, primary_key=True)  # 用户ID
    username = Column(String, unique=True)
    created_at = Column(DateTime, default=datetime.datetime.now)

class UserStockWatch(Base):
    """用户关注股票列表"""
    __tablename__ = "user_stock_watch"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, index=True)  # 用户ID
    stock_code = Column(String, index=True)  # 股票代码
    added_at = Column(DateTime, default=datetime.datetime.now)

class DailyMarketData(Base):
    """接口1: 每日全市场实时数据 - stock_zh_a_spot_em"""
    __tablename__ = "daily_market_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, index=True)  # 日期
    code = Column(String, index=True)  # 代码
    name = Column(String)  # 名称
    latest_price = Column(Float)  # 最新价
    change_pct = Column(Float)  # 涨跌幅
    change_amount = Column(Float)  # 涨跌额
    volume = Column(Float)  # 成交量(手)
    amount = Column(Float)  # 成交额(元)
    amplitude = Column(Float)  # 振幅
    high = Column(Float)  # 最高
    low = Column(Float)  # 最低
    open = Column(Float)  # 今开
    close_prev = Column(Float)  # 昨收
    volume_ratio = Column(Float)  # 量比
    turnover_rate = Column(Float)  # 换手率
    pe_dynamic = Column(Float)  # 市盈率-动态
    pb = Column(Float)  # 市净率
    total_market_cap = Column(Float)  # 总市值
    circulating_market_cap = Column(Float)  # 流通市值
    rise_speed = Column(Float)  # 涨速
    change_5min = Column(Float)  # 5分钟涨跌
    change_60day = Column(Float)  # 60日涨跌幅
    change_ytd = Column(Float)  # 年初至今涨跌幅
    updated_at = Column(DateTime, default=datetime.datetime.now)

class HistoricalData(Base):
    """接口2: 历史行情数据 - stock_zh_a_hist (前复权)"""
    __tablename__ = "historical_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String, index=True)  # 股票代码
    date = Column(Date, index=True)  # 日期
    open = Column(Float)  # 开盘
    close = Column(Float)  # 收盘
    high = Column(Float)  # 最高
    low = Column(Float)  # 最低
    volume = Column(Integer)  # 成交量(手)
    amount = Column(Float)  # 成交额(元)
    amplitude = Column(Float)  # 振幅
    change_pct = Column(Float)  # 涨跌幅
    change_amount = Column(Float)  # 涨跌额
    turnover_rate = Column(Float)  # 换手率
    created_at = Column(DateTime, default=datetime.datetime.now)

class DividendData(Base):
    """接口3: 分红派息数据 - news_trade_notify_dividend_baidu"""
    __tablename__ = "dividend_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String, index=True)  # 股票代码
    stock_name = Column(String)  # 股票简称
    ex_dividend_date = Column(Date, index=True)  # 除权日
    dividend = Column(String)  # 分红
    bonus_share = Column(String)  # 送股
    capitalization = Column(String)  # 转增
    physical = Column(String)  # 实物
    exchange = Column(String)  # 交易所
    report_period = Column(String)  # 报告期
    created_at = Column(DateTime, default=datetime.datetime.now)

class StockAnalysisResult(Base):
    """股票分析结果表"""
    __tablename__ = "stock_analysis_results"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String, index=True)
    stock_name = Column(String)
    analysis_date = Column(Date, index=True)
    
    # 价格信息
    latest_price = Column(Float)
    pe_ratio = Column(Float)  # 市盈率
    pb_ratio = Column(Float)  # 市净率
    
    # 波动率(基于历史数据计算)
    volatility_30d = Column(Float)  # 30日波动率
    volatility_60d = Column(Float)  # 60日波动率
    
    # 股息率(基于分红数据计算)
    dividend_yield = Column(Float)  # 年化股息率
    
    # ROE和成长性(从市场数据补充)
    roe = Column(Float)
    profit_growth = Column(Float)
    
    # 评分和建议
    volatility_score = Column(Integer)  # 波动率评分 (0-40)
    dividend_score = Column(Integer)  # 股息率评分 (0-30)
    growth_score = Column(Integer)  # 成长性评分 (0-30)
    total_score = Column(Integer)  # 总分 (0-100)
    suggestion = Column(String)  # 建议
    
    # 数据来源标记
    data_source = Column(String)  # 主要数据来源: market/history/mixed
    
    created_at = Column(DateTime, default=datetime.datetime.now)

# 创建所有表
Base.metadata.create_all(bind=engine)

# --- 数据服务层 ---

class StockDataService:
    """股票数据服务"""
    
    def __init__(self):
        self.last_market_fetch = None
    
    def get_db(self) -> Session:
        """获取数据库会话"""
        db = SessionLocal()
        try:
            return db
        finally:
            pass
    
    async def fetch_daily_market_data(self, force: bool = False) -> dict:
        """
        获取全市场每日数据 (接口1: stock_zh_a_spot_em)
        默认每日15:30自动执行，可手动强制执行
        """
        db = self.get_db()
        try:
            today = datetime.date.today()
            
            # 检查今天是否已经获取过数据
            existing = db.query(DailyMarketData).filter(
                DailyMarketData.date == today
            ).first()
            
            if existing and not force:
                db.close()
                return {"status": "skip", "message": "今日数据已存在，使用 force=True 强制更新"}
            
            print(f"开始获取全市场数据... 时间: {datetime.datetime.now()}")
            
            # 调用akshare接口
            df = ak.stock_zh_a_spot_em()
            
            if df.empty:
                db.close()
                return {"status": "error", "message": "未获取到数据"}
            
            # 如果是强制更新，删除今天的旧数据
            if force and existing:
                db.query(DailyMarketData).filter(
                    DailyMarketData.date == today
                ).delete()
                db.commit()
            
            # 批量插入数据
            count = 0
            for _, row in df.iterrows():
                market_data = DailyMarketData(
                    date=today,
                    code=str(row['代码']),
                    name=str(row['名称']),
                    latest_price=float(row['最新价']) if pd.notna(row['最新价']) else None,
                    change_pct=float(row['涨跌幅']) if pd.notna(row['涨跌幅']) else None,
                    change_amount=float(row['涨跌额']) if pd.notna(row['涨跌额']) else None,
                    volume=float(row['成交量']) if pd.notna(row['成交量']) else None,
                    amount=float(row['成交额']) if pd.notna(row['成交额']) else None,
                    amplitude=float(row['振幅']) if pd.notna(row['振幅']) else None,
                    high=float(row['最高']) if pd.notna(row['最高']) else None,
                    low=float(row['最低']) if pd.notna(row['最低']) else None,
                    open=float(row['今开']) if pd.notna(row['今开']) else None,
                    close_prev=float(row['昨收']) if pd.notna(row['昨收']) else None,
                    volume_ratio=float(row['量比']) if pd.notna(row['量比']) else None,
                    turnover_rate=float(row['换手率']) if pd.notna(row['换手率']) else None,
                    pe_dynamic=float(row['市盈率-动态']) if pd.notna(row['市盈率-动态']) else None,
                    pb=float(row['市净率']) if pd.notna(row['市净率']) else None,
                    total_market_cap=float(row['总市值']) if pd.notna(row['总市值']) else None,
                    circulating_market_cap=float(row['流通市值']) if pd.notna(row['流通市值']) else None,
                    rise_speed=float(row['涨速']) if pd.notna(row['涨速']) else None,
                    change_5min=float(row['5分钟涨跌']) if pd.notna(row['5分钟涨跌']) else None,
                    change_60day=float(row['60日涨跌幅']) if pd.notna(row['60日涨跌幅']) else None,
                    change_ytd=float(row['年初至今涨跌幅']) if pd.notna(row['年初至今涨跌幅']) else None,
                )
                db.add(market_data)
                count += 1
                
                # 每1000条提交一次
                if count % 1000 == 0:
                    db.commit()
                    print(f"已保存 {count} 条数据...")
            
            db.commit()
            self.last_market_fetch = datetime.datetime.now()
            
            db.close()
            return {
                "status": "success",
                "message": f"成功获取并保存 {count} 只股票的市场数据",
                "count": count,
                "date": str(today)
            }
            
        except Exception as e:
            db.close()
            return {"status": "error", "message": f"获取市场数据失败: {str(e)}"}
    
    async def fetch_historical_data(self, stock_code: str, start_date: str = None, 
                                    end_date: str = None, period: str = "daily") -> dict:
        """
        获取指定股票的历史数据 (接口2: stock_zh_a_hist)
        使用前复权数据
        """
        db = self.get_db()
        try:
            # 默认获取最近180天的数据
            if not end_date:
                end_date = datetime.date.today().strftime("%Y%m%d")
            if not start_date:
                start_date = (datetime.date.today() - datetime.timedelta(days=180)).strftime("%Y%m%d")
            
            print(f"获取 {stock_code} 历史数据: {start_date} 至 {end_date}")
            
            # 调用akshare接口 - 前复权
            df = ak.stock_zh_a_hist(
                symbol=stock_code,
                period=period,
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )
            
            if df.empty:
                db.close()
                return {"status": "error", "message": f"股票 {stock_code} 无历史数据"}
            
            # 删除该股票在此时间段的旧数据
            db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code,
                HistoricalData.date >= datetime.datetime.strptime(start_date, "%Y%m%d").date(),
                HistoricalData.date <= datetime.datetime.strptime(end_date, "%Y%m%d").date()
            ).delete()
            
            # 批量插入
            count = 0
            for _, row in df.iterrows():
                hist_data = HistoricalData(
                    stock_code=stock_code,
                    date=datetime.datetime.strptime(str(row['日期']), "%Y-%m-%d").date(),
                    open=float(row['开盘']) if pd.notna(row['开盘']) else None,
                    close=float(row['收盘']) if pd.notna(row['收盘']) else None,
                    high=float(row['最高']) if pd.notna(row['最高']) else None,
                    low=float(row['最低']) if pd.notna(row['最低']) else None,
                    volume=int(row['成交量']) if pd.notna(row['成交量']) else None,
                    amount=float(row['成交额']) if pd.notna(row['成交额']) else None,
                    amplitude=float(row['振幅']) if pd.notna(row['振幅']) else None,
                    change_pct=float(row['涨跌幅']) if pd.notna(row['涨跌幅']) else None,
                    change_amount=float(row['涨跌额']) if pd.notna(row['涨跌额']) else None,
                    turnover_rate=float(row['换手率']) if pd.notna(row['换手率']) else None,
                )
                db.add(hist_data)
                count += 1
            
            db.commit()
            db.close()
            
            return {
                "status": "success",
                "message": f"成功获取 {stock_code} 历史数据",
                "count": count,
                "start_date": start_date,
                "end_date": end_date
            }
            
        except Exception as e:
            db.close()
            return {"status": "error", "message": f"获取历史数据失败: {str(e)}"}
    
    async def fetch_dividend_data(self, date_str: str = None) -> dict:
        """
        获取指定日期的分红派息数据 (接口3: news_trade_notify_dividend_baidu)
        """
        db = self.get_db()
        try:
            if not date_str:
                date_str = datetime.date.today().strftime("%Y%m%d")
            
            print(f"获取 {date_str} 分红数据...")
            
            # 调用akshare接口
            df = ak.news_trade_notify_dividend_baidu(date=date_str)
            
            if df.empty:
                db.close()
                return {"status": "success", "message": f"{date_str} 无分红数据", "count": 0}
            
            # 删除该日期的旧数据
            target_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
            db.query(DividendData).filter(
                DividendData.ex_dividend_date == target_date
            ).delete()
            
            # 批量插入
            count = 0
            for _, row in df.iterrows():
                dividend_data = DividendData(
                    stock_code=str(row['股票代码']),
                    stock_name=str(row['股票简称']),
                    ex_dividend_date=datetime.datetime.strptime(str(row['除权日']), "%Y-%m-%d").date(),
                    dividend=str(row['分红']),
                    bonus_share=str(row['送股']),
                    capitalization=str(row['转增']),
                    physical=str(row['实物']),
                    exchange=str(row['交易所']),
                    report_period=str(row['报告期']),
                )
                db.add(dividend_data)
                count += 1
            
            db.commit()
            db.close()
            
            return {
                "status": "success",
                "message": f"成功获取 {date_str} 分红数据",
                "count": count,
                "date": date_str
            }
            
        except Exception as e:
            db.close()
            return {"status": "error", "message": f"获取分红数据失败: {str(e)}"}
    
    async def analyze_stock(self, stock_code: str, db: Session = None) -> dict:
        """
        分析单只股票
        优先使用市场数据，不足时使用历史数据和分红数据补充
        """
        should_close = False
        if db is None:
            db = self.get_db()
            should_close = True
        
        try:
            today = datetime.date.today()
            
            # 1. 获取最新市场数据 (接口1)
            market_data = db.query(DailyMarketData).filter(
                DailyMarketData.code == stock_code,
                DailyMarketData.date == today
            ).first()
            
            if not market_data:
                # 如果今天没有市场数据，尝试获取最近一次的
                market_data = db.query(DailyMarketData).filter(
                    DailyMarketData.code == stock_code
                ).order_by(desc(DailyMarketData.date)).first()
            
            if not market_data:
                if should_close:
                    db.close()
                return {"status": "error", "message": f"股票 {stock_code} 无市场数据"}
            
            # 基础数据
            latest_price = market_data.latest_price
            pe_ratio = market_data.pe_dynamic
            pb_ratio = market_data.pb
            stock_name = market_data.name
            data_source = "market"
            
            # 2. 计算波动率 (使用历史数据)
            volatility_30d = 0
            volatility_60d = 0
            
            # 获取最近60天的历史数据
            hist_data = db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code
            ).order_by(desc(HistoricalData.date)).limit(60).all()
            
            if hist_data and len(hist_data) >= 30:
                closes = [h.close for h in reversed(hist_data) if h.close]
                
                if len(closes) >= 30:
                    # 计算30日波动率
                    series_30 = pd.Series(closes[-30:])
                    log_ret_30 = np.log(series_30 / series_30.shift(1)).dropna()
                    volatility_30d = log_ret_30.std() * np.sqrt(252) * 100
                    
                    # 计算60日波动率
                    if len(closes) >= 60:
                        series_60 = pd.Series(closes[-60:])
                        log_ret_60 = np.log(series_60 / series_60.shift(1)).dropna()
                        volatility_60d = log_ret_60.std() * np.sqrt(252) * 100
                    
                    data_source = "mixed"
            
            # 如果历史数据不足，尝试获取
            if volatility_30d == 0:
                print(f"  {stock_code} 历史数据不足，尝试获取...")
                await self.fetch_historical_data(stock_code)
                # 重新查询
                hist_data = db.query(HistoricalData).filter(
                    HistoricalData.stock_code == stock_code
                ).order_by(desc(HistoricalData.date)).limit(60).all()
                
                if hist_data and len(hist_data) >= 30:
                    closes = [h.close for h in reversed(hist_data) if h.close]
                    if len(closes) >= 30:
                        series_30 = pd.Series(closes[-30:])
                        log_ret_30 = np.log(series_30 / series_30.shift(1)).dropna()
                        volatility_30d = log_ret_30.std() * np.sqrt(252) * 100
                        data_source = "mixed"
            
            # 3. 计算股息率 (使用分红数据)
            dividend_yield = 0
            
            # 获取最近一年的分红记录
            one_year_ago = today - datetime.timedelta(days=365)
            dividends = db.query(DividendData).filter(
                DividendData.stock_code == stock_code,
                DividendData.ex_dividend_date >= one_year_ago
            ).all()
            
            if dividends and latest_price:
                total_dividend = 0
                for div in dividends:
                    # 解析分红金额 (格式如 "1.10元" 或 "0.08港元")
                    div_str = str(div.dividend)
                    match = re.search(r'(\d+\.?\d*)', div_str)
                    if match:
                        total_dividend += float(match.group(1))
                
                # 年化股息率 = (年度分红 / 股价) * 100
                if total_dividend > 0:
                    dividend_yield = (total_dividend / latest_price) * 100
                    data_source = "mixed"
            
            # 4. ROE和成长性 (暂时从市场数据获取，后续可扩展)
            roe = 0
            profit_growth = 0
            
            # 5. 评分系统
            # 波动率评分 (0-40分): 波动率越低越好
            volatility_score = 0
            if volatility_30d > 0:
                if volatility_30d < 20:
                    volatility_score = 40
                elif volatility_30d < 30:
                    volatility_score = 30
                elif volatility_30d < 40:
                    volatility_score = 20
                elif volatility_30d < 50:
                    volatility_score = 10
            
            # 股息率评分 (0-30分): 股息率越高越好
            dividend_score = 0
            if dividend_yield >= 5:
                dividend_score = 30
            elif dividend_yield >= 4:
                dividend_score = 25
            elif dividend_yield >= 3:
                dividend_score = 20
            elif dividend_yield >= 2:
                dividend_score = 15
            elif dividend_yield >= 1:
                dividend_score = 10
            
            # 成长性评分 (0-30分): ROE越高越好
            growth_score = 0
            if roe > 15:
                growth_score = 30
            elif roe > 12:
                growth_score = 25
            elif roe > 10:
                growth_score = 20
            elif roe > 8:
                growth_score = 15
            elif roe > 5:
                growth_score = 10
            
            # 总分
            total_score = volatility_score + dividend_score + growth_score
            
            # 建议
            if total_score >= 70:
                suggestion = "强烈推荐"
            elif total_score >= 60:
                suggestion = "推荐"
            elif total_score >= 50:
                suggestion = "可以关注"
            elif total_score >= 40:
                suggestion = "观望"
            else:
                suggestion = "不推荐"
            
            # 6. 保存分析结果
            analysis_result = StockAnalysisResult(
                stock_code=stock_code,
                stock_name=stock_name,
                analysis_date=today,
                latest_price=latest_price,
                pe_ratio=pe_ratio,
                pb_ratio=pb_ratio,
                volatility_30d=round(volatility_30d, 2),
                volatility_60d=round(volatility_60d, 2),
                dividend_yield=round(dividend_yield, 2),
                roe=roe,
                profit_growth=profit_growth,
                volatility_score=volatility_score,
                dividend_score=dividend_score,
                growth_score=growth_score,
                total_score=total_score,
                suggestion=suggestion,
                data_source=data_source
            )
            
            db.add(analysis_result)
            db.commit()
            
            if should_close:
                db.close()
            
            return {
                "status": "success",
                "stock_code": stock_code,
                "stock_name": stock_name,
                "total_score": total_score,
                "suggestion": suggestion
            }
            
        except Exception as e:
            if should_close:
                db.close()
            return {"status": "error", "message": f"分析 {stock_code} 失败: {str(e)}"}
    
    async def analyze_all_watched_stocks(self) -> dict:
        """
        分析所有用户关注的股票
        """
        db = self.get_db()
        try:
            # 获取所有用户关注的股票(去重)
            watched_stocks = db.query(UserStockWatch.stock_code).distinct().all()
            watched_codes = [s[0] for s in watched_stocks]
            
            if not watched_codes:
                db.close()
                return {"status": "success", "message": "没有用户关注的股票", "count": 0}
            
            print(f"开始分析 {len(watched_codes)} 只被关注的股票...")
            
            success_count = 0
            error_count = 0
            
            for code in watched_codes:
                result = await self.analyze_stock(code, db)
                if result["status"] == "success":
                    success_count += 1
                    print(f"  ✓ {code} - {result.get('suggestion', '')}")
                else:
                    error_count += 1
                    print(f"  ✗ {code} - {result.get('message', '')}")
                
                # 避免请求过快
                await asyncio.sleep(0.5)
            
            db.close()
            
            return {
                "status": "success",
                "message": f"分析完成: 成功 {success_count}, 失败 {error_count}",
                "success_count": success_count,
                "error_count": error_count,
                "total": len(watched_codes)
            }
            
        except Exception as e:
            db.close()
            return {"status": "error", "message": f"批量分析失败: {str(e)}"}


# --- FastAPI应用 ---
app = FastAPI(title="价值分析系统", version="2.0")
stock_service = StockDataService()

# 定时任务调度器
scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup_event():
    """启动时初始化定时任务"""
    # 每日15:30自动获取全市场数据
    scheduler.add_job(
        stock_service.fetch_daily_market_data,
        CronTrigger(hour=15, minute=30),
        id="daily_market_fetch",
        replace_existing=True
    )
    
    # 每日16:00自动分析所有关注股票
    scheduler.add_job(
        stock_service.analyze_all_watched_stocks,
        CronTrigger(hour=16, minute=0),
        id="daily_analysis",
        replace_existing=True
    )
    
    scheduler.start()
    print("定时任务已启动:")
    print("  - 每日15:30获取全市场数据")
    print("  - 每日16:00分析所有关注股票")

@app.on_event("shutdown")
async def shutdown_event():
    """关闭时停止定时任务"""
    scheduler.shutdown()

# --- 用户管理接口 ---

@app.post("/users/create")
def create_user(user_id: str, username: str):
    """创建用户"""
    db = SessionLocal()
    try:
        existing = db.query(User).filter(User.user_id == user_id).first()
        if existing:
            db.close()
            raise HTTPException(status_code=400, detail="用户ID已存在")
        
        user = User(user_id=user_id, username=username)
        db.add(user)
        db.commit()
        db.close()
        
        return {"status": "success", "message": f"用户 {username} 创建成功"}
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

# --- 股票关注管理 ---

@app.post("/watch/add")
def add_watch_stock(user_id: str, stock_codes: str):
    """
    添加股票到用户关注列表
    stock_codes: 逗号分隔的股票代码,如 "600036,000001,600519"
    """
    db = SessionLocal()
    try:
        # 检查用户是否存在
        user = db.query(User).filter(User.user_id == user_id).first()
        if not user:
            db.close()
            raise HTTPException(status_code=404, detail="用户不存在")
        
        # 提取股票代码
        codes = re.findall(r'\d{6}', stock_codes)
        added = 0
        
        for code in set(codes):
            # 检查是否已关注
            existing = db.query(UserStockWatch).filter(
                UserStockWatch.user_id == user_id,
                UserStockWatch.stock_code == code
            ).first()
            
            if not existing:
                watch = UserStockWatch(user_id=user_id, stock_code=code)
                db.add(watch)
                added += 1
        
        db.commit()
        db.close()
        
        return {"status": "success", "message": f"成功添加 {added} 只股票到关注列表"}
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/watch/remove")
def remove_watch_stock(user_id: str, stock_code: str):
    """移除股票从用户关注列表"""
    db = SessionLocal()
    try:
        result = db.query(UserStockWatch).filter(
            UserStockWatch.user_id == user_id,
            UserStockWatch.stock_code == stock_code
        ).delete()
        
        db.commit()
        db.close()
        
        if result > 0:
            return {"status": "success", "message": f"已移除股票 {stock_code}"}
        else:
            raise HTTPException(status_code=404, detail="未找到该关注记录")
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/watch/list")
def list_watch_stocks(user_id: str):
    """查看用户关注的股票列表"""
    db = SessionLocal()
    try:
        watches = db.query(UserStockWatch).filter(
            UserStockWatch.user_id == user_id
        ).all()
        
        db.close()
        
        return {
            "user_id": user_id,
            "watched_stocks": [{"code": w.stock_code, "added_at": str(w.added_at)} for w in watches],
            "count": len(watches)
        }
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

# --- 数据获取接口 ---

@app.post("/data/market/fetch")
async def fetch_market_data(force: bool = False):
    """
    手动获取全市场数据
    force: 是否强制更新今日数据
    """
    result = await stock_service.fetch_daily_market_data(force=force)
    return result

@app.post("/data/history/fetch")
async def fetch_history_data(stock_code: str, start_date: str = None, end_date: str = None):
    """
    手动获取指定股票的历史数据
    stock_code: 股票代码
    start_date: 开始日期 (格式: 20240101)
    end_date: 结束日期 (格式: 20240131)
    """
    result = await stock_service.fetch_historical_data(stock_code, start_date, end_date)
    return result

@app.post("/data/dividend/fetch")
async def fetch_dividend_data(date_str: str = None):
    """
    手动获取分红数据
    date_str: 日期 (格式: 20240101), 默认为今天
    """
    result = await stock_service.fetch_dividend_data(date_str)
    return result

# --- 分析接口 ---

@app.post("/analyze/manual")
async def manual_analyze(background_tasks: BackgroundTasks):
    """手动触发分析所有关注股票"""
    background_tasks.add_task(stock_service.analyze_all_watched_stocks)
    return {"status": "success", "message": "分析任务已在后台启动"}

@app.post("/analyze/stock")
async def analyze_single_stock(stock_code: str):
    """分析单只股票"""
    result = await stock_service.analyze_stock(stock_code)
    return result

# --- 导出接口 ---

@app.get("/export/global")
def export_global_csv():
    """导出全局分析结果到CSV"""
    db = SessionLocal()
    try:
        # 获取每只股票的最新分析结果
        subquery = db.query(
            StockAnalysisResult.stock_code,
            func.max(StockAnalysisResult.analysis_date).label('max_date')
        ).group_by(StockAnalysisResult.stock_code).subquery()
        
        results = db.query(StockAnalysisResult).join(
            subquery,
            (StockAnalysisResult.stock_code == subquery.c.stock_code) &
            (StockAnalysisResult.analysis_date == subquery.c.max_date)
        ).order_by(desc(StockAnalysisResult.total_score)).all()
        
        data = []
        for r in results:
            data.append({
                "股票代码": r.stock_code,
                "股票名称": r.stock_name,
                "分析日期": str(r.analysis_date),
                "最新价": r.latest_price,
                "市盈率": r.pe_ratio,
                "市净率": r.pb_ratio,
                "30日波动率%": r.volatility_30d,
                "60日波动率%": r.volatility_60d,
                "股息率%": r.dividend_yield,
                "ROE%": r.roe,
                "利润增长%": r.profit_growth,
                "波动率评分": r.volatility_score,
                "股息率评分": r.dividend_score,
                "成长性评分": r.growth_score,
                "综合评分": r.total_score,
                "投资建议": r.suggestion,
                "数据来源": r.data_source
            })
        
        df = pd.DataFrame(data)
        output_file = "/mnt/user-data/outputs/全局股票分析结果.csv"
        df.to_csv(output_file, index=False, encoding="utf_8_sig")
        
        db.close()
        return FileResponse(output_file, filename="全局股票分析结果.csv")
        
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/export/user")
def export_user_csv(user_id: str):
    """导出用户关注股票的分析结果到CSV"""
    db = SessionLocal()
    try:
        # 获取用户关注的股票代码
        watched = db.query(UserStockWatch.stock_code).filter(
            UserStockWatch.user_id == user_id
        ).all()
        
        watched_codes = [w[0] for w in watched]
        
        if not watched_codes:
            db.close()
            raise HTTPException(status_code=404, detail="用户未关注任何股票")
        
        # 获取这些股票的最新分析结果
        subquery = db.query(
            StockAnalysisResult.stock_code,
            func.max(StockAnalysisResult.analysis_date).label('max_date')
        ).filter(
            StockAnalysisResult.stock_code.in_(watched_codes)
        ).group_by(StockAnalysisResult.stock_code).subquery()
        
        results = db.query(StockAnalysisResult).join(
            subquery,
            (StockAnalysisResult.stock_code == subquery.c.stock_code) &
            (StockAnalysisResult.analysis_date == subquery.c.max_date)
        ).order_by(desc(StockAnalysisResult.total_score)).all()
        
        data = []
        for r in results:
            data.append({
                "股票代码": r.stock_code,
                "股票名称": r.stock_name,
                "分析日期": str(r.analysis_date),
                "最新价": r.latest_price,
                "市盈率": r.pe_ratio,
                "市净率": r.pb_ratio,
                "30日波动率%": r.volatility_30d,
                "60日波动率%": r.volatility_60d,
                "股息率%": r.dividend_yield,
                "ROE%": r.roe,
                "利润增长%": r.profit_growth,
                "波动率评分": r.volatility_score,
                "股息率评分": r.dividend_score,
                "成长性评分": r.growth_score,
                "综合评分": r.total_score,
                "投资建议": r.suggestion,
                "数据来源": r.data_source
            })
        
        df = pd.DataFrame(data)
        output_file = f"/mnt/user-data/outputs/用户{user_id}_股票分析结果.csv"
        df.to_csv(output_file, index=False, encoding="utf_8_sig")
        
        db.close()
        return FileResponse(output_file, filename=f"用户{user_id}_股票分析结果.csv")
        
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

# --- 系统状态接口 ---

@app.get("/status")
def get_status():
    """获取系统状态"""
    db = SessionLocal()
    try:
        user_count = db.query(User).count()
        watch_count = db.query(UserStockWatch).count()
        market_data_count = db.query(DailyMarketData).count()
        historical_data_count = db.query(HistoricalData).count()
        dividend_data_count = db.query(DividendData).count()
        analysis_count = db.query(StockAnalysisResult).count()
        
        # 最新市场数据日期
        latest_market = db.query(func.max(DailyMarketData.date)).scalar()
        
        # 最新分析日期
        latest_analysis = db.query(func.max(StockAnalysisResult.analysis_date)).scalar()
        
        db.close()
        
        return {
            "system": "股票价值分析系统 v2.0",
            "users": user_count,
            "watched_stocks": watch_count,
            "market_data_records": market_data_count,
            "historical_data_records": historical_data_count,
            "dividend_data_records": dividend_data_count,
            "analysis_records": analysis_count,
            "latest_market_date": str(latest_market) if latest_market else None,
            "latest_analysis_date": str(latest_analysis) if latest_analysis else None,
            "scheduler_running": scheduler.running
        }
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    print("=" * 60)
    print("股票价值分析系统 v2.0 - 启动中...")
    print("=" * 60)
    print("\n核心功能:")
    print("✓ 多用户支持")
    print("✓ 三个数据源独立存储(市场/历史/分红)")
    print("✓ 定时任务: 15:30获取市场数据, 16:00分析股票")
    print("✓ 手动分析支持")
    print("✓ 全局/用户CSV导出")
    print("✓ 智能评分系统(波动率+股息率+成长性)")
    print("\n访问文档: http://localhost:8000/docs")
    print("=" * 60)
    uvicorn.run(app, host="0.0.0.0", port=8000)