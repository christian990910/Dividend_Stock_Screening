import os
import time
from contextlib import asynccontextmanager
import datetime
import asyncio
import re
import json
import pandas as pd
import numpy as np
import akshare as ak
from typing import Optional, List
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, desc, func, Boolean, Date
from sqlalchemy.orm import sessionmaker, Session, declarative_base
from starlette.background import BackgroundTasks
from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import FileResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import urllib3
import random
import efinance as ef

# ============================================================
# 网络配置 - 禁用代理和SSL警告
# ============================================================
# ============================================================
# 顶级补丁：全局拦截 requests，强制伪装并禁用代理
# ============================================================
from requests.sessions import Session

_orig_request = Session.request

def my_request(self, method, url, **kwargs):
    # 1. 强制抹除代理 (解决 RemoteDisconnected 的核心)
    kwargs['proxies'] = {'http': None, 'https': None}
    
    # 2. 注入伪装 Headers (如果接口没传 headers，我们就给它一个)
    if 'headers' not in kwargs or not kwargs['headers']:
        kwargs['headers'] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Connection': 'keep-alive'
        }
    
    # 3. 延长超时
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 30
        
    return _orig_request(self, method, url, **kwargs)

# 实施全局拦截：从此所有调用 requests 的库 (akshare, efinance) 都会带上伪装
Session.request = my_request

# 方法1: 环境变量禁用代理
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''
os.environ['http_proxy'] = ''
os.environ['https_proxy'] = ''
os.environ['NO_PROXY'] = '*'
os.environ['no_proxy'] = '*'

# 方法2: 禁用urllib3的SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 方法3: 禁用requests的代理(如果akshare内部使用requests)
import requests
requests.packages.urllib3.disable_warnings()

# ============================================================
# 数据库配置
# ============================================================

SQLALCHEMY_DATABASE_URL = "sqlite:///./stock_advanced_system.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- 数据模型 ---

class User(Base):
    """用户表"""
    __tablename__ = "users"
    user_id = Column(String, primary_key=True)
    username = Column(String, unique=True)
    created_at = Column(DateTime, default=datetime.datetime.now)

class UserStockWatch(Base):
    """用户关注股票列表"""
    __tablename__ = "user_stock_watch"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, index=True)
    stock_code = Column(String, index=True)
    added_at = Column(DateTime, default=datetime.datetime.now)

class DailyMarketData(Base):
    """接口1: 每日全市场实时数据 - stock_zh_a_spot_em"""
    __tablename__ = "daily_market_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, index=True)
    code = Column(String, index=True)
    name = Column(String)
    latest_price = Column(Float)
    change_pct = Column(Float)
    change_amount = Column(Float)
    volume = Column(Float)
    amount = Column(Float)
    amplitude = Column(Float)
    high = Column(Float)
    low = Column(Float)
    open = Column(Float)
    close_prev = Column(Float)
    volume_ratio = Column(Float)
    turnover_rate = Column(Float)
    pe_dynamic = Column(Float)
    pb = Column(Float)
    total_market_cap = Column(Float)
    circulating_market_cap = Column(Float)
    rise_speed = Column(Float)
    change_5min = Column(Float)
    change_60day = Column(Float)
    change_ytd = Column(Float)
    updated_at = Column(DateTime, default=datetime.datetime.now)

class HistoricalData(Base):
    """接口2: 历史行情数据 - stock_zh_a_hist (前复权)"""
    __tablename__ = "historical_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String, index=True)
    date = Column(Date, index=True)
    open = Column(Float)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Integer)
    amount = Column(Float)
    amplitude = Column(Float)
    change_pct = Column(Float)
    change_amount = Column(Float)
    turnover_rate = Column(Float)
    created_at = Column(DateTime, default=datetime.datetime.now)

class DividendData(Base):
    """接口3: 分红派息数据 - news_trade_notify_dividend_baidu"""
    __tablename__ = "dividend_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String, index=True)
    stock_name = Column(String)
    ex_dividend_date = Column(Date, index=True)
    dividend = Column(String)
    bonus_share = Column(String)
    capitalization = Column(String)
    physical = Column(String)
    exchange = Column(String)
    report_period = Column(String)
    created_at = Column(DateTime, default=datetime.datetime.now)

class StockAnalysisResult(Base):
    """股票分析结果表"""
    __tablename__ = "stock_analysis_results"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String, index=True)
    stock_name = Column(String)
    analysis_date = Column(Date, index=True)
    
    latest_price = Column(Float)
    pe_ratio = Column(Float)
    pb_ratio = Column(Float)
    
    volatility_30d = Column(Float)
    volatility_60d = Column(Float)
    
    dividend_yield = Column(Float)
    
    roe = Column(Float)
    profit_growth = Column(Float)
    
    volatility_score = Column(Integer)
    dividend_score = Column(Integer)
    growth_score = Column(Integer)
    total_score = Column(Integer)
    suggestion = Column(String)
    
    data_source = Column(String)
    
    created_at = Column(DateTime, default=datetime.datetime.now)

Base.metadata.create_all(bind=engine)

# ============================================================
# 重试装饰器
# ============================================================

def retry_on_error(max_retries=3, delay=2, backoff=2):
    """
    重试装饰器
    max_retries: 最大重试次数
    delay: 初始延迟秒数
    backoff: 延迟倍数
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        # 最后一次重试失败,抛出异常
                        raise e
                    
                    print(f"⚠️  第{attempt + 1}次尝试失败: {str(e)}")
                    print(f"   等待{current_delay}秒后重试...")
                    await asyncio.sleep(current_delay)
                    current_delay *= backoff
            
        return wrapper
    return decorator

# ============================================================
# 数据服务层
# ============================================================

class StockDataService:
    def __init__(self):
        self.last_market_fetch = None
        # 定义东财字段与数据库字段的映射
        self.em_fields_map = {
            'f12': 'code', 'f14': 'name', 'f2': 'latest_price', 'f3': 'change_pct',
            'f4': 'change_amount', 'f5': 'volume', 'f6': 'amount', 'f7': 'amplitude',
            'f15': 'high', 'f16': 'low', 'f17': 'open', 'f18': 'close_prev',
            'f8': 'turnover_rate', 'f9': 'pe_dynamic', 'f23': 'pb',
            'f20': 'total_market_cap', 'f21': 'circulating_market_cap',
            'f11': 'rise_speed', 'f22': 'change_5min'
        }

    def get_db(self) -> Session:
        db = SessionLocal()
        try: return db
        finally: pass

    def _safe_float(self, val):
        """安全转换 float 辅助函数"""
        try:
            if pd.isna(val) or val == '-' or val is None: return None
            return float(val)
        except:
            return None

    async def fetch_em_data_via_web_api(self, page_size: int = 100) -> pd.DataFrame:
        """【方案一】原生网页 API 分页请求方式"""
        all_dfs = []
        current_page = 1
        total_pages = 999
        
        url = "http://82.push2.eastmoney.com/api/qt/clist/get"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Referer": "http://quote.eastmoney.com/center/gridlist.html"
        }

        print(f"\n🌐 启动原生网页 API 抓取模式 (每页 {page_size} 条)")
        
        while current_page <= total_pages:
            params = {
                "pn": current_page, "pz": page_size,
                "po": "1", "np": "1", "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": "2", "invt": "2", "fid": "f3",
                "fs": "m:0 t:6,m:0 t:80,m:1 t:2,m:1 t:23,m:0 t:81 s:2048",
                "fields": ",".join(self.em_fields_map.keys()),
            }

            try:
                print(f"   ➤ 抓取第 {current_page}/{total_pages if total_pages != 999 else '?'} 页...")
                response = await asyncio.to_thread(
                    requests.get, 
                    url, 
                    params=params, 
                    headers=headers, 
                    timeout=20
                )
                res_json = response.json()
                
                if not res_json or 'data' not in res_json or res_json['data'] is None:
                    print(f"   ⚠️ 第 {current_page} 页未能获取有效数据")
                    break

                # 首页请求时更新总页数
                if current_page == 1:
                    total_records = res_json['data']['total']
                    total_pages = (total_records + page_size - 1) // page_size
                    print(f"   📊 全市场共计 {total_records} 只股票，预计爬取 {total_pages} 页")

                batch_df = pd.DataFrame(res_json['data']['diff'])
                all_dfs.append(batch_df)
                
                if current_page >= total_pages: break

                # 核心要求：随机间隔 10-50 秒
                wait_time = random.uniform(10, 50)
                print(f"   💤 随机等待 {wait_time:.1f} 秒以规避风控...")
                await asyncio.sleep(wait_time)
                
                current_page += 1

            except Exception as e:
                print(f"   ❌ 第 {current_page} 页出错: {str(e)[:50]}。60秒后重试...")
                await asyncio.sleep(60)
                continue

        if not all_dfs: return pd.DataFrame()
        
        final_df = pd.concat(all_dfs, ignore_index=True)
        # 统一字段名
        final_df = final_df.rename(columns={k: v for k, v in self.em_fields_map.items()})
        return final_df

    async def fetch_via_efinance(self) -> pd.DataFrame:
        """【方案二/保底方案】使用 efinance 库获取数据"""
        print("\n🔄 切换至 efinance 保底模式获取全量数据...")
        try:
            # efinance 的获取通常比较快，因为它内部做了多线程/并发优化
            df = ef.stock.get_realtime_quotes()
            if df.empty: return pd.DataFrame()
            
            # 将 efinance 的中文列名映射回系统统一的英文名
            ef_map = {
                '股票代码': 'code', '股票名称': 'name', '最新价': 'latest_price',
                '涨跌幅': 'change_pct', '涨跌额': 'change_amount', '成交量': 'volume',
                '成交额': 'amount', '振幅': 'amplitude', '最高': 'high', '最低': 'low',
                '今开': 'open', '昨收': 'close_prev', '换手率': 'turnover_rate',
                '动态市盈率': 'pe_dynamic', '市净率': 'pb', '总市值': 'total_market_cap',
                '流通市值': 'circulating_market_cap', '涨速': 'rise_speed'
            }
            df = df.rename(columns=ef_map)
            print(f"   ✅ efinance 成功获取 {len(df)} 条数据")
            return df
        except Exception as e:
            print(f"   ❌ efinance 模式也失效: {e}")
            return pd.DataFrame()

    @retry_on_error(max_retries=2, delay=5)
    async def fetch_daily_market_data(self, force: bool = False) -> dict:
        """主入口：具备切换机制的获取逻辑"""
        db = self.get_db()
        today = datetime.date.today()
        
        # 1. 检查今日是否已有数据
        if not force:
            existing = db.query(DailyMarketData).filter(DailyMarketData.date == today).first()
            if existing:
                db.close()
                return {"status": "skip", "message": "今日数据已存在"}

        print(f"\n{'='*60}\n📊 开始执行每日全市场数据采集程序")
        
        # 2. 尝试方案一（原生 API）
        try:
            df = await self.fetch_em_data_via_web_api(page_size=100)
            source = "EM_WebAPI"
        except Exception as e:
            print(f"⚠️ 方案一失败，正在启动方案二...")
            df = pd.DataFrame()

        # 3. 尝试方案二（efinance）
        if df.empty:
            df = await self.fetch_via_efinance()
            source = "efinance"

        if df.empty:
            db.close()
            return {"status": "error", "message": "所有数据源均不可用"}

        # 4. 数据清理与保存
        try:
            # 清理旧数据
            db.query(DailyMarketData).filter(DailyMarketData.date == today).delete()
            db.commit()

            print(f"\n💾 正在将 {len(df)} 只股票存入数据库...")
            batch_data = []
            for _, row in df.iterrows():
                # 使用 _safe_float 处理各种异常值
                m = DailyMarketData(
                    date=today,
                    code=str(row['code']),
                    name=str(row['name']),
                    latest_price=self._safe_float(row.get('latest_price')),
                    change_pct=self._safe_float(row.get('change_pct')),
                    change_amount=self._safe_float(row.get('change_amount')),
                    volume=self._safe_float(row.get('volume')),
                    amount=self._safe_float(row.get('amount')),
                    amplitude=self._safe_float(row.get('amplitude')),
                    high=self._safe_float(row.get('high')),
                    low=self._safe_float(row.get('low')),
                    open=self._safe_float(row.get('open')),
                    close_prev=self._safe_float(row.get('close_prev')),
                    turnover_rate=self._safe_float(row.get('turnover_rate')),
                    pe_dynamic=self._safe_float(row.get('pe_dynamic')),
                    pb=self._safe_float(row.get('pb')),
                    total_market_cap=self._safe_float(row.get('total_market_cap')),
                    circulating_market_cap=self._safe_float(row.get('circulating_market_cap')),
                    rise_speed=self._safe_float(row.get('rise_speed')),
                    updated_at=datetime.datetime.now()
                )
                batch_data.append(m)
                
                if len(batch_data) >= 500:
                    db.bulk_save_objects(batch_data)
                    db.commit()
                    batch_data = []

            if batch_data:
                db.bulk_save_objects(batch_data)
                db.commit()

            print(f"✅ 数据采集完成！来源: {source}, 总计: {len(df)} 条")
            db.close()
            return {"status": "success", "source": source, "count": len(df)}

        except Exception as e:
            db.close()
            print(f"❌ 存储入库失败: {e}")
            raise
    
    @retry_on_error(max_retries=2, delay=2)
    async def fetch_historical_data(self, stock_code: str, start_date: str = None, 
                                    end_date: str = None, period: str = "daily") -> dict:
        """
        获取指定股票的历史数据 (接口2: stock_zh_a_hist)
        使用前复权数据
        """
        db = self.get_db()
        try:
            if not end_date:
                end_date = datetime.date.today().strftime("%Y%m%d")
            if not start_date:
                start_date = (datetime.date.today() - datetime.timedelta(days=500)).strftime("%Y%m%d")
            
            print(f"📈 获取 {stock_code} 历史数据: {start_date} 至 {end_date}")
            
            df = pd.DataFrame()
            data_source = ""

            # --- 方案 1: 优先尝试 efinance (稳定性高，带伪装) ---
            try:
                # efinance 的 get_quote_history 会自动处理复权，默认是前复权
                df = ef.stock.get_quote_history(stock_code)
                if not df.empty:
                    # efinance 返回的是全量，我们需要按日期过滤
                    # 将 '日期' 列转换为字符串格式以便对比，或者统一转为 datetime
                    df['日期'] = pd.to_datetime(df['日期'])
                    # 转换 start_date 和 end_date 为 datetime 对象
                    s_dt = pd.to_datetime(start_date, format='%Y%m%d')
                    e_dt = pd.to_datetime(end_date, format='%Y%m%d')
                    
                    df = df[(df['日期'] >= s_dt) & (df['日期'] <= e_dt)]
                    
                    # 映射 efinance 的中文列名到数据库字段名
                    df = df.rename(columns={
                        '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low',
                        '成交量': 'volume', '成交额': 'amount', '振幅': 'amplitude',
                        '涨跌幅': 'change_pct', '涨跌额': 'change_amount', '换手率': 'turnover_rate'
                    })
                    data_source = "efinance"
            except Exception as e:
                print(f"   ⚠️ efinance 获取失败: {e}，尝试切换 AkShare...")

            # --- 方案 2: AkShare 保底 ---
            if df.empty:
                try:
                    df = ak.stock_zh_a_hist(
                        symbol=stock_code,
                        period=period,
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq"
                    )
                    if not df.empty:
                        # AkShare 的列名也是中文，需要映射
                        df = df.rename(columns={
                            '日期': '日期', '开盘': 'open', '收盘': 'close', '最高': 'high', '最低': 'low',
                            '成交量': 'volume', '成交额': 'amount', '振幅': 'amplitude',
                            '涨跌幅': 'change_pct', '涨跌额': 'change_amount', '换手率': 'turnover_rate'
                        })
                        data_source = "akshare"
                except Exception as e:
                    print(f"   ❌ AkShare 保底也失败: {e}")

            if df.empty:
                db.close()
                return {"status": "error", "message": f"股票 {stock_code} 无历史数据"}
            
            # --- 数据入库逻辑 ---
            # 删除旧数据
            target_start = datetime.datetime.strptime(start_date, "%Y%m%d").date()
            target_end = datetime.datetime.strptime(end_date, "%Y%m%d").date()
            
            db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code,
                HistoricalData.date >= target_start,
                HistoricalData.date <= target_end
            ).delete()
            
            count = 0
            for _, row in df.iterrows():
                # 处理日期：efinance 返回可能是 Timestamp
                raw_date = row['日期']
                if isinstance(raw_date, pd.Timestamp):
                    final_date = raw_date.date()
                else:
                    final_date = datetime.datetime.strptime(str(raw_date), "%Y-%m-%d").date()

                hist_data = HistoricalData(
                    stock_code=stock_code,
                    date=final_date,
                    open=self._safe_float(row.get('open')),
                    close=self._safe_float(row.get('close')),
                    high=self._safe_float(row.get('high')),
                    low=self._safe_float(row.get('low')),
                    volume=int(row.get('volume', 0)) if pd.notna(row.get('volume')) else 0,
                    amount=self._safe_float(row.get('amount')),
                    amplitude=self._safe_float(row.get('amplitude')),
                    change_pct=self._safe_float(row.get('change_pct')),
                    change_amount=self._safe_float(row.get('change_amount')),
                    turnover_rate=self._safe_float(row.get('turnover_rate')),
                )
                db.add(hist_data)
                count += 1
            
            db.commit()
            db.close()
            print(f"   ✓ 来源[{data_source}] 保存 {count} 条历史数据")
            
            return {
                "status": "success",
                "source": data_source,
                "count": count,
                "start_date": start_date,
                "end_date": end_date
            }
            
        except Exception as e:
            if db: db.close()
            print(f"   ✗ 获取历史数据流程崩溃: {str(e)}")
            raise
    
    async def fetch_dividend_data(self, date_str: str = None) -> dict:
        """
        获取指定日期的分红派息数据 (接口3: news_trade_notify_dividend_baidu)
        """
        db = self.get_db()
        try:
            if not date_str:
                date_str = datetime.date.today().strftime("%Y%m%d")
            
            print(f"💰 获取 {date_str} 分红数据...")
            
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
            
            print(f"   ✓ 保存 {count} 条分红数据")
            
            return {
                "status": "success",
                "message": f"成功获取 {date_str} 分红数据",
                "count": count,
                "date": date_str
            }
            
        except Exception as e:
            db.close()
            return {"status": "error", "message": f"获取分红数据失败: {str(e)}"}
        
    async def fetch_stock_financials(self, stock_code: str):
        """获取个股关键财务指标 (ROE, 净利增长)"""
        try:
            # 使用 efinance 获取基础信息 (包含 ROE 等)
            # 注意：ef.stock.get_base_info 返回的是 DataFrame
            df = await asyncio.to_thread(ef.stock.get_base_info, stock_code)
            if df.empty: return 0.0, 0.0
            
            # 这里的字段名通常是：'净资产收益率(%)', '净利润同比(%)'
            # 不同版本的 efinance 字段名可能有细微差别，建议加个 try-catch
            roe = self._safe_float(df.iloc[0].get('净资产收益率(%)', 0))
            growth = self._safe_float(df.iloc[0].get('净利润同比(%)', 0))
            return roe, growth
        except:
            print(f"      ⚠️ 财务数据获取失败 ({stock_code}): {e}")
            return 0.0, 0.0
        
    async def analyze_stock(self, stock_code: str, db: Session = None) -> dict:
        """分析单只股票"""
        should_close = False
        if db is None:
            db = self.get_db()
            should_close = True
        
        try:
            today = datetime.date.today()
            
            # 1. 获取最新市场数据
            market_data = db.query(DailyMarketData).filter(
                DailyMarketData.code == stock_code,
                DailyMarketData.date == today
            ).first()
            
            if not market_data:
                market_data = db.query(DailyMarketData).filter(
                    DailyMarketData.code == stock_code
                ).order_by(desc(DailyMarketData.date)).first()
            
            if not market_data:
                if should_close:
                    db.close()
                return {"status": "error", "message": f"股票 {stock_code} 无市场数据"}
            
            latest_price = market_data.latest_price
            pe_ratio = market_data.pe_dynamic
            pb_ratio = market_data.pb
            stock_name = market_data.name
            data_source = "market"
            
            # 2. 计算波动率
            volatility_30d = 0
            volatility_60d = 0
            
            hist_data = db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code
            ).order_by(desc(HistoricalData.date)).limit(60).all()
            
            if hist_data and len(hist_data) >= 30:
                closes = [h.close for h in reversed(hist_data) if h.close]
                
                if len(closes) >= 30:
                    series_30 = pd.Series(closes[-30:])
                    log_ret_30 = np.log(series_30 / series_30.shift(1)).dropna()
                    volatility_30d = log_ret_30.std() * np.sqrt(252) * 100
                    
                    if len(closes) >= 60:
                        series_60 = pd.Series(closes[-60:])
                        log_ret_60 = np.log(series_60 / series_60.shift(1)).dropna()
                        volatility_60d = log_ret_60.std() * np.sqrt(252) * 100
                    
                    data_source = "mixed"
            
            # 如果历史数据不足,尝试获取
            if volatility_30d == 0:
                print(f"   {stock_code} 历史数据不足，尝试获取...")
                await self.fetch_historical_data(stock_code)
                
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
            
            # 3. 计算股息率
            dividend_yield = 0
            one_year_ago = today - datetime.timedelta(days=365)
            dividends = db.query(DividendData).filter(
                DividendData.stock_code == stock_code,
                DividendData.ex_dividend_date >= one_year_ago
            ).all()
            
            if dividends and latest_price:
                total_dividend = 0
                for div in dividends:
                    div_str = str(div.dividend)
                    match = re.search(r'(\d+\.?\d*)', div_str)
                    if match:
                        total_dividend += float(match.group(1))
                
                if total_dividend > 0:
                    dividend_yield = (total_dividend / latest_price) * 100
                    data_source = "mixed"
            
            # 4. ROE和成长性
            print(f"   正在获取 {stock_code} 财务数据...")
            roe, profit_growth = await self.fetch_stock_financials(stock_code)
            
            # --- 5. 综合评分系统 ---
            
            # (A) 波动率评分 (最高 40分) - 越低分越高，代表稳健
            volatility_score = 0
            if volatility_30d > 0:
                if volatility_30d < 20: volatility_score = 40
                elif volatility_30d < 30: volatility_score = 30
                elif volatility_30d < 40: volatility_score = 20
                elif volatility_30d < 50: volatility_score = 10
            
            # (B) 股息率评分 (最高 30分) - 现金红利能力
            dividend_score = 0
            if dividend_yield >= 5: dividend_score = 30
            elif dividend_yield >= 4: dividend_score = 25
            elif dividend_yield >= 3: dividend_score = 20
            elif dividend_yield >= 2: dividend_score = 15
            elif dividend_yield >= 1: dividend_score = 10
            
            # (C) 成长性评分 (最高 30分) - ROE(20分) + 利润增长(10分)
            growth_score = 0
            
            # ROE 子项 (20分)
            if roe > 15: growth_score += 20
            elif roe > 10: growth_score += 15
            elif roe > 5: growth_score += 10
            elif roe > 0: growth_score += 5
            
            # 利润增长子项 (10分)
            if profit_growth > 20: growth_score += 10
            elif profit_growth > 10: growth_score += 7
            elif profit_growth > 0: growth_score += 4
            
            # --- 总分计算 ---
            total_score = volatility_score + dividend_score + growth_score
            
            # --- 投资建议逻辑 ---
            if total_score >= 80:
                suggestion = "🌟 极高价值 (财务强健+高分红+低波动)"
            elif total_score >= 70:
                suggestion = "强烈推荐"
            elif total_score >= 60:
                suggestion = "推荐"
            elif total_score >= 50:
                suggestion = "可以关注"
            elif total_score >= 40:
                suggestion = "观望"
            else:
                suggestion = "不推荐 (风险较高或价值不足)"
            
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
        """分析所有用户关注的股票"""
        db = self.get_db()
        try:
            watched_stocks = db.query(UserStockWatch.stock_code).distinct().all()
            watched_codes = [s[0] for s in watched_stocks]
            
            if not watched_codes:
                db.close()
                return {"status": "success", "message": "没有用户关注的股票", "count": 0}
            
            print(f"\n{'='*60}")
            print(f"📊 开始分析 {len(watched_codes)} 只被关注的股票")
            print(f"{'='*60}\n")
            
            success_count = 0
            error_count = 0
            
            for i, code in enumerate(watched_codes, 1):
                print(f"[{i}/{len(watched_codes)}] 分析 {code}...")
                result = await self.analyze_stock(code, db)
                if result["status"] == "success":
                    success_count += 1
                    print(f"   ✓ {result.get('stock_name', code)} - 评分:{result.get('total_score', 0)} - {result.get('suggestion', '')}")
                else:
                    error_count += 1
                    print(f"   ✗ {result.get('message', '')}")
                
                await asyncio.sleep(0.3)
            
            print(f"\n{'='*60}")
            print(f"✅ 分析完成: 成功 {success_count}, 失败 {error_count}")
            print(f"{'='*60}\n")
            
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



stock_service = StockDataService()

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""

    # ================= 启动阶段 =================
    print("\n🚀 正在启动价值分析系统...\n")

    scheduler.add_job(
        stock_service.fetch_daily_market_data,
        CronTrigger(hour=15, minute=30),
        id="daily_market_fetch",
        replace_existing=True
    )

    scheduler.add_job(
        stock_service.analyze_all_watched_stocks,
        CronTrigger(hour=16, minute=0),
        id="daily_analysis",
        replace_existing=True
    )

    scheduler.start()

    print("✅ 定时任务已启动:")
    print("   - 每日15:30获取全市场数据")
    print("   - 每日16:00分析所有关注股票\n")

    yield  # 👈 关键：生命周期分界线

    # ================= 关闭阶段 =================
    print("\n🛑 正在关闭系统...")

    if scheduler and scheduler.running:
        scheduler.shutdown()

    print("✅ 定时任务已安全停止\n")

# --- FastAPI应用 ---
app = FastAPI(
    title="价值分析系统 v2.1",
    version="2.1",
    lifespan=lifespan
)

# --- API接口 ---

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

@app.post("/watch/add")
def add_watch_stock(user_id: str, stock_codes: str):
    """添加股票到用户关注列表"""
    db = SessionLocal()
    try:
        user = db.query(User).filter(User.user_id == user_id).first()
        if not user:
            db.close()
            raise HTTPException(status_code=404, detail="用户不存在")
        
        codes = re.findall(r'\d{6}', stock_codes)
        added = 0
        
        for code in set(codes):
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

@app.post("/data/market/fetch")
async def fetch_market_data(force: bool = False):
    """手动获取全市场数据"""
    result = await stock_service.fetch_daily_market_data(force=force)
    return result

@app.post("/data/history/fetch")
async def fetch_history_data(stock_code: str, start_date: str = None, end_date: str = None):
    """手动获取指定股票的历史数据"""
    result = await stock_service.fetch_historical_data(stock_code, start_date, end_date)
    return result

@app.post("/data/dividend/fetch")
async def fetch_dividend_data(date_str: str = None):
    """手动获取分红数据"""
    result = await stock_service.fetch_dividend_data(date_str)
    return result

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

@app.get("/export/global")
def export_global_csv():
    """导出全局分析结果到CSV"""
    db = SessionLocal()
    try:
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
        out_dir = "outputs"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            
        df = pd.DataFrame(data)
        output_file = os.path.join(out_dir, "全局股票分析结果.csv")
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
        watched = db.query(UserStockWatch.stock_code).filter(
            UserStockWatch.user_id == user_id
        ).all()
        
        watched_codes = [w[0] for w in watched]
        
        if not watched_codes:
            db.close()
            raise HTTPException(status_code=404, detail="用户未关注任何股票")
        
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
        out_dir = "outputs"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
            
        df = pd.DataFrame(data)
        output_file = os.path.join(out_dir, "用户{user_id}_股票分析结果.csv")
        df.to_csv(output_file, index=False, encoding="utf_8_sig")
        
        db.close()
        return FileResponse(output_file, filename="用户{user_id}_股票分析结果.csv")
        
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

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
        
        latest_market = db.query(func.max(DailyMarketData.date)).scalar()
        latest_analysis = db.query(func.max(StockAnalysisResult.analysis_date)).scalar()
        
        db.close()
        
        return {
            "system": "股票价值分析系统 v2.1 (网络优化版)",
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
    print("\n" + "="*60)
    print("🚀 股票价值分析系统 v2.1 - 网络优化版")
    print("="*60)
    print("\n✨ 新增功能:")
    print("  • 智能重试机制 (最多3次重试)")
    print("  • 彻底禁用代理")
    print("  • 批量保存优化")
    print("  • 详细进度显示")
    print("\n📚 核心功能:")
    print("  ✓ 多用户支持")
    print("  ✓ 三数据源独立存储")
    print("  ✓ 自动定时任务 (15:30 + 16:00)")
    print("  ✓ 智能评分系统")
    print("\n🔗 访问:")
    print("  API文档: http://localhost:8000/docs")
    print("  系统状态: http://localhost:8000/status")
    print("="*60 + "\n")
    
    uvicorn.run(app, host="0.0.0.0", port=8000)