import os
import socket
# 1. 顶级补丁：强制 IPv4 (解决连接被重置的核心)
orig_getaddrinfo = socket.getaddrinfo
def patched_getaddrinfo(*args, **kwargs):
    res = orig_getaddrinfo(*args, **kwargs)
    return [r for r in res if r[0] == socket.AF_INET]
socket.getaddrinfo = patched_getaddrinfo

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
import requests
from requests.sessions import Session as RequestSession

# ============================================================
# 网络配置与全局拦截补丁
# ============================================================
_orig_request = RequestSession.request
def my_request(self, method, url, **kwargs):
    # 强制抹除代理并注入全局伪装
    kwargs['proxies'] = {'http': None, 'https': None}
    if 'headers' not in kwargs or not kwargs['headers']:
        kwargs['headers'] = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Accept': '*/*',
            'Connection': 'keep-alive'
        }
    if 'timeout' not in kwargs:
        kwargs['timeout'] = 30
    return _orig_request(self, method, url, **kwargs)

RequestSession.request = my_request

os.environ['NO_PROXY'] = '*'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ============================================================
# 数据库配置
# ============================================================
SQLALCHEMY_DATABASE_URL = "sqlite:///./stock_advanced_system.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- 数据模型 (保持完整) ---
class User(Base):
    __tablename__ = "users"
    user_id = Column(String, primary_key=True)
    username = Column(String, unique=True)
    created_at = Column(DateTime, default=datetime.datetime.now)

class UserStockWatch(Base):
    __tablename__ = "user_stock_watch"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, index=True)
    stock_code = Column(String, index=True)
    added_at = Column(DateTime, default=datetime.datetime.now)

class DailyMarketData(Base):
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
    updated_at = Column(DateTime, default=datetime.datetime.now)

class HistoricalData(Base):
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
# 核心数据服务层
# ============================================================
class StockDataService:
    def __init__(self):
        # 1:1 复刻你成功采集的凭证
        self.target_ut = "fa5fd1943c7b386f172d6893dbfba10b"
        self.target_cookies = {
            "qgqp_b_id": "9fb8c26c0a40e0e20ffd551bb6a52cdf",
            "st_nvi": "4U97b8QAwVvKIFT5nsAGl367a",
            "st_si": "69103863020676",
            "nid18": "03c4e656b6d9f1dfd8b102df6f142ef1",
            "st_sn": "23"
        }
        # 东财 f 字段映射到模型
        self.em_fields_map = {
            'f12': 'code', 'f14': 'name', 'f2': 'latest_price', 'f3': 'change_pct',
            'f4': 'change_amount', 'f5': 'volume', 'f6': 'amount', 'f7': 'amplitude',
            'f15': 'high', 'f16': 'low', 'f17': 'open', 'f18': 'close_prev',
            'f8': 'turnover_rate', 'f9': 'pe_dynamic', 'f23': 'pb',
            'f20': 'total_market_cap', 'f21': 'circulating_market_cap', 'f11': 'rise_speed'
        }

    def get_db(self) -> Session:
        db = SessionLocal()
        return db

    def _safe_float(self, val):
        try:
            if pd.isna(val) or val == '-' or val is None: return None
            return float(val)
        except: return None

    def refresh_ut(self):
        """
        自动从东方财富网页中提取最新 ut
        """
        print("🔄 正在刷新 ut 参数...")

        try:
            url = "https://quote.eastmoney.com/center/gridlist.html"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0 Safari/537.36"
            }

            response = requests.get(
                url,
                headers=headers,
                timeout=10,
                verify=False,
                proxies={"http": None, "https": None}
            )

            # 在页面JS里匹配 ut
            match = re.search(r'ut:\s*"([a-z0-9]+)"', response.text)

            if match:
                new_ut = match.group(1)
                self.target_ut = new_ut
                print(f"✅ 成功刷新 ut: {new_ut}")
                return True
            else:
                print("❌ 未能提取到 ut")
                return False

        except Exception as e:
            print("❌ 刷新 ut 失败:", e)
            return False

    async def fetch_em_data_via_web_api(self, page_size: int = 100) -> pd.DataFrame:
        all_dfs = []
        current_page = 1
        total_pages = 999

        url = "https://push2.eastmoney.com/api/qt/clist/get"

        headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive",
            "Referer": "https://quote.eastmoney.com/center/gridlist.html",
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1"
        }

        print(f"\n🌐 启动修正版网页 API 抓取模式 (每页 {page_size} 条)")

        # 创建 session（保持你原本结构）
        session = requests.Session()
        session.trust_env = False
        session.proxies = {"http": None, "https": None}

        # ✅ 注入 cookies（关键）
        session.cookies.update(self.target_cookies)

        while current_page <= total_pages:

            params = {
                "np": "1",
                "fltt": "1",
                "invt": "2",
                "cb": f"jQuery37109323508735388775_{int(time.time()*1000)}",
                "fs": "m:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:81+s:262144+f:!2",
                "fields": ",".join(self.em_fields_map.keys()),
                "fid": "f3",
                "pn": str(current_page),
                "pz": str(page_size),
                "po": "1",
                "dect": "1",
                "ut": self.target_ut,  # 使用你类初始化的 ut
                "wbp2u": "|0|0|0|web",
                "_": str(int(time.time() * 1000))
            }

            try:
                print(f"   ➤ 抓取第 {current_page}/{total_pages if total_pages != 999 else '?'} 页...")

                response = await asyncio.to_thread(
                    session.get,
                    url,
                    params=params,
                    headers=headers,
                    timeout=20,
                    verify=False
                )

                if response.status_code != 200:
                    print(f"   ⚠️ 状态码异常: {response.status_code}")
                    break

                raw_text = response.text

                # ✅ 解析 JSONP
                json_match = re.search(r'jQuery.*?\((.*)\)', raw_text)
                if not json_match:
                    print("   ⚠️ JSONP 解析失败")
                    break

                json_str = json_match.group(1)
                res_json = json.loads(json_str)

                if not res_json or not res_json.get("data"):
                    print("⚠️ 数据为空，可能 ut 失效，尝试刷新...")

                    # 尝试刷新 ut
                    if self.refresh_ut():
                        print("🔁 使用新 ut 重试当前页...")

                        params["ut"] = self.target_ut

                        response = await asyncio.to_thread(
                            session.get,
                            url,
                            params=params,
                            headers=headers,
                            timeout=20,
                            verify=False
                        )

                        raw_text = response.text
                        json_match = re.search(r'jQuery.*?\((.*)\)', raw_text)

                        if json_match:
                            json_str = json_match.group(1)
                            res_json = json.loads(json_str)

                            if not res_json or not res_json.get("data"):
                                print("❌ 刷新 ut 后仍失败")
                                break
                        else:
                            print("❌ JSONP解析失败")
                            break
                    else:
                        break

                if current_page == 1:
                    total_records = res_json["data"]["total"]
                    total_pages = (total_records + page_size - 1) // page_size
                    print(f"   📊 全市场共 {total_records} 只股票，预计 {total_pages} 页")

                batch_df = pd.DataFrame(res_json["data"]["diff"])
                all_dfs.append(batch_df)

                if current_page >= total_pages:
                    break

                # ✅ 保留你原本的随机等待机制
                wait_time = random.uniform(10, 50)
                print(f"   💤 随机等待 {wait_time:.1f} 秒...")
                await asyncio.sleep(wait_time)

                current_page += 1

            except Exception as e:
                print(f"   ❌ 第 {current_page} 页连接失败: {str(e)[:100]}")
                break

        session.close()

        if not all_dfs:
            return pd.DataFrame()

        final_df = pd.concat(all_dfs, ignore_index=True)

        # 字段映射保持你原逻辑
        final_df = final_df.rename(columns=self.em_fields_map)

        print(f"✅ 总计获取 {len(final_df)} 条数据")

        return final_df

    async def fetch_daily_market_data(self, force: bool = False) -> dict:
        db = self.get_db()
        today = datetime.date.today()
        if not force and db.query(DailyMarketData).filter(DailyMarketData.date == today).first():
            db.close(); return {"status": "skip", "message": "今日数据已存在"}

        try:
            df = await self.fetch_em_data_via_web_api(page_size=100)
            
            if df.empty:
                print("⚠️ Web API 失败，启动 efinance 保底...")
                df = await asyncio.to_thread(ef.stock.get_realtime_quotes)
                # 映射 efinance 字段
                df = df.rename(columns={'股票代码':'code','股票名称':'name','最新价':'latest_price','涨跌幅':'change_pct','动态市盈率':'pe_dynamic','市净率':'pb'})
            
            if df.empty: 
                db.close(); return {"status": "error", "message": "无法获取行情"}

            db.query(DailyMarketData).filter(DailyMarketData.date == today).delete()
            db.commit()

            print(f"💾 存入数据库: {len(df)} 条记录")
            batch = []
            for _, row in df.iterrows():
                code = str(row.get('code') or row.get('f12'))
                m = DailyMarketData(
                    date=today, code=re.sub(r'\D', '', code),
                    name=str(row.get('name') or row.get('f14')),
                    latest_price=self._safe_float(row.get('latest_price') or row.get('f2')),
                    change_pct=self._safe_float(row.get('change_pct') or row.get('f3')),
                    pe_dynamic=self._safe_float(row.get('pe_dynamic') or row.get('f9')),
                    pb=self._safe_float(row.get('pb') or row.get('f23')),
                    total_market_cap=self._safe_float(row.get('总市值') or row.get('f20')),
                    updated_at=datetime.datetime.now()
                )
                batch.append(m)
                if len(batch) >= 500:
                    db.bulk_save_objects(batch); db.commit(); batch = []
            if batch: db.bulk_save_objects(batch); db.commit()
            db.close(); return {"status": "success", "count": len(df)}
        except Exception as e: 
            if db: db.close()
            raise e

    async def fetch_financial_metrics(self, stock_code: str):
        """个股财务数据补偿抓取"""
        try:
            df = await asyncio.to_thread(ef.stock.get_base_info, stock_code)
            if df is None or df.empty: return 0.0, 0.0
            row = df.iloc[0]
            return self._safe_float(row.get('净资产收益率(%)', 0)), self._safe_float(row.get('净利润同比(%)', 0))
        except: return 0.0, 0.0

    async def fetch_historical_data(self, stock_code: str, start_date=None, end_date=None):
        db = self.get_db()
        try:
            if not end_date: end_date = datetime.date.today().strftime("%Y%m%d")
            if not start_date: start_date = (datetime.date.today() - datetime.timedelta(days=180)).strftime("%Y%m%d")
            
            # 优先 efinance 历史
            try:
                df = await asyncio.to_thread(ef.stock.get_quote_history, stock_code)
                df = df.rename(columns={'日期':'date','收盘':'close'})
            except:
                df = await asyncio.to_thread(ak.stock_zh_a_hist, symbol=stock_code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
                df = df.rename(columns={'日期':'date','收盘':'close'})

            if df.empty: db.close(); return {"status": "error"}

            db.query(HistoricalData).filter(HistoricalData.stock_code == stock_code).delete()
            for _, row in df.iterrows():
                h = HistoricalData(stock_code=stock_code, date=pd.to_datetime(row['date']).date(), close=self._safe_float(row['close']))
                db.add(h)
            db.commit(); db.close(); return {"status": "success"}
        except: 
            if db: db.close()
            return {"status": "error"}

    async def analyze_stock(self, stock_code: str, db: Session = None) -> dict:
        is_internal = db is None
        if is_internal: db = self.get_db()
        try:
            today = datetime.date.today()
            market_data = db.query(DailyMarketData).filter(DailyMarketData.code == stock_code).order_by(desc(DailyMarketData.date)).first()
            if not market_data: return {"status": "error", "message": "无实时数据"}

            # 1. 联网补全 ROE 和 增长
            roe, profit_growth = await self.fetch_financial_metrics(stock_code)
            
            # 2. 波动率与均线
            vol_30d = 0
            trend_score = 0
            hist = db.query(HistoricalData).filter(HistoricalData.stock_code == stock_code).order_by(desc(HistoricalData.date)).limit(65).all()
            if len(hist) >= 30:
                closes = [h.close for h in reversed(hist)]
                series = pd.Series(closes)
                vol_30d = np.log(series / series.shift(1)).std() * np.sqrt(252) * 100
                if len(closes) >= 60:
                    ma60 = series.rolling(60).mean().iloc[-1]
                    trend_score = 10 if closes[-1] > ma60 else 0

            # 3. 评分系统
            # 波动 (20) + 质量 (20) + 估值 (20) + 趋势 (10) + 股息预测 (30 - 暂缺则为0)
            v_score = 20 if vol_30d < 30 else 10
            q_score = (15 if roe > 12 else 5) + (5 if profit_growth > 10 else 0)
            
            pe = market_data.pe_dynamic
            valuation_score = 20 if (pe and pe < 15) else (10 if (pe and pe < 30) else 0)
            
            total = v_score + q_score + valuation_score + trend_score
            
            res = StockAnalysisResult(
                stock_code=stock_code, stock_name=market_data.name, analysis_date=today,
                latest_price=market_data.latest_price, pe_ratio=pe, pb_ratio=market_data.pb,
                volatility_30d=round(vol_30d, 2), roe=roe, profit_growth=profit_growth,
                total_score=total, suggestion="推荐" if total > 45 else "观望"
            )
            db.merge(res); db.commit()
            return {"status": "success", "score": total}
        finally:
            if is_internal: db.close()

    async def analyze_all_watched_stocks(self):
        db = self.get_db()
        try:
            watched = db.query(UserStockWatch.stock_code).distinct().all()
            for row in watched:
                code = row[0]
                await self.fetch_historical_data(code)
                await self.analyze_stock(code, db)
                await asyncio.sleep(0.5)
        finally: db.close()

# --- FastAPI 核心逻辑 (1:1 还原业务) ---
stock_service = StockDataService()
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(stock_service.fetch_daily_market_data, CronTrigger(hour=15, minute=35), id="market_sync")
    scheduler.add_job(stock_service.analyze_all_watched_stocks, CronTrigger(hour=16, minute=10), id="auto_analysis")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(title="股票价值分析系统 v2.5", lifespan=lifespan)

@app.post("/users/create")
def create_user(user_id: str, username: str):
    db = SessionLocal()
    try:
        db.add(User(user_id=user_id, username=username))
        db.commit(); return {"message": "success"}
    except: raise HTTPException(400, "User exists")
    finally: db.close()

@app.post("/watch/add")
def add_watch(user_id: str, stock_codes: str):
    db = SessionLocal()
    codes = re.findall(r'\d{6}', stock_codes)
    for c in set(codes):
        db.merge(UserStockWatch(user_id=user_id, stock_code=c))
    db.commit(); db.close(); return {"count": len(codes)}

@app.post("/data/market/fetch")
async def fetch_market(force: bool = False, background_tasks: BackgroundTasks = BackgroundTasks()):
    background_tasks.add_task(stock_service.fetch_daily_market_data, force=force)
    return {"message": "后台采集任务已启动"}

@app.get("/export/global")
def export_global():
    db = SessionLocal()
    try:
        data = db.query(StockAnalysisResult).order_by(desc(StockAnalysisResult.total_score)).all()
        df = pd.DataFrame([{"代码": r.stock_code, "名称": r.stock_name, "总分": r.total_score, "建议": r.suggestion, "ROE": r.roe} for r in data])
        if not os.path.exists("outputs"): os.makedirs("outputs")
        path = "outputs/global_report.csv"
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return FileResponse(path)
    finally: db.close()

@app.get("/status")
def get_status():
    db = SessionLocal()
    try:
        return {
            "market_count": db.query(DailyMarketData).count(),
            "watch_count": db.query(UserStockWatch).count(),
            "analysis_count": db.query(StockAnalysisResult).count()
        }
    finally: db.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)