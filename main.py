import os
import time
import datetime
import asyncio
import re
import json
import ssl
import urllib.request
import pandas as pd
import numpy as np
import akshare as ak
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, desc, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from starlette.background import BackgroundTasks
from fastapi import FastAPI
from fastapi.responses import FileResponse

# --- 0. 强制环境设置 (彻底禁用代理，防止干扰) ---
os.environ['HTTP_PROXY'] = ''
os.environ['HTTPS_PROXY'] = ''
os.environ['no_proxy'] = '*'

# --- 1. 数据库配置 ---
SQLALCHEMY_DATABASE_URL = "sqlite:///./stock_final_v10.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class StockTrack(Base):
    __tablename__ = "stock_track"
    code = Column(String, primary_key=True)

class StockHolding(Base):
    __tablename__ = "stock_holdings"
    code = Column(String, primary_key=True)
    quantity = Column(Integer, default=0)
    avg_price = Column(Float, default=0.0)

class StockHistory(Base):
    __tablename__ = "stock_history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, index=True)
    date = Column(DateTime, index=True)
    close = Column(Float)
    pe = Column(Float)
    pb = Column(Float)
    dividend_yield = Column(Float)

class DailyAnalysis(Base):
    __tablename__ = "daily_analysis"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, index=True)
    date = Column(DateTime, default=datetime.datetime.now)
    price = Column(Float)
    pe = Column(Float)
    pb = Column(Float)
    dividend_yield = Column(Float)
    roe = Column(Float)
    profit_growth = Column(Float)
    volatility = Column(Float)
    score = Column(Integer)
    suggestion = Column(String)

Base.metadata.create_all(bind=engine)

# --- 2. 强力底层请求工具 (原生 urllib 绕过 SSL 错误) ---
# --- 1. 强力底层请求工具 (增加 SSL 兼容性设置) ---
def raw_get(url: str):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Referer': 'https://quote.eastmoney.com/'
    }
    # 强制不使用代理并降低 SSL 校验等级以兼容老旧金融服务器
    ctx = ssl.create_default_context()
    ctx.set_ciphers('DEFAULT@SECLEVEL=1')
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=10, context=ctx) as response:
            return response.read().decode('utf-8', errors='ignore')
    except Exception as e:
        # print(f"请求异常: {url[:50]}... -> {e}")
        return None

# --- 2. 分析核心服务升级版 ---
class StableAnalysisService:
    def get_realtime_backup_sina(self, code: str):
        """备用方案：新浪财经实时接口 (极其稳定)"""
        symbol = f"sh{code}" if code.startswith(('6', '9', '5')) else f"sz{code}"
        # 新浪接口返回格式比较特殊，需要解析字符串
        url = f"http://hq.sinajs.cn/list={symbol}"
        res = raw_get(url)
        if not res or len(res) < 50: return None
        try:
            # 格式: var hq_str_sh600036="招商银行,31.25,31.30,31.50,..."
            data = res.split('"')[1].split(',')
            price = float(data[3]) # 当前价格
            return {
                "price": price,
                "pe": 0, # 新浪接口实时估值字段较隐蔽，暂设为0
                "pb": 0,
                "dv": 0
            }
        except: return None

    def get_em_spot(self, code: str):
        """主方案：东财实时接口 (优化了参数与解析)"""
        # 修正 secid 逻辑
        if code.startswith(('6', '9')): 
            secid = f"1.{code}" # 上海
        else:
            secid = f"0.{code}" # 深圳

        # 增加 ut 和 invt 参数，这是东财 API 稳定的关键
        url = f"https://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c69a3b9117956acb092b192&invt=2&fltt=2&fields=f43,f162,f167,f168&secid={secid}"
        res = raw_get(url)
        if not res: return self.get_realtime_backup_sina(code)
        
        try:
            d = json.loads(res).get('data')
            if not d: return self.get_realtime_backup_sina(code)
            
            # 核心改进：东财 push2 接口返回的 f43 等有时是 float，有时是 int*100
            # 增加逻辑判断：如果值 > 10000 通常需要除以 100
            def clean_val(val):
                if val == '-' or val is None: return 0
                return float(val)

            price = clean_val(d.get('f43'))
            # 东财部分 API 返回的价格是放大 100 倍的整数
            if price > 5000: price = price / 100 
            
            pe = clean_val(d.get('f162'))
            pb = clean_val(d.get('f167'))
            dv = clean_val(d.get('f168'))

            return {"price": price, "pe": pe, "pb": pb, "dv": dv}
        except:
            return self.get_realtime_backup_sina(code)

    def get_tencent_hist(self, code: str):
        """历史 K 线补全 (保持腾讯接口，这是最稳的)"""
        symbol = f"sh{code}" if code.startswith(('6', '9')) else f"sz{code}"
        url = f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline&param={symbol},day,,,140,qfq"
        res = raw_get(url)
        if not res: return None
        try:
            # 移除 _var 变量名
            json_str = res[res.find("=")+1:]
            data = json.loads(json_str)
            target_data = data['data'][symbol]
            k_data = target_data.get('qfqday') or target_data.get('day')
            if k_data:
                return [float(k[2]) for k in k_data]
        except: return None

    async def analyze_process(self, code: str):
        db = SessionLocal()
        try:
            print(f"正在分析 {code}...")
            # 1. 实时指标 (双源保障)
            spot = self.get_em_spot(code)
            if not spot or spot['price'] == 0:
                print(f"  [跳过] 无法获取 {code} 实时数据")
                return

            # 2. 历史价格
            hist_closes = self.get_tencent_hist(code)
            vol = 0
            if hist_closes and len(hist_closes) > 30:
                hist_closes.append(spot['price'])
                # 计算对数收益率标准差
                series = pd.Series(hist_closes)
                log_ret = np.log(series / series.shift(1)).dropna()
                vol = log_ret.std() * np.sqrt(252) * 100

            # 3. 财务报表 (每只必等 30s)
            print(f"  [等待 30s] 抓取财务报表...")
            await asyncio.sleep(30)
            try:
                # 财务报表使用 AkShare，因为它处理了复杂的报表合并逻辑
                df = ak.stock_financial_analysis_indicator(symbol=code)
                if not df.empty:
                    latest = df.iloc[0]
                    roe = float(latest['净资产收益率(%)'])
                    growth = float(latest['净利润同比增长率(%)'])
                else: roe, growth = 0, 0
            except:
                roe, growth = 0, 0

            # 4. 存入历史库
            db.add(StockHistory(code=code, date=datetime.datetime.now(), 
                               close=spot['price'], pe=spot['pe'], pb=spot['pb'], dividend_yield=spot['dv']))
            db.commit()

            # 5. 评分与保存 (同前...)
            score = 0
            if spot['dv'] > 4.0: score += 40
            if 0 < vol < 26: score += 30
            if roe > 12: score += 30

            db.add(DailyAnalysis(
                code=code, price=spot['price'], pe=spot['pe'], pb=spot['pb'],
                dividend_yield=spot['dv'], roe=roe, profit_growth=growth,
                volatility=round(vol, 2), score=score, suggestion="建议买入" if score >= 70 else "观望"
            ))
            db.commit()
            print(f"  [完成] {code} 评分: {score}")

        except Exception as e:
            print(f"  [分析失败] {code}: {e}")
        finally:
            db.close()

# --- 4. FastAPI ---
app = FastAPI()
service = StableAnalysisService()

@app.post("/stocks/bulk")
def bulk_add(codes: str):
    db = SessionLocal()
    clist = re.findall(r'\d{6}', codes)
    for c in set(clist):
        db.merge(StockTrack(code=c))
    db.commit()
    db.close()
    return {"msg": f"批量录入 {len(clist)} 只股票"}

@app.post("/holdings/")
def set_holding(code: str, qty: int, price: float):
    db = SessionLocal()
    db.merge(StockHolding(code=code, quantity=qty, avg_price=price))
    db.commit()
    db.close()
    return {"msg": "持仓已记录"}

async def run_task():
    db = SessionLocal()
    stocks = db.query(StockTrack).all()
    db.close()
    for s in stocks:
        await service.analyze_process(s.code)

@app.post("/analyze/manual")
def manual(bg: BackgroundTasks):
    bg.add_task(run_task)
    return {"msg": "分析任务已启动。采用底层直连技术，彻底规避断连报错。"}

@app.get("/export/csv")
def export():
    db = SessionLocal()
    sub = db.query(DailyAnalysis.code, func.max(DailyAnalysis.date).label('md')).group_by(DailyAnalysis.code).subquery()
    results = db.query(DailyAnalysis).join(sub, (DailyAnalysis.code==sub.c.code) & (DailyAnalysis.date==sub.c.md)).all()
    
    data = []
    for r in results:
        h = db.query(StockHolding).filter(StockHolding.code == r.code).first()
        data.append({
            "代码": r.code, "价格": r.price, "PE": r.pe, "PB": r.pb,
            "股息%": r.dividend_yield, "ROE%": r.roe, "净利增长%": r.profit_growth,
            "波动率%": r.volatility, "综合评分": r.score, "建议": r.suggestion,
            "持仓成本": h.avg_price if h else "--"
        })
    db.close()
    df = pd.DataFrame(data)
    df.to_csv("Pro_Value_Analysis.csv", index=False, encoding="utf_8_sig")
    return FileResponse("Pro_Value_Analysis.csv")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)