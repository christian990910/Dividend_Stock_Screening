import os
import time
import datetime
import asyncio
import re
import pandas as pd
import numpy as np
import akshare as ak
from typing import List, Optional

from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, desc, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler

# --- 1. 数据库配置 ---
SQLALCHEMY_DATABASE_URL = "sqlite:///./stock_pro_system.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- 2. 数据库模型 ---
class StockTrack(Base):
    """自选股监控名单"""
    __tablename__ = "stock_track"
    code = Column(String, primary_key=True)
    name = Column(String)

class StockHolding(Base):
    """实盘持仓表"""
    __tablename__ = "stock_holdings"
    code = Column(String, primary_key=True)
    quantity = Column(Integer, default=0)
    avg_price = Column(Float, default=0.0)

class StockHistory(Base):
    """股票基础数据缓存表 (用于本地分析)"""
    __tablename__ = "stock_history"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, index=True)
    date = Column(DateTime, index=True)
    close = Column(Float)
    pe = Column(Float)
    pb = Column(Float)
    dividend_yield = Column(Float)

class DailyAnalysis(Base):
    """每日深度分析结果结果表"""
    __tablename__ = "daily_analysis"
    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String, index=True)
    date = Column(DateTime, default=datetime.datetime.now)
    price = Column(Float)
    volatility = Column(Float)  # 年化波动率
    pe_percentile = Column(Float) # PE在历史中的百分位
    pb_percentile = Column(Float) # PB在历史中的百分位
    dividend_yield = Column(Float)
    score = Column(Integer)
    suggestion = Column(String)

Base.metadata.create_all(bind=engine)

# --- 3. 核心工具类 ---
class StockManager:
    @staticmethod
    def get_last_trading_date():
        now = datetime.datetime.now()
        target = now if now.hour >= 16 else now - datetime.timedelta(days=1)
        while target.weekday() >= 5:
            target -= datetime.timedelta(days=1)
        return target.date()

    @staticmethod
    def calculate_indicators(df: pd.DataFrame):
        """基于历史数据 DataFrame 计算关键指标"""
        if df.empty or len(df) < 10:
            return None
        
        # --- 核心修复：统一列名映射 ---
        # 兼容乐咕(dv_ratio)和东财(dividend_yield)的差异
        rename_map = {'dv_ratio': 'dividend_yield', 'trade_date': 'date'}
        df = df.rename(columns=rename_map)
        
        latest = df.iloc[-1]
        
        # 再次检查关键列是否存在
        required_cols = ['close', 'pe', 'pb', 'dividend_yield']
        for col in required_cols:
            if col not in df.columns:
                # 如果缺少列，补零防止崩溃
                df[col] = 0.0
        
        latest = df.iloc[-1]

        # 1. 计算波动率 (120日)
        hist_120 = df.tail(120).copy()
        log_ret = np.log(hist_120['close'] / hist_120['close'].shift(1))
        volatility = log_ret.std() * np.sqrt(252) * 100
        
        # 2. 计算 PE/PB 百分位
        pe_pct = (df['pe'] < latest['pe']).mean() * 100 if latest['pe'] > 0 else 0
        pb_pct = (df['pb'] < latest['pb']).mean() * 100 if latest['pb'] > 0 else 0
        
        # 3. 评分逻辑
        score = 0
        if latest['dividend_yield'] > 3.5: score += 30
        if 0 < pe_pct < 20: score += 20
        if 0 < pb_pct < 20: score += 20
        if 0 < volatility < 25: score += 30
        
        return {
            "price": float(latest['close']),
            "volatility": round(float(volatility), 2) if not np.isnan(volatility) else 0,
            "pe_pct": round(float(pe_pct), 2),
            "pb_pct": round(float(pb_pct), 2),
            "dividend": round(float(latest['dividend_yield']), 2),
            "score": int(score)
        }

# --- 4. 业务逻辑服务 (统一函数名版) ---
class AnalysisService:
    def __init__(self):
        self.mgr = StockManager()

    async def update_stock_data(self, db, code: str):
        """联网抓取历史数据 (乐咕优先，东财保底)"""
        print(f"正在联网更新 {code} 的历史数据...")
        df = None
        try:
            # 方案 A & B: 尝试乐咕接口
            for func_name in ["stock_a_indicator_lg", "stock_a_lg_indicator"]:
                if hasattr(ak, func_name):
                    try:
                        df = getattr(ak, func_name)(symbol=code)
                        if df is not None and not df.empty:
                            # 统一乐咕的列名到项目标准
                            df = df.rename(columns={'dv_ratio': 'dividend_yield'})
                            break
                    except:
                        continue
            
            # 方案 C: 东财保底
            if df is None or df.empty:
                print(f"警告: 乐咕接口失效，正在尝试东财历史接口保底...")
                df_em = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")
                if not df_em.empty:
                    df = pd.DataFrame()
                    df['trade_date'] = df_em['日期']
                    df['close'] = df_em['收盘']
                    df['pe'] = 0.0
                    df['pb'] = 0.0
                    df['dividend_yield'] = 0.0 # 修改这里：统一为 dividend_yield
            
            if df is None or df.empty:
                print(f"错误: 无法获取 {code} 的任何历史数据")
                return None
            
            # 存入数据库
            db.query(StockHistory).filter(StockHistory.code == code).delete()
            objs = []
            for _, row in df.iterrows():
                objs.append(StockHistory(
                    code=code,
                    date=pd.to_datetime(row['trade_date']),
                    close=float(row['close']),
                    pe=float(row.get('pe', 0)),
                    pb=float(row.get('pb', 0)),
                    dividend_yield=float(row.get('dividend_yield', 0)) # 修改这里
                ))
            db.bulk_save_objects(objs)
            db.commit()
            print(f"[{code}] 数据更新成功: 抓取到 {len(objs)} 条记录")
            return df

        except Exception as e:
            print(f"更新异常 {code}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    async def run_single_analysis(self, code: str):
        """单只股票分析"""
        db = SessionLocal()
        try:
            last_trade_date = self.mgr.get_last_trading_date()
            latest_record = db.query(StockHistory).filter(StockHistory.code == code).order_by(StockHistory.date.desc()).first()
            
            if not latest_record or latest_record.date.date() < last_trade_date:
                await asyncio.sleep(30)
                df_hist = await self.update_stock_data(db, code)
            else:
                print(f"本地数据已是最新: {code}")
                rows = db.query(StockHistory).filter(StockHistory.code == code).order_by(StockHistory.date.asc()).all()
                # 转换回 DataFrame 时确保列名一致
                df_hist = pd.DataFrame([{
                    'close': r.close, 
                    'pe': r.pe, 
                    'pb': r.pb, 
                    'dividend_yield': r.dividend_yield
                } for r in rows])

            if df_hist is None or df_hist.empty:
                return

            res = self.mgr.calculate_indicators(df_hist)
            if not res: return

            # 存储 DailyAnalysis 结果
            holding = db.query(StockHolding).filter(StockHolding.code == code).first()
            sug = "观望"
            if res['score'] >= 75:
                sug = "加仓" if holding and res['price'] < holding.avg_price else "建议建仓"

            analysis = DailyAnalysis(
                code=code,
                price=res['price'],
                volatility=res['volatility'],
                pe_percentile=res['pe_pct'],
                pb_percentile=res['pb_pct'],
                dividend_yield=res['dividend'],
                score=res['score'],
                suggestion=sug
            )
            db.add(analysis)
            db.commit()
            print(f"[{code}] 分析决策完成: {sug}")
        except Exception as e:
            print(f"分析流程出错 {code}: {e}")
        finally:
            db.close()

# --- 外部调用函数 ---
async def start_full_analysis():
    """遍历自选列表启动分析"""
    db = SessionLocal()
    stocks = db.query(StockTrack).all()
    db.close()
    
    total = len(stocks)
    print(f"开始批量分析任务，共 {total} 只股票...")
    
    for s in stocks:
        await service.run_single_analysis(s.code)
    
    print("所有分析任务已完成。")
# --- 5. FastAPI 接口设计 ---
app = FastAPI(title="智能策略系统 v3.0")
service = AnalysisService()

@app.post("/stocks/bulk")
def bulk_add_stocks(codes: str):
    """批量添加股票代码 (逗号或空格分隔)"""
    db = SessionLocal()
    code_list = list(set(re.findall(r'\d{6}', codes)))
    all_info = ak.stock_info_a_code_name()
    name_map = dict(zip(all_info['code'], all_info['name']))
    
    for c in code_list:
        if c in name_map:
            db.merge(StockTrack(code=c, name=name_map[c]))
    db.commit()
    db.close()
    return {"msg": f"监控列表已更新，当前共 {len(code_list)} 只股票"}

@app.post("/holdings/")
def set_holding(code: str, qty: int, price: float):
    """录入实盘持仓记录"""
    db = SessionLocal()
    db.merge(StockHolding(code=code, quantity=qty, avg_price=price))
    db.commit()
    db.close()
    return {"msg": "持仓信息已保存"}

async def start_full_analysis():
    db = SessionLocal()
    stocks = db.query(StockTrack).all()
    db.close()
    print(f"开始批量分析任务，预计耗时 {len(stocks)*30/60:.1f} 分钟...")
    for s in stocks:
        await service.run_single_analysis(s.code)

@app.post("/analyze/manual")
def manual_analysis(background_tasks: BackgroundTasks):
    background_tasks.add_task(start_full_analysis)
    return {"msg": "后台分析已启动，系统会自动对比本地数据，如需联网更新会自动限速。"}

@app.get("/export/csv")
def export_csv():
    """导出最新的分析报告"""
    db = SessionLocal()
    # 每个代码取最新的一条分析记录
    subquery = db.query(DailyAnalysis.code, func.max(DailyAnalysis.date).label('max_date')).group_by(DailyAnalysis.code).subquery()
    results = db.query(DailyAnalysis).join(subquery, (DailyAnalysis.code == subquery.c.code) & (DailyAnalysis.date == subquery.c.max_date)).all()
    
    data = []
    for r in results:
        track = db.query(StockTrack).filter(StockTrack.code == r.code).first()
        hold = db.query(StockHolding).filter(StockHolding.code == r.code).first()
        data.append({
            "日期": r.date.strftime("%Y-%m-%d"),
            "代码": r.code,
            "名称": track.name if track else "未知",
            "价格": r.price,
            "持仓成本": hold.avg_price if hold else "--",
            "股息%": r.dividend_yield,
            "PE百分位%": r.pe_percentile,
            "波动率%": r.volatility,
            "综合得分": r.score,
            "策略建议": r.suggestion
        })
    db.close()
    
    if not data: return {"msg": "无分析数据"}
    df = pd.DataFrame(data)
    filename = f"Analysis_Report_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(filename, index=False, encoding="utf_8_sig")
    return FileResponse(filename, filename=filename)

# --- 6. 定时任务配置 ---
scheduler = BackgroundScheduler()
# 每天凌晨 1 点执行全量分析
scheduler.add_job(lambda: asyncio.run(start_full_analysis()), 'cron', hour=1, minute=0)
scheduler.start()

if __name__ == "__main__":
    import uvicorn
    # 强制不使用系统代理防止报错 (如果需要)
    os.environ['no_proxy'] = '*'
    uvicorn.run(app, host="0.0.0.0", port=8000)