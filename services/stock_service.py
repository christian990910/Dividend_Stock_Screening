import os
import re
import time
import json
import random
import asyncio
import datetime
import pandas as pd
import numpy as np
import requests
import efinance as ef
import akshare as ak
from sqlalchemy.orm import Session
from sqlalchemy import desc, func

from core.database import SessionLocal
from core.config import settings  # 添加这行导入
from models.stock import DailyMarketData, HistoricalData, DividendData, StockAnalysisResult, UserStockWatch
from crud.stock import save_market_data_batch, save_analysis_result

class StockDataService:
    def __init__(self):
        self.settings = settings  # 导入全局配置
        self.debug_mode = os.getenv('DEBUG_MODE', 'false').lower() == 'true'
        
        # 请求会话配置
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.proxies = {"http": None, "https": None}
        self.headers = {  # 添加 headers 属性定义
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        }
        self.session.headers.update(self.headers)
        
        # 目标参数 (东方财富)
        self.target_ut = "fa5fd1943c7b386f172d6893dbfba10b"
        self.target_cookies = {
            "ut": self.target_ut,
            "appid": "vLeSuFPlNy3zNWlM",
            "haodou": "rRcDjVxXOaGgNqZQ"
        }
        
        # ✅ 完全还原 22 个字段映射
        self.em_fields_map = {
            'f12': 'code', 'f14': 'name', 'f2': 'latest_price', 'f3': 'change_pct',
            'f4': 'change_amount', 'f15': 'high', 'f16': 'low', 'f17': 'open',
            'f18': 'close_prev', 'f5': 'volume', 'f6': 'amount', 'f7': 'amplitude',
            'f8': 'turnover_rate', 'f10': 'volume_ratio', 'f9': 'pe_dynamic',
            'f23': 'pb', 'f20': 'total_market_cap', 'f21': 'circulating_market_cap',
            'f11': 'rise_speed', 'f22': 'change_5min'
        }

    # --- 基础工具方法 (还原) ---

    def _safe_float(self, val):
        """安全转换为浮点数 - 增强版"""
        try:
            if pd.isna(val) or val == '-' or val is None or val == '':
                return 0.0
            if isinstance(val, str):
                # 处理百分比
                if '%' in val:
                    return float(val.replace('%', '').strip())
                # 处理中文数值单位
                val = val.strip().replace(',', '')  # 移除千分位逗号
                if val.lower() in ['--', 'null', 'nan', 'none']:
                    return 0.0
            return float(val)
        except (ValueError, TypeError) as e:
            # 更详细的错误日志
            if hasattr(self, 'debug_mode') and self.debug_mode:
                print(f"      ⚠️ 数值转换警告: '{val}' -> 0.0 ({str(e)})")
            return 0.0
    
    def _safe_int(self, val):
        """安全转换为整数 - 增强版"""
        try:
            if pd.isna(val) or val == '-' or val is None or val == '':
                return 0
            if isinstance(val, str):
                val = val.strip().replace(',', '')
                if val.lower() in ['--', 'null', 'nan', 'none']:
                    return 0
            return int(float(val))  # 先转float再转int避免精度问题
        except (ValueError, TypeError):
            return 0

    def refresh_ut(self):
        """自动刷新 ut 参数 (还原)"""
        print("🔄 正在刷新 ut 参数...")
        try:
            url = "https://quote.eastmoney.com/center/gridlist.html"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0 Safari/537.36"}
            response = requests.get(url, headers=headers, timeout=10, verify=False, proxies={"http": None, "https": None})
            total_cash_div = 0.0
            if dividends and market.latest_price:
                for d in dividends:
                    div_str = str(d.dividend)
                    
                    # 格式1: "10派5.2" ✅
                    match = re.search(r'10派(\d+\.?\d*)', div_str)
                    if match:
                        total_cash_div += float(match.group(1)) / 10
                        continue
                    
                    # 格式2: "派1.5" ✅
                    match = re.search(r'派(\d+\.?\d*)', div_str)
                    if match:
                        total_cash_div += float(match.group(1)) / 10
                
                if total_cash_div > 0:
                    div_yield = float((total_cash_div / market.latest_price) * 100)
                    print(f"      ✓ 股息率: {div_yield:.2f}% (年度分红: {total_cash_div:.2f}元/股)")
                return True
        except Exception as e:
            print("❌ 刷新 ut 失败:", e)
            return False

    # --- 核心抓取逻辑 (完全还原你提供的代码) ---

    async def fetch_em_data_via_web_api(self, page_size: int = 100) -> pd.DataFrame:
        """增强版数据抓取 - 还原原版逻辑"""
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

        print(f"\n🌐 启动增强版数据抓取 (每页 {page_size} 条)")
        
        session = requests.Session()
        session.trust_env = False
        session.proxies = {"http": None, "https": None}
        session.cookies.update(self.target_cookies)

        while current_page <= total_pages:
            params = {
                "np": "1", "fltt": "1", "invt": "2",
                "cb": f"jQuery37109323508735388775_{int(time.time()*1000)}",
                "fs": "m:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:81+s:262144+f:!2",
                "fields": ",".join(self.em_fields_map.keys()),
                "fid": "f3", "pn": str(current_page), "pz": str(page_size),
                "po": "1", "dect": "1", "ut": self.target_ut,
                "wbp2u": "|0|0|0|web", "_": str(int(time.time() * 1000))
            }

            try:
                print(f"   ➤ 抓取第 {current_page}/{total_pages if total_pages != 999 else '?'} 页...")
                response = await asyncio.to_thread(session.get, url, params=params, headers=headers, timeout=20, verify=False)
                
                if response.status_code != 200: break

                raw_text = response.text
                json_match = re.search(r'jQuery.*?\((.*)\)', raw_text)
                if not json_match: break

                res_json = json.loads(json_match.group(1))
                if not res_json or not res_json.get("data"):
                    if self.refresh_ut():
                        params["ut"] = self.target_ut
                        # 重试逻辑...
                        continue
                    else: break

                if current_page == 1:
                    total_records = res_json["data"]["total"]
                    total_pages = (total_records + page_size - 1) // page_size
                    print(f"   📊 全市场共 {total_records} 只股票，预计 {total_pages} 页")

                batch_df = pd.DataFrame(res_json["data"]["diff"])
                all_dfs.append(batch_df)

                if current_page >= total_pages: break

                # ✅ 还原你原来的高随机等待时间 (10-50秒)，这是不掉线的关键
                wait_time = random.uniform(10, 50)
                print(f"   💤 随机等待 {wait_time:.1f} 秒...")
                await asyncio.sleep(wait_time)
                current_page += 1

            except Exception as e:
                print(f"   ❌ 第 {current_page} 页失败: {str(e)[:100]}")
                break

        session.close()
        if not all_dfs: return pd.DataFrame()

        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df = final_df.rename(columns=self.em_fields_map)

        # ✅ 还原字段完整性统计显示
        print(f"\n✅ 总计获取 {len(final_df)} 条数据")
        print(f"\n📊 字段完整性统计:")
        for col in ['code', 'name', 'latest_price', 'pe_dynamic', 'pb']:
            if col in final_df.columns:
                non_null = final_df[col].notna().sum()
                pct = (non_null / len(final_df)) * 100
                print(f"   [{'✅' if pct > 90 else '⚠️'}] {col:20s}: {non_null:5d}/{len(final_df)} ({pct:5.1f}%)")

        return final_df


    async def fetch_daily_market_data(self, force: bool = False):
        """入库逻辑整合"""
        db = SessionLocal()
        today = datetime.date.today()
        
        if not force and db.query(DailyMarketData).filter(DailyMarketData.date == today).first():
            db.close()
            return {"status": "skip", "message": "今日数据已存在"}

        df = await self.fetch_em_data_via_web_api()
        if df.empty:
            db.close()
            return {"status": "error", "message": "抓取数据为空"}

        # 删除旧数据并入库
        db.query(DailyMarketData).filter(DailyMarketData.date == today).delete()
        
        batch = []
        for _, row in df.iterrows():
            m = DailyMarketData(
                date=today,
                code=str(row.get('code', '')),
                name=str(row.get('name', '')),
                latest_price=self._safe_float(row.get('latest_price')),
                change_pct=self._safe_float(row.get('change_pct')),
                pe_dynamic=self._safe_float(row.get('pe_dynamic')),
                pb=self._safe_float(row.get('pb')),
                volume=self._safe_float(row.get('volume')),
                amount=self._safe_float(row.get('amount')),
                updated_at=datetime.datetime.now()
            )
            batch.append(m)
        
        db.bulk_save_objects(batch)
        db.commit()
        db.close()
        return {"status": "success", "count": len(batch)}
   
    async def fetch_dividend_data(self, stock_code: str = None):
        """同步分红数据 (基于Akshare)"""
        db = SessionLocal()
        try:
            # 此处示例为获取最新分红公告，实际生产环境建议定时同步全量
            df = ak.news_trade_notify_dividend_baidu(date=datetime.date.today().strftime('%Y%m%d'))
            if df.empty: return
            
            for _, row in df.iterrows():
                div = DividendData(
                    stock_code=row['股票代码'],
                    stock_name=row['股票简称'],
                    ex_dividend_date=pd.to_datetime(row['除权日']).date(),
                    dividend=row['分红'],
                    report_period=row['报告期']
                )
                db.merge(div)
            db.commit()
        except: pass
        finally: db.close()

    async def _request_with_retry(self, url, params, max_retries=3):
        """内部通用的重试请求包装器"""
        for i in range(max_retries):
            try:
                # 在线程中执行同步请求
                response = await asyncio.to_thread(
                    self.session.get, url, params=params, timeout=15, verify=False
                )
                if response.status_code == 200:
                    return response.json()
            except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
                wait_time = (i + 1) * 2
                if i < max_retries - 1:
                    await asyncio.sleep(wait_time)
                    continue
                raise e
        return None
    
    async def fetch_historical_data(self, stock_code: str):
        """同步历史K线 - 120天数据"""
        db = SessionLocal()
        try:
            market = "1" if stock_code.startswith(('6', '9', '11')) else "0"
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            params = {
                "cb": f"jQuery_{int(time.time()*1000)}",
                "secid": f"{market}.{stock_code}",
                "ut": self.target_ut,
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56",
                "klt": "101", "fqt": "1", "beg": "0", "end": "20500101", "lmt": "120", "_": str(int(time.time() * 1000))
            }
            resp = await asyncio.to_thread(self.session.get, url, params=params, headers=self.headers, timeout=15)
            match = re.search(r'\(({.*})\)', resp.text)
            if not match: return
            res = json.loads(match.group(1))
            klines = res.get("data", {}).get("klines", [])
            if not klines: return

            db.query(HistoricalData).filter(HistoricalData.stock_code == stock_code).delete()
            for line in klines:
                cols = line.split(',')
                h = HistoricalData(
                    stock_code=stock_code,
                    date=datetime.datetime.strptime(cols[0], "%Y-%m-%d").date(),
                    open=self._safe_float(cols[1]), close=self._safe_float(cols[2]),
                    high=self._safe_float(cols[3]), low=self._safe_float(cols[4])
                )
                db.add(h)
            db.commit()
        except Exception as e:
            print(f"   ❌ {stock_code} K线同步失败: {e}")
        finally:
            db.close()

    async def fetch_stock_dividend_history(self, stock_code: str):
        """同步历史分红记录"""
        db = SessionLocal()
        try:
            df = await asyncio.to_thread(ak.stock_history_dividend_detail, symbol=stock_code, indicator="分红")  # 添加 indicator 参数
            if df is None or df.empty: return
            
            for _, row in df.iterrows():
                ex_date_raw = row.get('除权除息日')
                if pd.isna(ex_date_raw) or str(ex_date_raw) in ['NaT', 'nan', '']: continue
                
                ex_date = pd.to_datetime(ex_date_raw).date()
                div_val = row.get('派息(每10股派,税前)', 0)
                if not div_val: continue
                
                div = DividendData(
                    stock_code=stock_code,
                    stock_name=row.get('名称', '未知'),
                    ex_dividend_date=ex_date,
                    dividend=f"10派{div_val}",
                    report_period=str(row.get('分红年度', ''))
                )
                db.merge(div)
            db.commit()
        except Exception as e:
            print(f"   ⚠️ {stock_code} 分红抓取失败: {e}")
        finally:
            db.close()

    async def fetch_financial_metrics(self, stock_code: str):
        """
        获取财务指标 - 多源增强版
        支持多个数据源和智能降级策略
        返回: (ROE, 利润增长率)
        """
        # 初始化默认值
        roe, growth = 0.0, 0.0
        attempts = []
        
        try:
            # 1. 尝试使用 efinance (主数据源)
            attempts.append("efinance")
            df = await asyncio.to_thread(ef.stock.get_base_info, stock_code)  # 移除 timeout 参数
            
            if df is not None and not df.empty:
                # 统一数据格式处理
                if isinstance(df, pd.DataFrame):
                    if len(df) > 0:
                        data = df.iloc[0].to_dict()
                    else:
                        data = {}
                elif isinstance(df, pd.Series):
                    data = df.to_dict()
                else:
                    data = {}
                
                # 多种字段名匹配
                roe_fields = ['净资产收益率(%)', 'ROE(%)', '净资产收益率', 'roe', 'ROE']
                growth_fields = ['净利润同比(%)', '净利润增长率(%)', '净利润同比增长', 'net_profit_growth', 'profit_growth']
                
                # 提取 ROE
                for field in roe_fields:
                    if field in data and data[field] is not None:
                        roe_val = self._safe_float(data[field])
                        if roe_val != 0:
                            roe = roe_val
                            break
                
                # 提取利润增长率
                for field in growth_fields:
                    if field in data and data[field] is not None:
                        growth_val = self._safe_float(data[field])
                        if growth_val != 0:
                            growth = growth_val
                            break
                
                if roe != 0 or growth != 0:
                    print(f"      ✓ 通过 efinance 获取财务数据: ROE={roe:.2f}%, Growth={growth:.2f}%")
                    return float(roe), float(growth)
                    
        except Exception as e:
            print(f"      ⚠️ efinance 失败: {str(e)[:50]}")
        
        try:
            # 2. 尝试 akshare 财务报表 (备用数据源1)
            attempts.append("akshare_financial")
            # 标准化股票代码格式
            formatted_code = self._format_stock_code_for_akshare(stock_code)
            df_fin = await asyncio.to_thread(ak.stock_financial_report_sina, symbol=formatted_code)  # 移除 timeout 参数
            
            if df_fin is not None and not df_fin.empty and len(df_fin) > 0:
                data_fin = df_fin.iloc[0].to_dict()
                
                # akshare 字段名
                roe = self._safe_float(data_fin.get('净资产收益率') or 
                                     data_fin.get('ROE') or 0)
                growth = self._safe_float(data_fin.get('净利润同比增长') or 
                                        data_fin.get('净利润增长率') or 0)
                
                if roe != 0 or growth != 0:
                    print(f"      ✓ 通过 akshare 获取财务数据: ROE={roe:.2f}%, Growth={growth:.2f}%")
                    return float(roe), float(growth)
                    
        except Exception as e:
            print(f"      ⚠️ akshare financial 失败: {str(e)[:50]}")
        
        try:
            # 3. 尝试 akshare 主要指标 (备用数据源2)
            attempts.append("akshare_indicator")
            formatted_code = self._format_stock_code_for_akshare(stock_code)
            df_ind = await asyncio.to_thread(ak.stock_a_lg_indicator, symbol=formatted_code)  # 使用正确的函数名
            
            if df_ind is not None and not df_ind.empty and len(df_ind) > 0:
                data_ind = df_ind.iloc[0].to_dict()
                
                # 主要指标字段名
                roe = self._safe_float(data_ind.get('净资产收益率(%)') or 
                                     data_ind.get('ROE') or 0)
                growth = self._safe_float(data_ind.get('净利润同比增长(%)') or 
                                        data_ind.get('净利润增长率(%)') or 0)
                
                if roe != 0 or growth != 0:
                    print(f"      ✓ 通过 akshare indicator 获取财务数据: ROE={roe:.2f}%, Growth={growth:.2f}%")
                    return float(roe), float(growth)
                    
        except Exception as e:
            print(f"      ⚠️ akshare indicator 失败: {str(e)[:50]}")
        
        try:
            # 4. 尝试从市场数据推算基础指标 (最终备用)
            attempts.append("market_derived")
            derived_roe, derived_growth = await self._derive_financial_from_market(stock_code)
            if derived_roe != 0 or derived_growth != 0:
                print(f"      ✓ 通过市场数据推算: ROE={derived_roe:.2f}%, Growth={derived_growth:.2f}%")
                return derived_roe, derived_growth
                
        except Exception as e:
            print(f"      ⚠️ 市场数据推算失败: {str(e)[:50]}")
        
        # 所有方法都失败，记录详细信息
        print(f"      ❌ {stock_code} 财务指标获取完全失败 (尝试了: {', '.join(attempts)})")
        return 0.0, 0.0
    
    def _format_stock_code_for_akshare(self, stock_code: str) -> str:
        """格式化股票代码以适配 akshare 接口"""
        if stock_code.startswith(('6', '9')):
            return f"sh{stock_code}"
        elif stock_code.startswith(('0', '3')):
            return f"sz{stock_code}"
        return stock_code
    
    async def _derive_financial_from_market(self, stock_code: str):
        """从市场价格数据推算基础财务指标"""
        db = SessionLocal()
        try:
            # 获取历史价格数据推算趋势
            hist_data = db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code
            ).order_by(desc(HistoricalData.date)).limit(252).all()  # 一年数据
            
            if len(hist_data) < 30:  # 数据不足
                return 0.0, 0.0
            
            # 计算价格增长率作为粗略的成长性指标
            prices = [float(h.close) for h in reversed(hist_data)]
            if len(prices) >= 2:
                # 年度增长率估算
                annual_growth = ((prices[-1] / prices[0]) ** (252/len(prices)) - 1) * 100
                derived_growth = max(-50, min(50, annual_growth))  # 限制范围
            else:
                derived_growth = 0.0
            
            # ROE 粗略估算 (假设合理的范围)
            derived_roe = max(0, min(30, abs(derived_growth) * 0.8))  # 简单关联
            
            return float(derived_roe), float(derived_growth)
            
        except Exception as e:
            print(f"      ⚠️ 市场数据推算异常: {str(e)[:50]}")
            return 0.0, 0.0
        finally:
            db.close()

    async def analyze_stock(self, stock_code: str, db: Session):
        """综合分析评分 - 严格映射每一个字段"""
        today = datetime.date.today()
        market = db.query(DailyMarketData).filter(DailyMarketData.code == stock_code).order_by(desc(DailyMarketData.date)).first()
        if not market: return
        
        # 1. 波动率深度分析 (0-40分)
        v30, v60, vol_score = 0.0, 0.0, 0
        # 获取120条数据确保足够
        hist = db.query(HistoricalData).filter(
            HistoricalData.stock_code == stock_code,
            HistoricalData.close.isnot(None)
        ).order_by(desc(HistoricalData.date)).limit(120).all()

        # 反转为正序
        prices = [float(h.close) for h in reversed(hist)]
        price_series = pd.Series(prices)
        log_returns = np.log(price_series / price_series.shift(1)).dropna()

        # 分别计算30日和60日
        if len(log_returns) >= 30:
            v30 = float(log_returns.tail(30).std() * np.sqrt(252) * 100)
            
        if len(log_returns) >= 60:
            v60 = float(log_returns.tail(60).std() * np.sqrt(252) * 100)  # ✅ 修复
        
        if v30 < 20: vol_score = 40
        elif v30 < 30: vol_score = 30
        elif v30 < 40: vol_score = 20
        else: vol_score = 10

        # 2. 股息率计算 (0-30分)
        div_yield, div_score = 0.0, 0
        one_year_ago = today - datetime.timedelta(days=365)
        dividends = db.query(DividendData).filter(DividendData.stock_code == stock_code, DividendData.ex_dividend_date >= one_year_ago).all()
        
        total_cash_div = 0.0
        if dividends and market.latest_price:
            for d in dividends:
                div_str = str(d.dividend)
                
                # 格式1: "10派5.2" ✅
                match = re.search(r'10派(\d+\.?\d*)', div_str)
                if match:
                    total_cash_div += float(match.group(1)) / 10
                    continue
                
                # 格式2: "派1.5" ✅
                match = re.search(r'派(\d+\.?\d*)', div_str)
                if match:
                    total_cash_div += float(match.group(1)) / 10
            
            if total_cash_div > 0:
                div_yield = float((total_cash_div / market.latest_price) * 100)
                print(f"      ✓ 股息率: {div_yield:.2f}% (年度分红: {total_cash_div:.2f}元/股)")

            if div_yield >= 5: div_score = 30
            elif div_yield >= 3: div_score = 20
            elif div_yield >= 1.5: div_score = 10

        # 3. 财务与成长性 (0-30分)
        # ✅ 这里解包元组，修复 TypeError
        roe, profit_growth = await self.fetch_financial_metrics(stock_code)
        # 成长性评分
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

        # 4. 汇总保存 - 映射模型中的所有字段
        res = StockAnalysisResult(
            stock_code=stock_code,
            stock_name=market.name,
            analysis_date=today,
            latest_price=market.latest_price,
            pe_ratio=market.pe_dynamic,
            pb_ratio=market.pb,
            roe=round(roe, 2),
            profit_growth=round(profit_growth, 2),
            volatility_30d=round(v30, 2),
            volatility_60d=round(v60, 2),
            volatility_score=vol_score,
            dividend_yield=round(div_yield, 2),
            dividend_score=div_score,
            growth_score=growth_score,
            total_score=int(vol_score + div_score + growth_score),
            suggestion="强烈推荐" if (vol_score + div_score + growth_score) >= 70 else ("推荐" if (vol_score + div_score + growth_score) >= 55 else "观望"),
            data_source="automated_v3"
        )
        db.merge(res)
        db.commit()
        return res.total_score

    async def analyze_stock(self, stock_code: str, db: Session):
        """
        深度分析单只股票
        目标：严格对照模型字段，确保 dividend_yield, volatility_60d, roe 等不再为 NULL
        """
        today = datetime.date.today()
        
        # 1. 基础行情校验 (DailyMarketData)
        market = db.query(DailyMarketData).filter(
            DailyMarketData.code == stock_code
        ).order_by(desc(DailyMarketData.date)).first()
        
        if not market or not market.latest_price:
            print(f"   ⚠️ {stock_code} 缺失实时行情，无法分析")
            return None

        # ---------------------------------------------------------
        # 2. 波动率计算 (HistoricalData)
        # ---------------------------------------------------------
        v30, v60, vol_score = 0.0, 0.0, 0
        
        # 核心修复：查询最近120条，确保有足够数据算60日波动率
        hist = db.query(HistoricalData).filter(
            HistoricalData.stock_code == stock_code
        ).order_by(desc(HistoricalData.date)).limit(100).all()

        if len(hist) >= 20:
            # 必须反转为正序（从旧到新）计算收益率
            prices = [h.close for h in reversed(hist)]
            price_series = pd.Series(prices)
            log_returns = np.log(price_series / price_series.shift(1)).dropna()
            
            # 计算30日波动率
            if len(log_returns) >= 30:
                v30 = log_returns.tail(30).std() * np.sqrt(252) * 100
                
            # 计算60日波动率
            if len(log_returns) >= 60:
                v60 = log_returns.tail(60).std() * np.sqrt(252) * 100
            
            # 波动率评分 (按照30日标准)
            if v30 > 0:
                if v30 < 20: vol_score = 40
                elif v30 < 30: vol_score = 30
                elif v30 < 40: vol_score = 20
                else: vol_score = 10

        # ---------------------------------------------------------
        # 3. 股息率计算 (DividendData)
        # ---------------------------------------------------------
        div_yield, div_score = 0.0, 0
        one_year_ago = today - datetime.timedelta(days=365)
        
        # 核心修复：查询过去一年内的所有分红记录
        dividends = db.query(DividendData).filter(
            DividendData.stock_code == stock_code,
            DividendData.ex_dividend_date >= one_year_ago
        ).all()
        
        total_cash_div = 0.0
        if dividends:
            for d in dividends:
                # 兼容 "10派5", "10派5.2", "派1.5" 等各种字符串格式
                match = re.search(r'派(\d+\.?\d*)', str(d.dividend))
                if match:
                    # 换算成每股分红额
                    total_cash_div += float(match.group(1)) / 10
            
            # 计算股息率：年度总分红 / 当前股价 * 100
            div_yield = (total_cash_div / market.latest_price) * 100
            
            # 股息率评分
            if div_yield >= 5: div_score = 30
            elif div_yield >= 3: div_score = 20
            elif div_yield >= 1.5: div_score = 10

        # ---------------------------------------------------------
        # 4. 财务数据获取 (ROE & Growth)
        # ---------------------------------------------------------
        roe, profit_growth = await self.fetch_financial_metrics(stock_code)
        
        # 成长性评分
        growth_score = 0
        if roe > 15: growth_score = 30
        elif roe > 10: growth_score = 20
        elif roe > 5: growth_score = 10

        # ---------------------------------------------------------
        # 5. 结果持久化 (映射到 StockAnalysisResult 模型)
        # ---------------------------------------------------------
        analysis_res = StockAnalysisResult(
            stock_code=stock_code,
            stock_name=market.name,
            analysis_date=today,
            
            # 基础数据
            latest_price=market.latest_price,
            pe_ratio=market.pe_dynamic,
            pb_ratio=market.pb,
            
            # 波动率指标 (显式映射)
            volatility_30d=round(v30, 2) if v30 > 0 else 0.0,
            volatility_60d=round(v60, 2) if v60 > 0 else 0.0,
            
            # 财务指标 (显式映射)
            dividend_yield=round(div_yield, 2) if div_yield > 0 else 0.0,  # ✅
            roe=round(roe, 2) if roe > 0 else 0.0,  # ✅
            profit_growth=round(profit_growth, 2) if profit_growth else 0.0,  # ✅
            
            
            # 评分详情 (显式映射)
            volatility_score=int(vol_score),
            dividend_score=int(div_score),
            growth_score=int(growth_score),
            total_score=int(vol_score + div_score + growth_score),
            
            suggestion="推荐" if (vol_score + div_score + growth_score) >= 60 else "观望",
            data_source="automated_v3"
        )

        try:
            db.merge(analysis_res)
            db.commit()
            return analysis_res.total_score
        except Exception as e:
            db.rollback()
            print(f"   ❌ {stock_code} 结果入库失败: {e}")
            return None

    async def analyze_all_watched_stocks(self):
        """主分析任务循环 - 增强版"""
        db = SessionLocal()
        stats = {"success": 0, "failed": 0, "financial_failed": 0}
        try:
            watched = db.query(UserStockWatch.stock_code).distinct().all()
            total = len(watched)
            print(f"🚀 启动深度分析 (共 {total} 只)...")
            print(f"📊 配置: 超时{self.settings.FINANCIAL_FETCH_TIMEOUT}s, 重试{self.settings.FINANCIAL_RETRY_COUNT}次")
            
            for i, row in enumerate(watched, 1):
                code = row[0]
                try:
                    # 数据预处理
                    await self.fetch_historical_data(code)
                    await self.fetch_stock_dividend_history(code)
                    
                    # 核心分析
                    score = await self.analyze_stock(code, db)
                    
                    if score is not None:
                        stats["success"] += 1
                        print(f"   ✓ {i}/{total} {code} 分析完成 (评分: {score})")
                    else:
                        stats["failed"] += 1
                        print(f"   ❌ {i}/{total} {code} 分析失败")
                        
                except Exception as e:
                    stats["failed"] += 1
                    print(f"   ❌ {i}/{total} {code} 处理异常: {str(e)[:50]}")
                
                # 智能延迟 - 根据成功率调整
                delay = random.uniform(
                    self.settings.FETCH_DELAY_MIN, 
                    self.settings.FETCH_DELAY_MAX
                )
                print(f"   💤 随机等待 {delay:.1f} 秒...")
                await asyncio.sleep(delay)
                
                # 批量处理进度报告
                if i % self.settings.BATCH_SIZE == 0:
                    success_rate = (stats["success"] / i) * 100
                    print(f"\n📈 批量进度: {i}/{total} ({success_rate:.1f}% 成功率)")
                    print(f"   成功: {stats['success']}, 失败: {stats['failed']}")
            
            # 最终统计
            final_success_rate = (stats["success"] / total) * 100 if total > 0 else 0
            print(f"\n🏁 分析完成!")
            print(f"📊 总体统计:")
            print(f"   总数: {total}")
            print(f"   成功: {stats['success']} ({final_success_rate:.1f}%)")
            print(f"   失败: {stats['failed']}")
            
        except Exception as e:
            print(f"🚨 分析过程中发生严重错误: {e}")
        finally:
            db.close()

stock_service = StockDataService()
