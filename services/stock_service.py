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
from core.config import settings  # 确保这行存在
from models.stock import DailyMarketData, HistoricalData, DividendData, StockAnalysisResult, UserStockWatch
from models.holdings import UserStockHolding  # 添加这行导入
from crud.stock import save_market_data_batch, save_analysis_result

class StockDataService:
    def __init__(self):
        self.settings = settings
        self.debug_mode = os.getenv('DEBUG_MODE', 'false').lower() == 'true'
        
        # 添加缓存层
        self.financial_cache = {}  # 财务数据缓存
        self.cache_expiry = {}     # 缓存过期时间
        self.CACHE_TTL = 3600      # 缓存有效期1小时
        
        # 请求会话配置
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.proxies = {"http": None, "https": None}
        self.headers = {
            # 使用你URL中暗示的移动设备User-Agent
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://quote.eastmoney.com/center/gridlist.html",
            "X-Requested-With": "XMLHttpRequest"  # AJAX请求标识
        }
        self.session.headers.update(self.headers)
        
        # 目标参数 (东方财富) - 使用你提供的参数
        self.target_ut = "bd1d9ddb04089700cf9c27f6f7426281"  # 你提供的ut值
        self.target_cookies = {
            "ut": self.target_ut,
            # 可以添加更多cookie如果需要
        }
        
        # API字段映射 - 匹配你提供的fields参数
        self.em_fields_map = {
            "f12": "code", "f14": "name", "f2": "latest_price", 
            "f3": "change_pct", "f4": "change_amount", "f15": "high",
            "f16": "low", "f17": "open", "f18": "prev_close",
            "f5": "volume", "f6": "amount", "f20": "pe_dynamic",
            "f23": "pb", "f115": "market_cap", "f116": "circulating_market_cap"
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
        
            result = float(val)
            
            # 添加异常值检查
            if result > 1000000:  # 超过100万的PE值视为异常
                print(f"      ⚠️ 检测到异常PE值: {result}, 已修正为0")
                return 0.0
            if result < 0:  # 负PE值处理
                return 0.0
                
            return result
        except (ValueError, TypeError) as e:
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
        """增强版数据抓取 - 完全匹配你提供的API格式"""
        all_dfs = []
        current_page = 1
        total_pages = 999
        url = "https://push2.eastmoney.com/api/qt/clist/get"

        headers = {
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
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
            # 完全匹配你提供的参数格式
            params = {
                "cb": f"jQuery341015241163678647807_{int(time.time()*1000)}",
                "pn": str(current_page),
                "np": "1",
                "ut": self.target_ut,
                "fltt": "2",
                "invt": "2",
                "fs": "m:0+t:6+f:!2,m:0+t:13+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:81+s:2048",
                "fields": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f19,f20,f21,f23,f24,f25,f22,f11,f62,f111,f128,f136,f115,f148,f152",
                "wbp2u": "|0|0|0|wap",
                "fid": "f3",
                "po": "1",
                "pz": str(page_size),
                "_": str(int(time.time() * 1000))
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
        """增强版重试请求包装器"""
        for i in range(max_retries):
            try:
                # 在线程中执行同步请求
                response = await asyncio.to_thread(
                    self.session.get, url, params=params, timeout=15, verify=False
                )
                if response.status_code == 200:
                    return response.json()
            except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError, requests.exceptions.RemoteDisconnected) as e:
                wait_time = (i + 1) * 3  # 增加等待时间
                if i < max_retries - 1:
                    print(f"      ⚠️ 网络连接失败，{wait_time}秒后重试... ({i+1}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    continue
                raise e
            except requests.exceptions.Timeout as e:
                if i < max_retries - 1:
                    print(f"      ⚠️ 请求超时，重试中... ({i+1}/{max_retries})")
                    continue
                raise e
        return None
    
    async def fetch_historical_data(self, stock_code: str):
        """同步历史K线 - 优化版"""
        # 首先检查本地数据
        db = SessionLocal()
        try:
            existing_count = db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code
            ).count()
            
            # 优化：如果已有足够数据（比如100条以上），就不重复获取
            if existing_count >= 100:
                print(f"      ℹ️ 已有{existing_count}条K线数据，跳过获取")
                return True
        finally:
            db.close()
        
        # 设置完整的请求头
        headers = {
            "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 18_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Mobile/15E148 Safari/604.1",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://quote.eastmoney.com/center/gridlist.html",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        try:
            market = "1" if stock_code.startswith(('6', '9', '11')) else "0"
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            params = {
                "cb": f"jQuery_{int(time.time()*1000)}",
                "secid": f"{market}.{stock_code}",
                "ut": self.target_ut,
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56",
                "klt": "101", "fqt": "1", "beg": "0", "end": "20500101", 
                "lmt": "120", "_": str(int(time.time() * 1000))  # 限制获取120条数据
            }
            
            # 使用带有完整请求头的会话
            response = await asyncio.to_thread(
                self.session.get, url, params=params,
                headers=headers, timeout=20, verify=False
            )
            
            if response.status_code == 200:
                match = re.search(r'\(({.*})\)', response.text)
                if match:
                    res = json.loads(match.group(1))
                    klines = res.get("data", {}).get("klines", [])
                    if klines:
                        db = SessionLocal()
                        try:
                            # 只保留最新的120条数据，避免数据膨胀
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
                            print(f"      ✓ K线数据获取成功 ({len(klines)}条)")
                            return True
                        finally:
                            db.close()
            print(f"      ⚠️ K线获取失败，使用现有数据")
            return True
            
        except Exception as e:
            print(f"      ⚠️ K线获取异常: {str(e)[:50]}")
            return True
        
    async def _fetch_kline_local(self, stock_code: str):
        """本地数据补充方案"""
        db = SessionLocal()
        try:
            # 检查是否已有部分数据
            existing_count = db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code
            ).count()
            
            if existing_count > 0:
                print(f"      ℹ️ 使用现有{existing_count}条K线数据")
                return True
            
            # 如果完全没有数据，生成基础数据用于分析
            market_data = db.query(DailyMarketData).filter(
                DailyMarketData.code == stock_code
            ).first()
            
            if market_data and market_data.latest_price:
                # 生成一条基础K线数据
                fake_kline = HistoricalData(
                    stock_code=stock_code,
                    date=datetime.date.today(),
                    open=market_data.latest_price,
                    close=market_data.latest_price,
                    high=market_data.latest_price * 1.02,
                    low=market_data.latest_price * 0.98
                )
                db.add(fake_kline)
                db.commit()
                print(f"      ℹ️ 生成基础K线数据用于分析")
                return True
                
        except Exception as e:
            print(f"      ⚠️ 本地数据补充失败: {str(e)[:50]}")
        finally:
            db.close()
        
        return False
    
    async def _save_kline_data(self, stock_code: str, df):
        """保存K线数据的通用方法"""
        db = SessionLocal()
        try:
            # 清理旧数据
            db.query(HistoricalData).filter(HistoricalData.stock_code == stock_code).delete()
            
            # 保存新数据
            for _, row in df.iterrows():
                h = HistoricalData(
                    stock_code=stock_code,
                    date=pd.to_datetime(row['date'] if 'date' in row else row.index).date(),
                    open=self._safe_float(row.get('open', 0)),
                    close=self._safe_float(row.get('close', 0)),
                    high=self._safe_float(row.get('high', 0)),
                    low=self._safe_float(row.get('low', 0))
                )
                db.add(h)
            
            db.commit()
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
        获取财务指标 - 智能缓存增强版
        支持缓存、多源、智能降级策略
        返回: (ROE, 利润增长率)
        """
        # 检查缓存
        cache_key = f"financial_{stock_code}"
        current_time = time.time()
        
        if (cache_key in self.financial_cache and 
            cache_key in self.cache_expiry and 
            current_time < self.cache_expiry[cache_key]):
            cached_data = self.financial_cache[cache_key]
            if self.debug_mode:
                print(f"      📦 使用缓存数据: ROE={cached_data[0]:.2f}%, Growth={cached_data[1]:.2f}%")
            return cached_data
        
        # 初始化默认值
        roe, growth = 0.0, 0.0
        attempts = []
        success_source = None
        
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
                    success_source = "efinance"
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
                    success_source = "akshare_financial"
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
                    success_source = "akshare_indicator"
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
        
        # 数据质量评估和缓存
        data_quality = self._assess_data_quality(roe, growth, success_source)
        
        if data_quality >= 0.7:  # 高质量数据才缓存
            self.financial_cache[cache_key] = (float(roe), float(growth))
            self.cache_expiry[cache_key] = current_time + self.CACHE_TTL
            if self.debug_mode:
                print(f"      💾 缓存高质量数据 (质量: {data_quality:.2f})")
        
        # 所有方法都失败，记录详细信息
        if roe == 0 and growth == 0:
            print(f"      ❌ {stock_code} 财务指标获取完全失败 (尝试了: {', '.join(attempts)})")
        
        return float(roe), float(growth)
    
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

    def _assess_data_quality(self, roe: float, growth: float, source: str) -> float:
        """评估数据质量 (0-1)"""
        quality = 0.0
        
        # 来源权重
        source_weights = {
            "efinance": 1.0,
            "akshare_financial": 0.8,
            "akshare_indicator": 0.6,
            "market_derived": 0.3
        }
        quality += source_weights.get(source, 0.1)
        
        # 数值合理性检查
        if -50 <= roe <= 50:  # ROE合理范围
            quality += 0.3
        if -100 <= growth <= 200:  # 增长率合理范围
            quality += 0.3
            
        # 非零值加分
        if roe != 0:
            quality += 0.2
        if growth != 0:
            quality += 0.2
            
        return min(1.0, quality)

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
        """主分析任务循环 - 智能增量更新版"""
        db = SessionLocal()
        stats = {
            "success": 0, 
            "failed": 0, 
            "financial_failed": 0,
            "network_errors": 0,
            "data_errors": 0,
            "timeout_errors": 0
        }
        semaphore = asyncio.Semaphore(self.settings.CONCURRENT_LIMIT)
        
        try:
            # 修复：去重并验证股票代码格式
            watched_raw = db.query(UserStockWatch.stock_code).distinct().all()
            watched_codes = list(set([w[0] for w in watched_raw if w[0] and len(w[0]) == 6 and w[0].isdigit()]))
            total = len(watched_codes)
            
            # 添加重复检查日志
            if len(watched_raw) != len(watched_codes):
                print(f"⚠️ 发现重复股票代码，原始:{len(watched_raw)} 去重后:{len(watched_codes)}")
            
            # 智能增量更新检查
            update_needed = await self._check_update_needed(db, [(code,) for code in watched_codes])
            if not update_needed:
                print("💡 数据已是最新，跳过更新")
                return
            
            print(f"🚀 启动深度分析 (共 {total} 只)...")
            print(f"📊 配置: 并发数{self.settings.CONCURRENT_LIMIT}, 超时{self.settings.FINANCIAL_FETCH_TIMEOUT}s")
            
            # 获取高优先级股票
            priority_stocks = await self._get_priority_stocks(db, [(code,) for code in watched_codes])
            
            # 记录已处理的股票，防止重复
            processed_stocks = set()
            tasks = []
            
            async def process_stock(i, stock_code):
                # 防止重复处理
                if stock_code in processed_stocks:
                    print(f"   ⚠️ {stock_code} 已在处理队列中，跳过")
                    return
                processed_stocks.add(stock_code)
                
                async with semaphore:
                    try:
                        # 智能跳过K线失败的股票
                        if not await self.fetch_historical_data(stock_code):
                            print(f"      ⚠️ K线获取失败，但仍继续分析...")
                        await self.fetch_stock_dividend_history(stock_code)
                        score = await self.analyze_stock(stock_code, db)
                        
                        if score is not None:
                            stats["success"] += 1
                            print(f"   ✓ {i}/{total} {stock_code} 分析完成 (评分: {score})")
                        else:
                            stats["failed"] += 1
                            print(f"   ❌ {i}/{total} {stock_code} 分析失败")
                        
                    except Exception as e:
                        stats["failed"] += 1
                        error_msg = str(e).lower()
                        
                        if "connection" in error_msg or "timeout" in error_msg:
                            stats["network_errors"] += 1
                        elif "timeout" in error_msg:
                            stats["timeout_errors"] += 1
                        elif "data" in error_msg or "format" in error_msg:
                            stats["data_errors"] += 1
                        else:
                            stats["financial_failed"] += 1
                        
                        print(f"   ❌ {i}/{total} {stock_code} 处理异常: {str(e)[:50]}")
                    
                    # 智能延迟 + 进度显示
                    delay = random.uniform(
                        self.settings.FETCH_DELAY_MIN, 
                        self.settings.FETCH_DELAY_MAX
                    )
                    
                    # 显示详细进度
                    success_rate = (stats["success"] / i * 100) if i > 0 else 0
                    eta_minutes = ((total - i) * (self.settings.FETCH_DELAY_MAX + self.settings.FETCH_DELAY_MIN) / 2) / 60
                    
                    print(f"   💤 等待 {delay:.1f} 秒... (成功率: {success_rate:.1f}%, 预计剩余: {eta_minutes:.1f}分钟)")
                    await asyncio.sleep(delay)
            
            # 先处理高优先级股票
            print(f"🎯 优先处理 {len(priority_stocks)} 只重要股票...")
            for i, code in enumerate(priority_stocks, 1):
                tasks.append(process_stock(i, code))
            
            # 再处理其他股票
            remaining_stocks = [code for code in watched_codes if code not in priority_stocks]
            print(f"📋 处理剩余 {len(remaining_stocks)} 只股票...")
            for i, code in enumerate(remaining_stocks, len(priority_stocks) + 1):
                tasks.append(process_stock(i, code))
                
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # 最终统计
            final_success_rate = (stats["success"] / total) * 100 if total > 0 else 0
            print(f"\n🏁 分析完成!")
            print(f"📊 总体统计:")
            print(f"   总数: {total}")
            print(f"   成功: {stats['success']} ({final_success_rate:.1f}%)")
            print(f"   失败: {stats['failed']}")
            
        except Exception as e:
            print(f"🚨 分析过程中发生严重错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            db.close()
    
    async def _check_update_needed(self, db: Session, watched_stocks):
        """检查是否需要更新"""
        # 检查最新分析日期
        latest_analysis = db.query(StockAnalysisResult).order_by(
            desc(StockAnalysisResult.analysis_date)
        ).first()
        
        if not latest_analysis:
            return True
            
        # 如果今天已经分析过，且股票数量没变，则不需要更新
        today_count = db.query(StockAnalysisResult).filter(
            StockAnalysisResult.analysis_date == datetime.date.today()
        ).count()
        
        # 检查是否所有关注的股票都有今天的分析结果
        watched_codes = set([row[0] for row in watched_stocks])
        today_analyzed_codes = set([
            result.stock_code for result in 
            db.query(StockAnalysisResult.stock_code).filter(
                StockAnalysisResult.analysis_date == datetime.date.today()
            ).all()
        ])
        
        return not watched_codes.issubset(today_analyzed_codes)
    
    async def _get_priority_stocks(self, db: Session, all_stocks):
        """获取高优先级股票（持仓或高评分）"""
        # 获取持仓股票
        holdings = db.query(UserStockHolding.stock_code).filter(
            UserStockHolding.is_active == True
        ).distinct().all()
        
        # 获取高评分股票（上次评分>80）
        high_score = db.query(StockAnalysisResult.stock_code).filter(
            StockAnalysisResult.total_score > 80
        ).distinct().all()
        
        priority_set = set([h[0] for h in holdings] + [s[0] for s in high_score])
        all_codes = set([row[0] for row in all_stocks])
        
        return list(priority_set.intersection(all_codes))
    
    async def _check_network_health(self):
        """检查网络连接健康度"""
        try:
            response = await asyncio.to_thread(
                requests.get, "https://httpbin.org/get", timeout=5
            )
            return response.status_code == 200
        except:
            return False
    
    async def _adaptive_delay(self, network_healthy: bool):
        """自适应延迟调整"""
        if network_healthy:
            return random.uniform(
                self.settings.FETCH_DELAY_MIN,
                self.settings.FETCH_DELAY_MAX
            )
        else:
            # 网络不佳时增加延迟
            return random.uniform(
                self.settings.FETCH_DELAY_MAX,
                self.settings.FETCH_DELAY_MAX * 2
            )
    async def clean_abnormal_pe_data(self):
        """清理异常的PE数据"""
        db = SessionLocal()
        try:
            # 查找异常PE值的记录
            abnormal_records = db.query(StockAnalysisResult).filter(
                StockAnalysisResult.pe_ratio > 1000000
            ).all()
            
            if abnormal_records:
                print(f"🔍 发现 {len(abnormal_records)} 条异常PE数据记录")
                for record in abnormal_records:
                    print(f"   {record.stock_code} - {record.analysis_date}: PE={record.pe_ratio}")
                    # 修正为0或重新计算
                    record.pe_ratio = 0.0
                    
                db.commit()
                print("✅ 异常PE数据已清理")
            else:
                print("✅ 未发现异常PE数据")
                
        except Exception as e:
            print(f"❌ 清理异常数据失败: {e}")
        finally:
            db.close()

    async def validate_analysis_data(self):
        """验证分析数据的合理性"""
        db = SessionLocal()
        try:
            # 检查最近一周的分析数据
            one_week_ago = datetime.date.today() - datetime.timedelta(days=7)
            
            suspicious_records = db.query(StockAnalysisResult).filter(
                StockAnalysisResult.analysis_date >= one_week_ago,
                (StockAnalysisResult.pe_ratio > 1000000) | 
                (StockAnalysisResult.pe_ratio < 0) |
                (StockAnalysisResult.total_score > 100) |
                (StockAnalysisResult.total_score < 0)
            ).all()
            
            if suspicious_records:
                print(f"⚠️ 发现 {len(suspicious_records)} 条可疑数据:")
                for record in suspicious_records:
                    issues = []
                    if record.pe_ratio > 1000000 or record.pe_ratio < 0:
                        issues.append(f"PE异常({record.pe_ratio})")
                    if record.total_score > 100 or record.total_score < 0:
                        issues.append(f"评分异常({record.total_score})")
                    
                    print(f"   {record.stock_code} {record.analysis_date}: {', '.join(issues)}")
            else:
                print("✅ 数据验证通过")
                
        except Exception as e:
            print(f"❌ 数据验证失败: {e}")
        finally:
            db.close()

stock_service = StockDataService()
