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
        
        # 优化请求会话配置
        self.session = requests.Session()
        self.session.trust_env = False
        self.session.proxies = {"http": None, "https": None}
        
        # 增强连接池配置
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,  # 总重试次数
            backoff_factor=1,  # 退避因子
            status_forcelist=[429, 500, 502, 503, 504],  # 需要重试的状态码
            allowed_methods=["HEAD", "GET", "OPTIONS"]  # 允许重试的方法
        )
        
        adapter = HTTPAdapter(
            pool_connections=10,  # 连接池大小
            pool_maxsize=20,      # 最大连接数
            max_retries=retry_strategy
        )
        
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # 设置请求头
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Cache-Control": "max-age=0"
        }
        self.session.headers.update(self.headers)
        
        # 目标参数 (东方财富)
        self.target_ut = "bd1d9ddb04089700cf9c27f6f7426281"
        self.target_cookies = {
            "ut": self.target_ut,
        }

        self._check_akshare_interfaces()

    def _check_akshare_interfaces(self):
        """检查akshare可用接口"""
        if self.debug_mode:
            print("🔍 检查akshare接口可用性...")
        
        # 测试常用接口
        interfaces_to_check = [
            'stock_financial_abstract_ths',
            'stock_financial_report_sina', 
            'stock_a_indicator_lg',
            'stock_a_lg_indicator',
            'stock_individual_info'
        ]
        
        available_interfaces = []
        for interface in interfaces_to_check:
            if hasattr(ak, interface):
                available_interfaces.append(interface)
                if self.debug_mode:
                    print(f"   ✓ {interface}")
            else:
                if self.debug_mode:
                    print(f"   ✗ {interface}")
        
        self.available_akshare_interfaces = available_interfaces
        if self.debug_mode:
            print(f"✅ 可用接口: {len(available_interfaces)}个")

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
        """增强版数据抓取 - 双重保障 (Akshare官方接口 + 极简防屏蔽直连)"""
        print(f"\n🌐 启动全市场行情抓取...")

        # 🏆 方案 A: 优先使用开源社区持续更新的 Akshare 接口 (最稳定、最抗封)
        try:
            print("   ➤ 尝试使用 Akshare 官方通道获取数据...")
            # 调用 akshare 的东方财富实时行情接口
            df = await asyncio.to_thread(ak.stock_zh_a_spot_em)
            if df is not None and not df.empty:
                print("   ✓ Akshare 通道获取成功！")
                
                # 映射 akshare 的中文列名到数据库英文字段
                ak_map = {
                    '代码': 'code',
                    '名称': 'name',
                    '最新价': 'latest_price',
                    '涨跌幅': 'change_pct',
                    '市盈率-动态': 'pe_dynamic',
                    '市净率': 'pb',
                    '成交量': 'volume',
                    '成交额': 'amount'
                }
                df = df.rename(columns=ak_map)
                
                # 清洗可能的非数值 (把 '-' 或 'NaN' 转为 0)
                for col in ['latest_price', 'change_pct', 'pe_dynamic', 'pb', 'volume', 'amount']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        
                print(f"✅ 总计获取 {len(df)} 条数据")
                return df
                
        except Exception as e:
            print(f"   ⚠️ Akshare 通道失败: {str(e)[:100]}")
            print("   🔄 自动切换至纯净备用通道...")

        # 🛡️ 方案 B: 极简纯净 HTTP 直连 
        # (去掉复杂的假Cookie，仅保留最核心的浏览器头部，防止画蛇添足被拦截)
        print("   ➤ 启用原生极简 HTTP 瀑布流抓取...")
        all_dfs = []
        current_page = 1
        total_pages = 999
        url = "https://push2.eastmoney.com/api/qt/clist/get"

        headers = {
            "Host": "push2.eastmoney.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Referer": "https://quote.eastmoney.com/",
            "Connection": "keep-alive"
        }

        # 构建一个带重试机制的纯净 Session
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        session = requests.Session()
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[403, 429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        session.headers.update(headers)

        while current_page <= total_pages:
            cb_name = f"jQuery3410{random.randint(100000, 999999)}_{int(time.time()*1000)}"
            params = {
                "cb": cb_name,
                "pn": str(current_page),
                "np": "1",
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
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
                response = await asyncio.to_thread(session.get, url, params=params, timeout=15, verify=False)
                
                if response.status_code != 200: break
                match = re.search(r'jQuery.*?\((.*)\)', response.text)
                if not match: break

                res_json = json.loads(match.group(1))
                if not res_json or not res_json.get("data"): break

                # 第一页获取总页数
                if current_page == 1:
                    total_pages = (res_json["data"]["total"] + page_size - 1) // page_size

                batch_df = pd.DataFrame(res_json["data"]["diff"])
                all_dfs.append(batch_df)

                if current_page >= total_pages: break
                
                # 备用方案稍微短一点的等待时间
                wait_time = random.uniform(2.5, 6.0)
                await asyncio.sleep(wait_time)
                current_page += 1

            except Exception as e:
                print(f"   ❌ 备用通道报错停止: {str(e)[:50]}")
                break

        session.close()
        if not all_dfs: return pd.DataFrame()

        final_df = pd.concat(all_dfs, ignore_index=True)
        em_fields_map = {
            'f12': 'code', 'f14': 'name', 'f2': 'latest_price', 
            'f3': 'change_pct', 'f9': 'pe_dynamic', 'f22': 'pb', 
            'f5': 'volume', 'f6': 'amount'
        }
        rename_map = {k: v for k, v in em_fields_map.items() if k in final_df.columns}
        final_df = final_df.rename(columns=rename_map)
        
        print(f"\n✅ 备用通道获取 {len(final_df)} 条数据")
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
        """同步历史K线 - 增强版(网络稳定性优化)"""
        # 首先检查本地数据
        db = SessionLocal()
        try:
            existing_count = db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code
            ).count()
            
            # 优化：如果已有足够数据（比如100条以上），就不重复获取
            if existing_count >= 100:
                if self.debug_mode:
                    print(f"      ℹ️ 已有{existing_count}条K线数据，跳过获取")
                return True
        finally:
            db.close()
        
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
                "lmt": "120", "_": str(int(time.time() * 1000))
            }
            
            # 使用增强的会话和重试机制
            response = await asyncio.to_thread(
                self._robust_request, url, params, timeout=20
            )
            
            if response and response.status_code == 200:
                match = re.search(r'\(({.*})\)', response.text)
                if match:
                    res = json.loads(match.group(1))
                    klines = res.get("data", {}).get("klines", [])
                    if klines:
                        db = SessionLocal()
                        try:
                            # 只保留最新的120条数据，避免数据膨胀
                            db.query(HistoricalData).filter(HistoricalData.stock_code == stock_code).delete()
                            saved_count = 0
                            for line in klines:
                                cols = line.split(',')
                                if len(cols) >= 5:  # 确保数据完整
                                    h = HistoricalData(
                                        stock_code=stock_code,
                                        date=datetime.datetime.strptime(cols[0], "%Y-%m-%d").date(),
                                        open=self._safe_float(cols[1]), 
                                        close=self._safe_float(cols[2]),
                                        high=self._safe_float(cols[3]), 
                                        low=self._safe_float(cols[4])
                                    )
                                    db.add(h)
                                    saved_count += 1
                            db.commit()
                            if self.debug_mode:
                                print(f"      ✓ K线数据获取成功 ({saved_count}条)")
                            return True
                        except Exception as e:
                            if self.debug_mode:
                                print(f"      ⚠️ K线数据保存异常: {str(e)[:50]}")
                            db.rollback()
                        finally:
                            db.close()
            
            if self.debug_mode:
                print(f"      ⚠️ K线获取失败，使用现有数据")
            return True
            
        except Exception as e:
            if self.debug_mode:
                print(f"      ⚠️ K线获取异常: {str(e)[:100]}")
            return True

    def _robust_request(self, url, params, timeout=20):
        """增强版HTTP请求 - 遇到403/429自动重试与反爬休眠"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                # 动态追加防爬头部
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
                    "Referer": "https://quote.eastmoney.com/"
                }
                response = self.session.get(url, params=params, headers=headers, timeout=timeout, verify=False)
                
                if response.status_code == 200:
                    return response
                elif response.status_code in [403, 429]:
                    if getattr(self, 'debug_mode', False):
                        print(f"      ⚠️ 被反爬拦截 (HTTP {response.status_code})，休眠伪装中...")
                    time.sleep(random.uniform(5, 12))
                    continue
                elif response.status_code in [500, 502, 503, 504]:
                    time.sleep((attempt + 1) * 2)
                    continue
                else:
                    return None
            except requests.exceptions.RequestException as e:
                time.sleep((attempt + 1) * 3)
                continue
        return None
        
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
        """获取财务指标 - 修复版"""
        # 缓存检查
        cache_key = f"financial_{stock_code}"
        if cache_key in self.financial_cache:
            if time.time() < self.cache_expiry[cache_key]:
                cached_data = self.financial_cache[cache_key]
                if self.debug_mode:
                    print(f"      ℹ️ 使用缓存财务数据: ROE={cached_data[0]:.2f}%, Growth={cached_data[1]:.2f}%")
                return cached_data
        
        roe, growth = 0.0, 0.0
        attempts = []
        success_source = "none"
        
        try:
            # 1. 首选：efinance 财务数据
            attempts.append("efinance")
            df = await asyncio.to_thread(ef.stock.get_base_info, stock_code)
            
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
                    if self.debug_mode:
                        print(f"      ✓ 通过 efinance 获取财务数据: ROE={roe:.2f}%, Growth={growth:.2f}%")
                    success_source = "efinance"
                    # 缓存结果
                    self.financial_cache[cache_key] = (float(roe), float(growth))
                    self.cache_expiry[cache_key] = time.time() + self.CACHE_TTL
                    return float(roe), float(growth)
                    
        except Exception as e:
            if self.debug_mode:
                print(f"      ⚠️ efinance 失败: {str(e)[:50]}")
        
        try:
            # 2. 备选：akshare 财务报表 (修复接口调用)
            attempts.append("akshare_financial")
            formatted_code = self._format_stock_code_for_akshare(stock_code)
            
            # 使用正确的akshare接口
            try:
                df_fin = await asyncio.to_thread(ak.stock_financial_abstract_ths, symbol=stock_code)
            except AttributeError:
                # 如果上面接口不存在，尝试其他接口
                try:
                    df_fin = await asyncio.to_thread(ak.stock_financial_report_sina, symbol=formatted_code)
                except:
                    df_fin = None
            
            if df_fin is not None and not df_fin.empty and len(df_fin) > 0:
                data_fin = df_fin.iloc[0].to_dict()
                
                # akshare 字段名
                roe = self._safe_float(data_fin.get('净资产收益率') or 
                                    data_fin.get('ROE') or 
                                    data_fin.get('净资产收益率(%)') or 0)
                growth = self._safe_float(data_fin.get('净利润同比增长') or 
                                        data_fin.get('净利润增长率') or 
                                        data_fin.get('净利润同比(%)') or 0)
                
                if roe != 0 or growth != 0:
                    if self.debug_mode:
                        print(f"      ✓ 通过 akshare 获取财务数据: ROE={roe:.2f}%, Growth={growth:.2f}%")
                    success_source = "akshare_financial"
                    self.financial_cache[cache_key] = (float(roe), float(growth))
                    self.cache_expiry[cache_key] = time.time() + self.CACHE_TTL
                    return float(roe), float(growth)
                    
        except Exception as e:
            if self.debug_mode:
                print(f"      ⚠️ akshare financial 失败: {str(e)[:50]}")
        
        try:
            # 3. 再备选：akshare 主要指标 (修复接口名称)
            attempts.append("akshare_indicator")
            formatted_code = self._format_stock_code_for_akshare(stock_code)
            
            # 尝试多种akshare指标接口
            df_ind = None
            indicator_functions = [
                'stock_a_indicator_lg',  # 正确的接口名
                'stock_a_lg_indicator',  # 备选接口名
                'stock_individual_info', # 其他可能的接口
            ]
            
            for func_name in indicator_functions:
                try:
                    if hasattr(ak, func_name):
                        df_ind = await asyncio.to_thread(getattr(ak, func_name), symbol=stock_code)
                        if df_ind is not None and not df_ind.empty:
                            break
                except:
                    continue
            
            if df_ind is not None and not df_ind.empty and len(df_ind) > 0:
                data_ind = df_ind.iloc[0].to_dict()
                
                # 指标字段名匹配
                roe_fields = ['净资产收益率(%)', 'ROE', 'roe', '净资产收益率']
                growth_fields = ['净利润同比(%)', '净利润增长率(%)', '净利润同比增长']
                
                for field in roe_fields:
                    if field in data_ind and data_ind[field] is not None:
                        roe_val = self._safe_float(data_ind[field])
                        if roe_val != 0:
                            roe = roe_val
                            break
                
                for field in growth_fields:
                    if field in data_ind and data_ind[field] is not None:
                        growth_val = self._safe_float(data_ind[field])
                        if growth_val != 0:
                            growth = growth_val
                            break
                
                if roe != 0 or growth != 0:
                    if self.debug_mode:
                        print(f"      ✓ 通过 akshare 指标获取: ROE={roe:.2f}%, Growth={growth:.2f}%")
                    success_source = "akshare_indicator"
                    self.financial_cache[cache_key] = (float(roe), float(growth))
                    self.cache_expiry[cache_key] = time.time() + self.CACHE_TTL
                    return float(roe), float(growth)
                    
        except Exception as e:
            if self.debug_mode:
                print(f"      ⚠️ akshare indicator 失败: {str(e)[:50]}")
        
        # 4. 最后备选：从市场价格数据推算
        try:
            attempts.append("market_derived")
            derived_roe, derived_growth = await self._derive_financial_from_market(stock_code)
            if derived_roe != 0 or derived_growth != 0:
                if self.debug_mode:
                    print(f"      ✓ 通过市场数据推算: ROE={derived_roe:.2f}%, Growth={derived_growth:.2f}%")
                success_source = "market_derived"
                self.financial_cache[cache_key] = (float(derived_roe), float(derived_growth))
                self.cache_expiry[cache_key] = time.time() + self.CACHE_TTL
                return float(derived_roe), float(derived_growth)
        except Exception as e:
            if self.debug_mode:
                print(f"      ⚠️ 市场数据推算失败: {str(e)[:50]}")
        
        # 所有方法都失败，记录详细信息
        if roe == 0 and growth == 0:
            if self.debug_mode:
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
        """从市场价格数据推算基础财务指标 - 增强版"""
        db = SessionLocal()
        try:
            # 获取历史价格数据推算趋势
            hist_data = db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code
            ).order_by(HistoricalData.date.desc()).limit(252).all()  # 一年数据
            
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
            # 对于科创板股票(688开头)，使用不同的估算逻辑
            if stock_code.startswith('688'):
                derived_roe = max(0, min(30, abs(derived_growth) * 0.6))  # 科创板估值更高
            else:
                derived_roe = max(0, min(30, abs(derived_growth) * 0.8))  # 传统股票
            
            if self.debug_mode:
                print(f"      ℹ️ 市场数据推算: ROE≈{derived_roe:.2f}%, Growth≈{derived_growth:.2f}% (基于{len(prices)}天数据)")
            
            return float(derived_roe), float(derived_growth)
            
        except Exception as e:
            if self.debug_mode:
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

    

    async def analyze_all_watched_stocks(self):
        """主分析任务循环 - 修复版"""
        db = SessionLocal()
        stats = {
            "success": 0, 
            "failed": 0, 
            "financial_failed": 0,
            "network_errors": 0,
            "data_errors": 0,
            "timeout_errors": 0,
            "total_processed": 0  # 改名为total_processed避免混淆
        }
        semaphore = asyncio.Semaphore(self.settings.CONCURRENT_LIMIT)
        
        try:
            # 获取关注股票列表
            watched_raw = db.query(UserStockWatch.stock_code).distinct().all()
            watched_codes = list(set([w[0] for w in watched_raw if w[0] and len(w[0]) == 6 and w[0].isdigit()]))
            total = len(watched_codes)
            
            print(f"🚀 启动深度分析 (共 {total} 只)...")
            print(f"📊 配置: 并发数{self.settings.CONCURRENT_LIMIT}, 超时{self.settings.FINANCIAL_FETCH_TIMEOUT}s")
            
            # 获取高优先级股票
            priority_stocks = await self._get_priority_stocks(db, [(code,) for code in watched_codes])
            print(f"🎯 优先处理 {len(priority_stocks)} 只重要股票...")
            
            # 记录已处理的股票
            processed_stocks = set()
            tasks = []
            
            async def process_stock(stock_index, stock_code):
                # 防止重复处理
                if stock_code in processed_stocks:
                    return
                processed_stocks.add(stock_code)
                
                async with semaphore:
                    try:
                        stats["total_processed"] += 1
                        current_index = stats["total_processed"]
                        
                        # 智能跳过K线失败的股票
                        kline_success = await self.fetch_historical_data(stock_code)
                        if not kline_success and self.debug_mode:
                            print(f"      ⚠️ K线获取失败，但仍继续分析...")
                        
                        await self.fetch_stock_dividend_history(stock_code)
                        score = await self.analyze_stock(stock_code, db)
                        
                        if score is not None:
                            stats["success"] += 1
                            # 修复成功率计算
                            success_rate = (stats["success"] / current_index) * 100 if current_index > 0 else 0
                            print(f"   ✓ {current_index}/{total} {stock_code} 分析完成 (评分: {score}, 成功率: {success_rate:.1f}%)")
                        else:
                            stats["failed"] += 1
                            success_rate = (stats["success"] / current_index) * 100 if current_index > 0 else 0
                            print(f"   ❌ {current_index}/{total} {stock_code} 分析失败 (成功率: {success_rate:.1f}%)")
                        
                    except Exception as e:
                        stats["failed"] += 1
                        stats["total_processed"] += 1
                        current_index = stats["total_processed"]
                        success_rate = (stats["success"] / current_index) * 100 if current_index > 0 else 0
                        error_msg = str(e).lower()
                        
                        if "connection" in error_msg or "disconnected" in error_msg:
                            stats["network_errors"] += 1
                        elif "timeout" in error_msg:
                            stats["timeout_errors"] += 1
                        elif "data" in error_msg or "format" in error_msg:
                            stats["data_errors"] += 1
                        else:
                            stats["financial_failed"] += 1
                        
                        print(f"   ❌ {current_index}/{total} {stock_code} 处理异常: {str(e)[:50]} (成功率: {success_rate:.1f}%)")
                    
                    # 延迟策略
                    delay = random.uniform(
                        self.settings.FETCH_DELAY_MIN, 
                        self.settings.FETCH_DELAY_MAX
                    )
                    
                    # 显示详细进度
                    remaining = total - current_index
                    eta_minutes = (remaining * delay) / 60 if remaining > 0 else 0
                    print(f"   💤 等待 {delay:.1f} 秒... (预计剩余: {eta_minutes:.1f}分钟)")
                    await asyncio.sleep(delay)
            
            # 处理所有股票
            all_stocks = priority_stocks + [code for code in watched_codes if code not in priority_stocks]
            for i, code in enumerate(all_stocks, 1):
                tasks.append(process_stock(i, code))
                
            await asyncio.gather(*tasks, return_exceptions=True)
            
            # 最终统计
            final_success_rate = (stats["success"] / total) * 100 if total > 0 else 0
            print(f"\n🏁 分析完成!")
            print(f"📊 总体统计:")
            print(f"   总数: {total}")
            print(f"   成功: {stats['success']} ({final_success_rate:.1f}%)")
            print(f"   失败: {stats['failed']}")
            if stats["network_errors"] > 0:
                print(f"   网络错误: {stats['network_errors']}")
            if stats["timeout_errors"] > 0:
                print(f"   超时错误: {stats['timeout_errors']}")
            if stats["data_errors"] > 0:
                print(f"   数据错误: {stats['data_errors']}")
            if stats["financial_failed"] > 0:
                print(f"   财务数据失败: {stats['financial_failed']}")
                
        except Exception as e:
            print(f"🚨 分析过程中发生严重错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            db.close()
    
    async def analyze_stock(self, stock_code: str, db: Session):
        """深度分析单只股票 (优化版 100 分评分机制)"""
        today = datetime.date.today()
        market = db.query(DailyMarketData).filter(DailyMarketData.code == stock_code).order_by(desc(DailyMarketData.date)).first()
        if not market or not market.latest_price: return None

        # 1. 趋势与波动率 (20分)
        v30, v60, vol_score = 0.0, 0.0, 0
        ma60 = 0.0
        hist = db.query(HistoricalData).filter(HistoricalData.stock_code == stock_code, HistoricalData.close.isnot(None)).order_by(desc(HistoricalData.date)).limit(100).all()

        if len(hist) >= 20:
            prices = [float(h.close) for h in reversed(hist)]
            price_series = pd.Series(prices)
            log_returns = np.log(price_series / price_series.shift(1)).dropna()
            
            if len(log_returns) >= 30: v30 = log_returns.tail(30).std() * np.sqrt(252) * 100
            if len(log_returns) >= 60:
                v60 = log_returns.tail(60).std() * np.sqrt(252) * 100
                ma60 = price_series.tail(60).mean() 
            
            vol_sub_score = 10 if 0 < v30 < 25 else (7 if v30 < 35 else (4 if v30 < 45 else 0))
            trend_sub_score = 0
            if ma60 > 0:
                latest_p = market.latest_price
                if latest_p > ma60 * 1.05: trend_sub_score = 10     
                elif latest_p >= ma60: trend_sub_score = 7          
                elif latest_p > ma60 * 0.90: trend_sub_score = 3    
            vol_score = vol_sub_score + trend_sub_score

        # 2. 股息率防守计算 (20分)
        div_yield, div_score = 0.0, 0
        one_year_ago = today - datetime.timedelta(days=365)
        dividends = db.query(DividendData).filter(DividendData.stock_code == stock_code, DividendData.ex_dividend_date >= one_year_ago).all()
        total_cash_div = 0.0
        if dividends:
            for d in dividends:
                match = re.search(r'派(\d+\.?\d*)', str(d.dividend))
                if match: total_cash_div += float(match.group(1)) / 10
            
            if total_cash_div > 0 and market.latest_price > 0:
                div_yield = (total_cash_div / market.latest_price) * 100
            
            if div_yield >= 4.0: div_score = 20
            elif div_yield >= 2.5: div_score = 15
            elif div_yield >= 1.0: div_score = 10
            elif div_yield > 0: div_score = 5

        # 3. 财务与估值 (60分)
        roe, profit_growth = await self.fetch_financial_metrics(stock_code)
        
        roe_score = 20 if roe >= 20 else (15 if roe >= 15 else (10 if roe >= 10 else (5 if roe >= 5 else 0)))
        pg_score = 20 if profit_growth >= 30 else (15 if profit_growth >= 15 else (10 if profit_growth > 0 else (5 if profit_growth > -10 else 0)))
        
        pe_score = 0
        pe = market.pe_dynamic
        if 0 < pe <= 15: pe_score = 20        
        elif 15 < pe <= 30: pe_score = 15     
        elif 30 < pe <= 50: pe_score = 10     
        elif 50 < pe <= 100: pe_score = 5     
        
        growth_score = roe_score + pg_score + pe_score
        total_score = int(vol_score + div_score + growth_score)
        
        # 动态评级
        if total_score >= 80: suggestion = "强烈推荐"
        elif total_score >= 65: suggestion = "推荐买入"
        elif total_score >= 50: suggestion = "观望持仓"
        else: suggestion = "谨慎回避"

        analysis_res = StockAnalysisResult(
            stock_code=stock_code, stock_name=market.name, analysis_date=today,
            latest_price=market.latest_price, pe_ratio=market.pe_dynamic, pb_ratio=market.pb,
            volatility_30d=round(v30, 2) if v30 > 0 else 0.0,
            volatility_60d=round(v60, 2) if v60 > 0 else 0.0,
            dividend_yield=round(div_yield, 2) if div_yield > 0 else 0.0,
            roe=round(roe, 2) if roe > 0 else 0.0,
            profit_growth=round(profit_growth, 2) if profit_growth else 0.0,
            volatility_score=int(vol_score), dividend_score=int(div_score), growth_score=int(growth_score),
            total_score=total_score, suggestion=suggestion, data_source="automated_v4" 
        )

        try:
            db.merge(analysis_res)
            db.commit()
            return analysis_res.total_score
        except Exception as e:
            db.rollback()
            print(f"   ❌ {stock_code} 结果入库失败: {e}")
            return None

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
