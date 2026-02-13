import os
import socket
# 1. 顶级补丁：强制 IPv4
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
from sqlalchemy import create_engine, Column, String, Float, DateTime, Integer, desc, func, Text, Boolean, Date
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
import email_service

# ============================================================
# 网络配置
# ============================================================
_orig_request = RequestSession.request
def my_request(self, method, url, **kwargs):
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
class User(Base):
    """
    用户表 - 存储系统用户信息
    
    用途: 管理系统用户,实现多用户隔离
    """
    __tablename__ = "users"
    
    # 系统字段
    user_id = Column(Integer, primary_key=True, autoincrement=True, comment="用户ID - 系统自增")
    
    # 登录信息
    account = Column(String, unique=True, nullable=False, comment="登录账号 - 用户名,唯一,用于登录")
    nickname = Column(String, nullable=False, comment="用户昵称 - 显示名称")
    password_hash = Column(String, nullable=False, comment="密码哈希 - 使用bcrypt加密存储")
    
    # 通知设置
    email = Column(String, nullable=False, comment="通知邮箱 - 用于接收分析报告")
    email_verified = Column(Boolean, default=False, comment="邮箱验证状态 - True:已验证, False:未验证")
    enable_daily_report = Column(Boolean, default=True, comment="启用每日报告 - True:发送, False:不发送")
    
    # 其他信息
    avatar_url = Column(String, comment="头像URL - 用户头像地址")
    phone = Column(String, comment="手机号 - 可选")
    
    # 状态信息
    is_active = Column(Boolean, default=True, comment="账号状态 - True:正常, False:禁用")
    last_login_at = Column(DateTime, comment="最后登录时间")
    
    # 时间戳
    created_at = Column(DateTime, default=datetime.datetime.now, comment="注册时间")
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now, comment="更新时间")

class UserStockWatch(Base):
    """
    用户股票关注表 - 存储用户关注的股票列表
    
    用途: 记录每个用户关注的股票,支持个性化分析
    """
    __tablename__ = "user_stock_watch"
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="主键ID - 自增")
    user_id = Column(String, index=True, comment="用户ID - 外键关联users表")
    stock_code = Column(String, index=True, comment="股票代码 - 6位数字,如600036")
    added_at = Column(DateTime, default=datetime.datetime.now, comment="添加时间 - 用户添加关注的时间")

# ============================================================
# 持仓记录表 
# ============================================================

class UserStockHolding(Base):
    """
    用户持仓表 - 记录用户购买的股票
    
    功能:
    - 记录购买数量和价格
    - 计算持仓成本和盈亏
    - 支持多次买入(不同批次)
    """
    __tablename__ = "user_stock_holdings"
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="主键ID")
    
    # 关联信息
    user_id = Column(Integer, index=True, nullable=False, comment="用户ID - 外键关联users.user_id")
    stock_code = Column(String, index=True, nullable=False, comment="股票代码 - 6位数字")
    stock_name = Column(String, comment="股票名称 - 冗余字段,方便查询")
    
    # 购买信息
    purchase_quantity = Column(Integer, nullable=False, comment="购买数量 - 股数(股)")
    purchase_price = Column(Float, nullable=False, comment="购买单价 - 买入价格(元/股)")
    purchase_amount = Column(Float, comment="购买金额 - 数量*单价(元)")
    purchase_date = Column(Date, nullable=False, comment="购买日期 - 实际买入日期")
    
    # 成本信息
    commission = Column(Float, default=0, comment="手续费 - 交易手续费(元)")
    total_cost = Column(Float, comment="总成本 - 购买金额+手续费(元)")
    cost_price = Column(Float, comment="成本价 - 总成本/数量(元/股)")
    
    # 当前状态
    current_quantity = Column(Integer, comment="当前持有数量 - 可能因卖出而减少(股)")
    current_price = Column(Float, comment="当前价格 - 最新市价(元/股,自动更新)")
    current_value = Column(Float, comment="当前市值 - 当前数量*当前价格(元)")
    
    # 盈亏信息
    profit_loss = Column(Float, comment="浮动盈亏 - 当前市值-总成本(元)")
    profit_loss_pct = Column(Float, comment="盈亏比例 - (当前价-成本价)/成本价*100(%)")
    
    # 交易记录
    trade_type = Column(String, default='buy', comment="交易类型 - buy:买入, sell:卖出, dividend:分红")
    trade_note = Column(Text, comment="交易备注 - 用户自定义备注")
    
    # 状态标记
    is_active = Column(Boolean, default=True, comment="是否持有 - True:持有中, False:已卖出")
    
    # 时间戳
    created_at = Column(DateTime, default=datetime.datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now, comment="更新时间")


# ============================================================
# 邮件通知记录表
# ============================================================

class EmailNotification(Base):
    """
    邮件通知记录表 - 记录每次发送的邮件
    
    功能:
    - 跟踪邮件发送状态
    - 记录失败原因
    - 支持重发机制
    """
    __tablename__ = "email_notifications"
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="主键ID")
    
    # 收件信息
    user_id = Column(Integer, index=True, nullable=False, comment="用户ID")
    recipient_email = Column(String, nullable=False, comment="收件人邮箱")
    
    # 邮件内容
    email_type = Column(String, nullable=False, comment="邮件类型 - daily_report:每日报告, verify:验证邮件, alert:预警")
    subject = Column(String, nullable=False, comment="邮件主题")
    content = Column(Text, comment="邮件内容 - HTML格式")
    
    # 附件信息
    has_attachment = Column(Boolean, default=False, comment="是否有附件")
    attachment_path = Column(String, comment="附件路径 - CSV文件路径")
    attachment_name = Column(String, comment="附件名称 - 显示的文件名")
    
    # 发送状态
    status = Column(String, default='pending', comment="发送状态 - pending:待发送, sent:已发送, failed:失败")
    send_time = Column(DateTime, comment="发送时间 - 实际发送时间")
    error_message = Column(Text, comment="错误信息 - 发送失败时的错误详情")
    retry_count = Column(Integer, default=0, comment="重试次数")
    
    # 时间戳
    created_at = Column(DateTime, default=datetime.datetime.now, comment="创建时间")
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now, comment="更新时间")

# ============================================================
# 市场数据表
# ============================================================

class DailyMarketData(Base):
    """
    每日市场数据表 - 存储全市场股票的每日实时行情
    
    数据源: 东方财富网API (stock_zh_a_spot_em)
    更新频率: 每日15:30自动更新
    用途: 获取最新价格、估值、成交等实时数据
    """
    __tablename__ = "daily_market_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="主键ID - 自增")
    date = Column(Date, index=True, comment="数据日期 - 交易日期")
    code = Column(String, index=True, comment="股票代码 - 6位数字")
    name = Column(String, comment="股票名称 - 中文简称,如'招商银行'")
    
    # 价格相关字段
    latest_price = Column(Float, comment="最新价 - 当前交易价格(元)")
    change_pct = Column(Float, comment="涨跌幅 - 相对昨收的涨跌百分比(%)")
    change_amount = Column(Float, comment="涨跌额 - 相对昨收的涨跌金额(元)")
    high = Column(Float, comment="最高价 - 当日最高成交价(元)")
    low = Column(Float, comment="最低价 - 当日最低成交价(元)")
    open = Column(Float, comment="开盘价 - 当日开盘价格(元)")
    close_prev = Column(Float, comment="昨收价 - 前一交易日收盘价(元)")
    
    # 成交相关字段
    volume = Column(Float, comment="成交量 - 当日成交股票数量(手,1手=100股)")
    amount = Column(Float, comment="成交额 - 当日成交金额总额(元)")
    amplitude = Column(Float, comment="振幅 - (最高-最低)/昨收*100(%)")
    turnover_rate = Column(Float, comment="换手率 - 成交量/流通股本*100(%)")
    volume_ratio = Column(Float, comment="量比 - 当日成交量/近5日平均成交量")
    
    # 估值相关字段
    pe_dynamic = Column(Float, comment="市盈率-动态 - 股价/最近12个月每股收益")
    pb = Column(Float, comment="市净率 - 股价/每股净资产")
    
    # 市值相关字段
    total_market_cap = Column(Float, comment="总市值 - 股价*总股本(元)")
    circulating_market_cap = Column(Float, comment="流通市值 - 股价*流通股本(元)")
    
    # 其他指标
    rise_speed = Column(Float, comment="涨速 - 当前涨跌幅变化速度(%/分钟)")
    change_5min = Column(Float, comment="5分钟涨跌 - 最近5分钟的涨跌幅(%)")
    
    updated_at = Column(DateTime, default=datetime.datetime.now, comment="更新时间 - 数据入库时间")


# ============================================================
# 历史数据表
# ============================================================

class HistoricalData(Base):
    """
    历史行情数据表 - 存储股票的历史K线数据
    
    数据源: efinance / akshare (前复权)
    更新频率: 按需获取
    用途: 计算技术指标(波动率、均线等)
    数据类型: 前复权数据,已调整历史价格
    """
    __tablename__ = "historical_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="主键ID - 自增")
    stock_code = Column(String, index=True, comment="股票代码 - 6位数字")
    date = Column(Date, index=True, comment="交易日期 - K线日期")
    
    # OHLC数据 (Open High Low Close)
    open = Column(Float, comment="开盘价 - 当日开盘价格(元,前复权)")
    close = Column(Float, comment="收盘价 - 当日收盘价格(元,前复权)")
    high = Column(Float, comment="最高价 - 当日最高价格(元,前复权)")
    low = Column(Float, comment="最低价 - 当日最低价格(元,前复权)")
    
    # 成交数据
    volume = Column(Integer, comment="成交量 - 当日成交股数(股)")
    amount = Column(Float, comment="成交额 - 当日成交金额(元)")
    
    # 技术指标
    amplitude = Column(Float, comment="振幅 - (最高-最低)/昨收*100(%)")
    change_pct = Column(Float, comment="涨跌幅 - (收盘-昨收)/昨收*100(%)")
    change_amount = Column(Float, comment="涨跌额 - 收盘价-昨收价(元)")
    turnover_rate = Column(Float, comment="换手率 - 成交量/流通股本*100(%)")
    
    created_at = Column(DateTime, default=datetime.datetime.now, comment="创建时间 - 数据入库时间")


# ============================================================
# 分红数据表
# ============================================================

class DividendData(Base):
    """
    分红派息数据表 - 存储股票的分红配股信息
    
    数据源: 百度股市通 (news_trade_notify_dividend_baidu)
    更新频率: 按需获取
    用途: 计算股息率,评估分红能力
    """
    __tablename__ = "dividend_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="主键ID - 自增")
    stock_code = Column(String, index=True, comment="股票代码 - 6位数字")
    stock_name = Column(String, comment="股票名称 - 中文简称")
    ex_dividend_date = Column(Date, index=True, comment="除权除息日 - 分红生效日期")
    
    # 分红方案
    dividend = Column(String, comment="现金分红 - 每10股派现金额(元),如'10派5'表示每10股派5元")
    bonus_share = Column(String, comment="送股 - 每10股送股数量,如'10送3'表示每10股送3股")
    capitalization = Column(String, comment="转增股本 - 每10股转增数量,如'10转5'表示每10股转增5股")
    physical = Column(String, comment="实物分配 - 其他形式的分配")
    
    # 其他信息
    exchange = Column(String, comment="交易所 - 上交所/深交所")
    report_period = Column(String, comment="报告期 - 分红对应的财报期,如'2023年报'")
    
    created_at = Column(DateTime, default=datetime.datetime.now, comment="创建时间 - 数据入库时间")


# ============================================================
# 分析结果表
# ============================================================

class StockAnalysisResult(Base):
    """
    股票分析结果表 - 存储股票的综合分析评分
    
    生成方式: 系统自动分析计算
    更新频率: 每日16:00自动更新
    用途: 根据三维度评分筛选优质股票
    评分维度: 波动率(0-40) + 股息率(0-30) + 成长性(0-30) = 总分(0-100)
    """
    __tablename__ = "stock_analysis_results"
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="主键ID - 自增")
    stock_code = Column(String, index=True, comment="股票代码 - 6位数字")
    stock_name = Column(String, comment="股票名称 - 中文简称")
    analysis_date = Column(Date, index=True, comment="分析日期 - 数据分析日期")
    
    # 基础数据
    latest_price = Column(Float, comment="最新价 - 分析时的股票价格(元)")
    pe_ratio = Column(Float, comment="市盈率 - 动态市盈率")
    pb_ratio = Column(Float, comment="市净率 - 当前市净率")
    
    # 波动率指标
    volatility_30d = Column(Float, comment="30日波动率 - 最近30个交易日的年化波动率(%)")
    volatility_60d = Column(Float, comment="60日波动率 - 最近60个交易日的年化波动率(%)")
    
    # 财务指标
    dividend_yield = Column(Float, comment="股息率 - 年度分红/当前股价*100(%)")
    roe = Column(Float, comment="ROE净资产收益率 - 净利润/净资产*100(%)")
    profit_growth = Column(Float, comment="利润增长率 - 净利润同比增长率(%)")
    
    # 评分详情
    volatility_score = Column(Integer, comment="波动率评分 - 0-40分,波动越低分数越高")
    dividend_score = Column(Integer, comment="股息率评分 - 0-30分,股息率越高分数越高")
    growth_score = Column(Integer, comment="成长性评分 - 0-30分,ROE越高分数越高")
    total_score = Column(Integer, comment="综合评分 - 总分0-100分")
    
    # 分析结果
    suggestion = Column(String, comment="投资建议 - 强烈推荐/推荐/可以关注/观望/不推荐")
    data_source = Column(String, comment="数据来源 - market/enhanced/mixed")
    
    created_at = Column(DateTime, default=datetime.datetime.now, comment="创建时间 - 分析结果生成时间")


# ============================================================
# 指数成分股表 (新增)
# ============================================================

class IndexConstituent(Base):
    """
    指数成分股表 - 存储各大指数的成分股及权重信息
    
    数据源: 中证指数公司/交易所官网
    更新频率: 季度调整,每季度首月更新
    用途: 
    1. 跟踪指数成分股变化
    2. 分析行业配置权重
    3. 指数增强策略构建
    4. 成分股轮换监控
    
    支持指数:
    - 沪深300 (000300)
    - 中证500 (000905)
    - 上证50 (000016)
    - 创业板指 (399006)
    - 科创50 (000688)
    等主要市场指数
    """
    __tablename__ = "index_constituents"
    
    id = Column(Integer, primary_key=True, autoincrement=True, comment="主键ID - 自增")
    
    # 时间标识
    date = Column(Date, index=True, comment="生效日期 - 成分股调整生效日期,用于历史追溯")
    
    # 指数信息
    index_code = Column(String, index=True, comment="指数代码 - 6位数字,如'000300'表示沪深300")
    index_name = Column(String, comment="指数名称 - 中文名称,如'沪深300'")
    index_name_eng = Column(String, comment="指数英文名称 - 如'CSI 300'")
    
    # 成分股信息
    constituent_code = Column(String, index=True, comment="成份券代码 - 6位股票代码,如'600036'")
    constituent_name = Column(String, comment="成份券名称 - 股票中文简称,如'招商银行'")
    constituent_name_eng = Column(String, comment="成份券英文名称 - 如'China Merchants Bank'")
    
    # 交易所信息
    exchange = Column(String, comment="交易所 - 上交所/深交所,值为'SH'或'SZ'")
    exchange_eng = Column(String, comment="交易所英文名称 - 'Shanghai Stock Exchange'或'Shenzhen Stock Exchange'")
    
    # 权重信息
    weight = Column(Float, comment="权重 - 该成分股在指数中的权重百分比(%),如5.23表示占比5.23%")
    
    # 辅助字段
    industry = Column(String, comment="所属行业 - 成分股所属的申万一级行业")
    market_cap = Column(Float, comment="市值 - 成分股总市值(亿元)")
    
    created_at = Column(DateTime, default=datetime.datetime.now, comment="创建时间 - 数据入库时间")
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now, 
                       comment="更新时间 - 最后修改时间")
    
    # 状态标记
    is_active = Column(Integer, default=1, comment="是否有效 - 1:当前成分股, 0:已调出")


# ============================================================
# 索引和约束说明
# ============================================================

"""
数据库索引设计:

1. 联合索引:
   - (date, code) on daily_market_data
   - (stock_code, date) on historical_data
   - (date, index_code) on index_constituents
   
2. 单字段索引:
   - user_id, stock_code on user_stock_watch
   - code on daily_market_data
   - stock_code on historical_data, dividend_data, stock_analysis_results
   - index_code, constituent_code on index_constituents

3. 唯一约束:
   - (date, code) on daily_market_data (一天一只股票只有一条记录)
   - (stock_code, date) on historical_data (避免重复K线)
   - (date, index_code, constituent_code) on index_constituents (避免重复成分股)
"""


Base.metadata.create_all(bind=engine)

# ============================================================
# 核心数据服务层 - 增强字段映射
# ============================================================
class StockDataService:
    def __init__(self):
        self.target_ut = "fa5fd1943c7b386f172d6893dbfba10b"
        self.target_cookies = {
            "qgqp_b_id": "9fb8c26c0a40e0e20ffd551bb6a52cdf",
            "st_nvi": "4U97b8QAwVvKIFT5nsAGl367a",
            "st_si": "69103863020676",
            "nid18": "03c4e656b6d9f1dfd8b102df6f142ef1",
            "st_sn": "23"
        }
        
        # ✅ 增强版字段映射 - 包含所有可用字段
        self.em_fields_map = {
            # 基础信息
            'f12': 'code',           # 代码
            'f14': 'name',           # 名称
            
            # 价格相关
            'f2': 'latest_price',    # 最新价
            'f3': 'change_pct',      # 涨跌幅
            'f4': 'change_amount',   # 涨跌额
            'f15': 'high',           # 最高
            'f16': 'low',            # 最低
            'f17': 'open',           # 今开
            'f18': 'close_prev',     # 昨收
            
            # 成交相关
            'f5': 'volume',          # 成交量(手)
            'f6': 'amount',          # 成交额(元)
            'f7': 'amplitude',       # 振幅
            'f8': 'turnover_rate',   # 换手率
            'f10': 'volume_ratio',   # 量比
            
            # 估值相关
            'f9': 'pe_dynamic',      # 市盈率-动态
            'f23': 'pb',             # 市净率
            
            # 市值相关
            'f20': 'total_market_cap',        # 总市值
            'f21': 'circulating_market_cap',  # 流通市值
            
            # 其他
            'f11': 'rise_speed',     # 涨速
            'f22': 'change_5min',    # 5分钟涨跌
        }

    def get_db(self) -> Session:
        db = SessionLocal()
        return db

    def _safe_float(self, val):
        """安全转换为浮点数"""
        try:
            if pd.isna(val) or val == '-' or val is None or val == '':
                return None
            # 处理百分比
            if isinstance(val, str) and '%' in val:
                return float(val.replace('%', ''))
            return float(val)
        except:
            return None
    
    def _safe_int(self, val):
        """安全转换为整数"""
        try:
            if pd.isna(val) or val == '-' or val is None or val == '':
                return None
            return int(float(val))
        except:
            return None

    def refresh_ut(self):
        """自动刷新 ut 参数"""
        print("🔄 正在刷新 ut 参数...")
        try:
            url = "https://quote.eastmoney.com/center/gridlist.html"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0 Safari/537.36"
            }
            response = requests.get(url, headers=headers, timeout=10, verify=False, proxies={"http": None, "https": None})
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
        """增强版数据抓取 - 完整字段映射"""
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
        print(f"   字段数量: {len(self.em_fields_map)} 个\n")

        session = requests.Session()
        session.trust_env = False
        session.proxies = {"http": None, "https": None}
        session.cookies.update(self.target_cookies)

        while current_page <= total_pages:
            params = {
                "np": "1",
                "fltt": "1",
                "invt": "2",
                "cb": f"jQuery37109323508735388775_{int(time.time()*1000)}",
                "fs": "m:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2,m:0+t:81+s:262144+f:!2",
                "fields": ",".join(self.em_fields_map.keys()),  # 请求所有字段
                "fid": "f3",
                "pn": str(current_page),
                "pz": str(page_size),
                "po": "1",
                "dect": "1",
                "ut": self.target_ut,
                "wbp2u": "|0|0|0|web",
                "_": str(int(time.time() * 1000))
            }

            try:
                print(f"   ➤ 抓取第 {current_page}/{total_pages if total_pages != 999 else '?'} 页...")

                response = await asyncio.to_thread(
                    session.get, url, params=params, headers=headers, timeout=20, verify=False
                )

                if response.status_code != 200:
                    print(f"   ⚠️ 状态码异常: {response.status_code}")
                    break

                raw_text = response.text
                json_match = re.search(r'jQuery.*?\((.*)\)', raw_text)
                
                if not json_match:
                    print("   ⚠️ JSONP 解析失败")
                    break

                json_str = json_match.group(1)
                res_json = json.loads(json_str)

                if not res_json or not res_json.get("data"):
                    print("⚠️ 数据为空，尝试刷新 ut...")
                    if self.refresh_ut():
                        print("🔁 使用新 ut 重试...")
                        params["ut"] = self.target_ut
                        response = await asyncio.to_thread(
                            session.get, url, params=params, headers=headers, timeout=20, verify=False
                        )
                        raw_text = response.text
                        json_match = re.search(r'jQuery.*?\((.*)\)', raw_text)
                        if json_match:
                            json_str = json_match.group(1)
                            res_json = json.loads(json_str)
                            if not res_json or not res_json.get("data"):
                                print("❌ 刷新后仍失败")
                                break
                        else:
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

                wait_time = random.uniform(10, 50)
                print(f"   💤 随机等待 {wait_time:.1f} 秒...")
                await asyncio.sleep(wait_time)

                current_page += 1

            except Exception as e:
                print(f"   ❌ 第 {current_page} 页失败: {str(e)[:100]}")
                break

        session.close()

        if not all_dfs:
            return pd.DataFrame()

        final_df = pd.concat(all_dfs, ignore_index=True)
        
        # 字段重命名
        final_df = final_df.rename(columns=self.em_fields_map)

        print(f"\n✅ 总计获取 {len(final_df)} 条数据")
        
        # 显示字段完整性统计
        print(f"\n📊 字段完整性统计:")
        for col in ['code', 'name', 'latest_price', 'pe_dynamic', 'pb', 'volume', 'amount', 'turnover_rate', 'amplitude']:
            if col in final_df.columns:
                non_null = final_df[col].notna().sum()
                pct = (non_null / len(final_df)) * 100
                status = "✅" if pct > 90 else ("⚠️" if pct > 50 else "❌")
                print(f"   {status} {col:20s}: {non_null:5d}/{len(final_df)} ({pct:5.1f}%)")

        return final_df

    async def fetch_daily_market_data(self, force: bool = False) -> dict:
        """增强版市场数据获取 - 完整字段保存"""
        db = self.get_db()
        today = datetime.date.today()
        
        try:
            if not force and db.query(DailyMarketData).filter(DailyMarketData.date == today).first():
                db.close()
                return {"status": "skip", "message": "今日数据已存在"}

            # 方案1: 使用增强的Web API
            df = await self.fetch_em_data_via_web_api(page_size=100)
            
            # 方案2: 如果失败，使用 efinance 保底
            if df.empty:
                print("⚠️ Web API 失败，启动 efinance 保底...")
                df = await asyncio.to_thread(ef.stock.get_realtime_quotes)
                # efinance 字段映射
                df = df.rename(columns={
                    '股票代码': 'code',
                    '股票名称': 'name',
                    '最新价': 'latest_price',
                    '涨跌幅': 'change_pct',
                    '涨跌额': 'change_amount',
                    '成交量': 'volume',
                    '成交额': 'amount',
                    '振幅': 'amplitude',
                    '最高': 'high',
                    '最低': 'low',
                    '今开': 'open',
                    '昨收': 'close_prev',
                    '量比': 'volume_ratio',
                    '换手率': 'turnover_rate',
                    '动态市盈率': 'pe_dynamic',
                    '市净率': 'pb',
                    '总市值': 'total_market_cap',
                    '流通市值': 'circulating_market_cap'
                })
            
            if df.empty:
                db.close()
                return {"status": "error", "message": "无法获取行情"}

            # 删除今日旧数据
            db.query(DailyMarketData).filter(DailyMarketData.date == today).delete()
            db.commit()

            print(f"\n💾 存入数据库: {len(df)} 条记录")
            
            batch = []
            field_stats = {}  # 统计各字段的非空数量
            
            for _, row in df.iterrows():
                # 提取代码（优先从映射后的字段获取）
                code = str(row.get('code') or row.get('f12', ''))
                code = re.sub(r'\D', '', code)  # 只保留数字
                
                if not code:
                    continue
                
                # 创建记录 - 使用增强的字段映射
                m = DailyMarketData(
                    date=today,
                    code=code,
                    name=str(row.get('name') or row.get('f14', '')),
                    
                    # 价格相关
                    latest_price=self._safe_float(row.get('latest_price') or row.get('f2')),
                    change_pct=self._safe_float(row.get('change_pct') or row.get('f3')),
                    change_amount=self._safe_float(row.get('change_amount') or row.get('f4')),
                    high=self._safe_float(row.get('high') or row.get('f15')),
                    low=self._safe_float(row.get('low') or row.get('f16')),
                    open=self._safe_float(row.get('open') or row.get('f17')),
                    close_prev=self._safe_float(row.get('close_prev') or row.get('f18')),
                    
                    # 成交相关
                    volume=self._safe_float(row.get('volume') or row.get('f5')),
                    amount=self._safe_float(row.get('amount') or row.get('f6')),
                    amplitude=self._safe_float(row.get('amplitude') or row.get('f7')),
                    turnover_rate=self._safe_float(row.get('turnover_rate') or row.get('f8')),
                    volume_ratio=self._safe_float(row.get('volume_ratio') or row.get('f10')),
                    
                    # 估值相关
                    pe_dynamic=self._safe_float(row.get('pe_dynamic') or row.get('f9')),
                    pb=self._safe_float(row.get('pb') or row.get('f23')),
                    
                    # 市值相关
                    total_market_cap=self._safe_float(row.get('total_market_cap') or row.get('f20')),
                    circulating_market_cap=self._safe_float(row.get('circulating_market_cap') or row.get('f21')),
                    
                    # 其他
                    rise_speed=self._safe_float(row.get('rise_speed') or row.get('f11')),
                    change_5min=self._safe_float(row.get('change_5min') or row.get('f22')),
                    
                    updated_at=datetime.datetime.now()
                )
                
                # 统计字段
                for field in ['latest_price', 'pe_dynamic', 'pb', 'volume', 'amount', 'turnover_rate']:
                    val = getattr(m, field)
                    if val is not None:
                        field_stats[field] = field_stats.get(field, 0) + 1
                
                batch.append(m)
                
                # 批量提交
                if len(batch) >= 500:
                    db.bulk_save_objects(batch)
                    db.commit()
                    batch = []
                    print(f"\n✅ 数据保存完成，共 {saved_count} 条")
            
            # 提交剩余数据
            if batch:
                db.bulk_save_objects(batch)
                db.commit()
                saved_count += len(batch)
            
            # 显示保存统计
            print(f"\n✅ 数据保存完成!")
            print(f"\n📊 字段保存统计:")
            total = len(df)
            for field, count in sorted(field_stats.items()):
                pct = (count / total) * 100
                status = "✅" if pct > 90 else ("⚠️" if pct > 50 else "❌")
                print(f"   {status} {field:20s}: {count:5d}/{total} ({pct:5.1f}%)")
            
            db.close()
            return {"status": "success", "count": len(df), "field_stats": field_stats}
            
        except Exception as e:
            db.close()
            raise e

    async def fetch_financial_metrics(self, stock_code: str):
        """个股财务数据补偿抓取"""
        try:
            df = await asyncio.to_thread(ef.stock.get_base_info, stock_code)
            if df is None or df.empty:
                return 0.0, 0.0
            row = df.iloc[0]
            roe = self._safe_float(row.get('净资产收益率(%)', 0))
            growth = self._safe_float(row.get('净利润同比(%)', 0))
            return roe if roe else 0.0, growth if growth else 0.0
        except:
            return 0.0, 0.0

    async def fetch_historical_data(self, stock_code: str, start_date=None, end_date=None):
        """获取历史数据"""
        db = self.get_db()
        try:
            if not end_date:
                end_date = datetime.date.today().strftime("%Y%m%d")
            if not start_date:
                start_date = (datetime.date.today() - datetime.timedelta(days=180)).strftime("%Y%m%d")
            
            # 优先 efinance
            try:
                df = await asyncio.to_thread(ef.stock.get_quote_history, stock_code)
                if not df.empty:
                    df = df.rename(columns={'日期': 'date', '收盘': 'close', '开盘': 'open', 
                                           '最高': 'high', '最低': 'low', '成交量': 'volume',
                                           '成交额': 'amount', '振幅': 'amplitude', 
                                           '涨跌幅': 'change_pct', '涨跌额': 'change_amount',
                                           '换手率': 'turnover_rate'})
            except:
                df = await asyncio.to_thread(ak.stock_zh_a_hist, symbol=stock_code, 
                                            period="daily", start_date=start_date, 
                                            end_date=end_date, adjust="qfq")
                if not df.empty:
                    df = df.rename(columns={'日期': 'date', '收盘': 'close', '开盘': 'open',
                                           '最高': 'high', '最低': 'low', '成交量': 'volume',
                                           '成交额': 'amount', '振幅': 'amplitude',
                                           '涨跌幅': 'change_pct', '涨跌额': 'change_amount',
                                           '换手率': 'turnover_rate'})

            if df.empty:
                db.close()
                return {"status": "error"}

            # 删除旧数据
            db.query(HistoricalData).filter(HistoricalData.stock_code == stock_code).delete()
            
            # 保存新数据
            for _, row in df.iterrows():
                h = HistoricalData(
                    stock_code=stock_code,
                    date=pd.to_datetime(row['date']).date(),
                    close=self._safe_float(row.get('close')),
                    open=self._safe_float(row.get('open')),
                    high=self._safe_float(row.get('high')),
                    low=self._safe_float(row.get('low')),
                    volume=self._safe_int(row.get('volume')),
                    amount=self._safe_float(row.get('amount')),
                    amplitude=self._safe_float(row.get('amplitude')),
                    change_pct=self._safe_float(row.get('change_pct')),
                    change_amount=self._safe_float(row.get('change_amount')),
                    turnover_rate=self._safe_float(row.get('turnover_rate'))
                )
                db.add(h)
                
            db.commit()
            db.close()
            return {"status": "success", "count": len(df)}
            
        except Exception as e:
            db.close()
            return {"status": "error", "message": str(e)}

    async def fetch_dividend_data(self, date_str: str = None) -> dict:
        """获取分红数据"""
        db = self.get_db()
        try:
            if not date_str:
                date_str = datetime.date.today().strftime("%Y%m%d")
            
            print(f"💰 获取 {date_str} 分红数据...")
            
            df = ak.news_trade_notify_dividend_baidu(date=date_str)
            
            if df.empty:
                db.close()
                return {"status": "success", "message": f"{date_str} 无分红数据", "count": 0}
            
            target_date = datetime.datetime.strptime(date_str, "%Y%m%d").date()
            db.query(DividendData).filter(DividendData.ex_dividend_date == target_date).delete()
            
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
            
            return {"status": "success", "count": count}
            
        except Exception as e:
            db.close()
            return {"status": "error", "message": str(e)}

    async def analyze_stock(self, stock_code: str, db: Session = None) -> dict:
        """分析单只股票"""
        is_internal = db is None
        if is_internal:
            db = self.get_db()
            
        try:
            today = datetime.date.today()
            market_data = db.query(DailyMarketData).filter(
                DailyMarketData.code == stock_code
            ).order_by(desc(DailyMarketData.date)).first()
            
            if not market_data:
                return {"status": "error", "message": "无实时数据"}

            # 1. 获取 ROE 和增长率
            roe, profit_growth = await self.fetch_financial_metrics(stock_code)
            
            # 2. 计算波动率
            vol_30d = 0
            vol_60d = 0
            trend_score = 0
            
            hist = db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code
            ).order_by(desc(HistoricalData.date)).limit(65).all()
            
            if len(hist) >= 30:
                closes = [h.close for h in reversed(hist) if h.close]
                if len(closes) >= 30:
                    series = pd.Series(closes)
                    
                    # 30日波动率
                    series_30 = pd.Series(closes[-30:])
                    log_ret_30 = np.log(series_30 / series_30.shift(1)).dropna()
                    vol_30d = log_ret_30.std() * np.sqrt(252) * 100 if len(log_ret_30) > 0 else 0
                    
                    # 60日波动率
                    if len(closes) >= 60:
                        series_60 = pd.Series(closes[-60:])
                        log_ret_60 = np.log(series_60 / series_60.shift(1)).dropna()
                        vol_60d = log_ret_60.std() * np.sqrt(252) * 100 if len(log_ret_60) > 0 else 0
                        
                        ma60 = series.rolling(60).mean().iloc[-1]
                        trend_score = 10 if closes[-1] > ma60 else 0

            # 3. 股息率计算
            dividend_yield = 0
            one_year_ago = today - datetime.timedelta(days=365)
            dividends = db.query(DividendData).filter(
                DividendData.stock_code == stock_code,
                DividendData.ex_dividend_date >= one_year_ago
            ).all()
            
            if dividends and market_data.latest_price:
                total_dividend = 0
                for div in dividends:
                    div_str = str(div.dividend)
                    match = re.search(r'(\d+\.?\d*)', div_str)
                    if match:
                        total_dividend += float(match.group(1))
                
                if total_dividend > 0:
                    dividend_yield = (total_dividend / market_data.latest_price) * 100

            # 4. 评分系统
            volatility_score = 0
            if vol_30d > 0:
                if vol_30d < 20:
                    volatility_score = 40
                elif vol_30d < 30:
                    volatility_score = 30
                elif vol_30d < 40:
                    volatility_score = 20
                elif vol_30d < 50:
                    volatility_score = 10
            
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
            
            total = volatility_score + dividend_score + growth_score
            
            if total >= 70:
                suggestion = "强烈推荐"
            elif total >= 60:
                suggestion = "推荐"
            elif total >= 50:
                suggestion = "可以关注"
            elif total >= 40:
                suggestion = "观望"
            else:
                suggestion = "不推荐"
            
            res = StockAnalysisResult(
                stock_code=stock_code,
                stock_name=market_data.name,
                analysis_date=today,
                latest_price=market_data.latest_price,
                pe_ratio=market_data.pe_dynamic,
                pb_ratio=market_data.pb,
                volatility_30d=round(vol_30d, 2),
                volatility_60d=round(vol_60d, 2),
                dividend_yield=round(dividend_yield, 2),
                roe=round(roe, 2) if roe else 0,
                profit_growth=round(profit_growth, 2) if profit_growth else 0,
                volatility_score=volatility_score,
                dividend_score=dividend_score,
                growth_score=growth_score,
                total_score=total,
                suggestion=suggestion,
                data_source="enhanced"
            )
            
            db.merge(res)
            db.commit()
            
            return {"status": "success", "score": total, "suggestion": suggestion}
            
        except Exception as e:
            return {"status": "error", "message": str(e)}
        finally:
            if is_internal:
                db.close()

    async def analyze_all_watched_stocks(self):
        """分析所有关注股票"""
        db = self.get_db()
        try:
            watched = db.query(UserStockWatch.stock_code).distinct().all()
            
            print(f"\n📊 开始分析 {len(watched)} 只关注股票...")
            
            success = 0
            failed = 0
            
            for i, row in enumerate(watched, 1):
                code = row[0]
                print(f"[{i}/{len(watched)}] 分析 {code}...")
                
                # 先确保有历史数据
                hist_result = await self.fetch_historical_data(code)
                if hist_result.get("status") == "success":
                    print(f"   ✓ 历史数据: {hist_result.get('count', 0)} 条")
                
                # 分析
                result = await self.analyze_stock(code, db)
                
                if result["status"] == "success":
                    print(f"   ✓ 评分: {result.get('score', 0)} - {result.get('suggestion', '')}")
                    success += 1
                else:
                    print(f"   ✗ {result.get('message', '')}")
                    failed += 1
                    
                await asyncio.sleep(0.5)
                
            print(f"\n✅ 分析完成! 成功: {success}, 失败: {failed}\n")
            
        except Exception as e:
            print(f"❌ 批量分析失败: {str(e)}")
        finally:
            db.close()
    
#    async def send_daily_reports(self):
#        db = self.get_db()
#        """发送每日报告到所有用户"""
#        email_service = EmailService()
#        
#        # 获取所有启用邮件的用户
#        users = db.query(User).filter(
#            User.enable_daily_report == True,
#            User.email_verified == True
#        ).all()
#        
#        for user in users:
#            # 获取用户的分析结果
#            results = get_user_analysis_results(user.user_id)
#            
#            # 生成CSV
#            csv_path = ReportGenerator.generate_user_csv(
#                user.user_id, 
#                results
#            )
#            
#            # 计算摘要
#            summary = ReportGenerator.calculate_summary(results)
#            
#            # 发送邮件
#            success, error = email_service.send_daily_report(
#                user.email,
#                user.nickname,
#                csv_path,
#                summary
#            )
#            
#            # 记录发送状态
#            notification = EmailNotification(
#                user_id=user.user_id,
#                recipient_email=user.email,
#                email_type='daily_report',
#                status='sent' if success else 'failed',
#                error_message=error if not success else None
#            )
#            db.add(notification)
#        
#        db.commit()

# --- FastAPI 应用 ---
stock_service = StockDataService()
scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("\n🚀 正在启动增强版价值分析系统...\n")

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

    # 每日16:05 - 发送邮件报告 (新增)
    # scheduler.add_job(
    #     email_service.send_all_daily_reports,
    #     CronTrigger(hour=18, minute=5),
    #     id="daily_email_reports"
    # )

    # 每日20:00 - 更新持仓盈亏 (新增)
    # scheduler.add_job(
    #     holdings_service.update_all_holdings_profit,
    #     CronTrigger(hour=20, minute=0),
    #     id="update_holdings"
    # )

    scheduler.start()

    print("✅ 定时任务已启动:")
    print("   - 每日15:30获取全市场数据")
    print("   - 每日16:00分析所有关注股票\n")

    yield

    print("\n🛑 正在关闭系统...")
    if scheduler and scheduler.running:
        scheduler.shutdown()
    print("✅ 系统已停止\n")

app = FastAPI(
    title="价值分析系统",
    version="2.3",
    lifespan=lifespan
)

# --- API接口 (保持不变,此处省略重复代码) ---
@app.post("/users/create")
def create_user(user_id: str, username: str):
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
    except HTTPException:
        db.close()
        raise
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/watch/add")
def add_watch_stock(user_id: str, stock_codes: str):
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
    except HTTPException:
        db.close()
        raise
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/watch/remove")
def remove_watch_stock(user_id: str, stock_code: str):
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
    except HTTPException:
        db.close()
        raise
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/watch/list")
def list_watch_stocks(user_id: str):
    db = SessionLocal()
    try:
        watches = db.query(UserStockWatch).filter(UserStockWatch.user_id == user_id).all()
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
    result = await stock_service.fetch_daily_market_data(force=force)
    return result

@app.post("/data/history/fetch")
async def fetch_history_data(stock_code: str, start_date: str = None, end_date: str = None):
    result = await stock_service.fetch_historical_data(stock_code, start_date, end_date)
    return result

@app.post("/data/dividend/fetch")
async def fetch_dividend_data(date_str: str = None):
    result = await stock_service.fetch_dividend_data(date_str)
    return result

@app.post("/analyze/manual")
async def manual_analyze(background_tasks: BackgroundTasks):
    background_tasks.add_task(stock_service.analyze_all_watched_stocks)
    return {"status": "success", "message": "分析任务已在后台启动"}

@app.post("/analyze/stock")
async def analyze_single_stock(stock_code: str):
    result = await stock_service.analyze_stock(stock_code)
    return result

@app.get("/export/global")
def export_global_csv():
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
    db = SessionLocal()
    try:
        watched = db.query(UserStockWatch.stock_code).filter(UserStockWatch.user_id == user_id).all()
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
        
        out_dir = "outputs"
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        
        df = pd.DataFrame(data)
        output_file = os.path.join(out_dir, f"用户{user_id}_股票分析结果.csv")
        df.to_csv(output_file, index=False, encoding="utf_8_sig")
        
        db.close()
        return FileResponse(output_file, filename=f"用户{user_id}_股票分析结果.csv")
        
    except HTTPException:
        db.close()
        raise
    except Exception as e:
        db.close()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/status")
def get_status():
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
            "system": "价值分析系统",
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
    print("🚀 价值分析系统")
    print("="*60)
    print("\n✨ 新增优化:")
    print("  • 完整字段映射 (22个字段)")
    print("  • 实时字段完整性统计")
    print("  • 增强的数据保存逻辑")
    print("  • 改进的异常值处理")
    print("  • 历史数据完整保存")
    print("\n📚 核心功能:")
    print("  ✓ 多用户支持")
    print("  ✓ 三数据源独立存储")
    print("  ✓ 自动定时任务")
    print("  ✓ 智能评分系统")
    print("\n🔗 访问:")
    print("  API文档: http://localhost:8000/docs")
    print("  系统状态: http://localhost:8000/status")
    print("="*60 + "\n")
    
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)