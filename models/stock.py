from sqlalchemy import Column, Integer, String, Float, DateTime, Date, Text
import datetime
from core.database import Base

class DailyMarketData(Base):
    """
    每日市场数据表
    """
    __tablename__ = "daily_market_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True, 
               comment="主键ID - 自增")
    date = Column(Date, index=True, comment="数据日期 - 交易日期")
    code = Column(String(10), index=True, comment="股票代码 - 6位数字")
    name = Column(String(50), comment="股票名称 - 中文简称")
    
    # 价格相关
    latest_price = Column(Float, comment="最新价 - 当前交易价格(元)")
    change_pct = Column(Float, comment="涨跌幅 - 相对昨收涨跌百分比(%)")
    change_amount = Column(Float, comment="涨跌额 - 相对昨收涨跌金额(元)")
    high = Column(Float, comment="最高价 - 当日最高成交价(元)")
    low = Column(Float, comment="最低价 - 当日最低成交价(元)")
    open = Column(Float, comment="开盘价 - 当日开盘价格(元)")
    close_prev = Column(Float, comment="昨收价 - 前一交易日收盘价(元)")
    
    # 成交相关
    volume = Column(Float, comment="成交量 - 当日成交股票数量(手)")
    amount = Column(Float, comment="成交额 - 当日成交金额总额(元)")
    amplitude = Column(Float, comment="振幅 - (最高-最低)/昨收*100(%)")
    turnover_rate = Column(Float, comment="换手率 - 成交量/流通股本*100(%)")
    volume_ratio = Column(Float, comment="量比 - 当日成交量/近5日平均成交量")
    
    # 估值相关
    pe_dynamic = Column(Float, comment="市盈率-动态 - 股价/最近12个月每股收益")
    pb = Column(Float, comment="市净率 - 股价/每股净资产")
    
    # 市值相关
    total_market_cap = Column(Float, comment="总市值 - 股价*总股本(元)")
    circulating_market_cap = Column(Float, comment="流通市值 - 股价*流通股本(元)")
    
    # 其他指标
    rise_speed = Column(Float, comment="涨速 - 当前涨跌幅变化速度(%/分钟)")
    change_5min = Column(Float, comment="5分钟涨跌 - 最近5分钟涨跌幅(%)")
    
    updated_at = Column(DateTime, default=datetime.datetime.now, 
                       comment="更新时间 - 数据入库时间")

class UserStockWatch(Base):
    """
    用户股票关注表
    """
    __tablename__ = "user_stock_watch"
    
    id = Column(Integer, primary_key=True, autoincrement=True, 
               comment="主键ID - 自增")
    user_id = Column(Integer, index=True, comment="用户ID - 外键关联users表")
    stock_code = Column(String(10), index=True, comment="股票代码 - 6位数字")
    added_at = Column(DateTime, default=datetime.datetime.now, 
                     comment="添加时间 - 用户添加关注的时间")

class StockAnalysisResult(Base):
    """
    股票分析结果表
    """
    __tablename__ = "stock_analysis_results"
    
    id = Column(Integer, primary_key=True, autoincrement=True, 
               comment="主键ID - 自增")
    stock_code = Column(String(10), index=True, comment="股票代码 - 6位数字")
    stock_name = Column(String(50), comment="股票名称 - 中文简称")
    analysis_date = Column(Date, index=True, comment="分析日期 - 数据分析日期")
    
    # 基础数据
    latest_price = Column(Float, comment="最新价 - 分析时股票价格(元)")
    pe_ratio = Column(Float, comment="市盈率 - 动态市盈率")
    pb_ratio = Column(Float, comment="市净率 - 当前市净率")
    
    # 波动率指标
    volatility_30d = Column(Float, comment="30日波动率 - 最近30个交易日年化波动率(%)")
    volatility_60d = Column(Float, comment="60日波动率 - 最近60个交易日年化波动率(%)")
    
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
    suggestion = Column(String(50), comment="投资建议 - 强烈推荐/推荐/可以关注/观望/不推荐")
    data_source = Column(String(20), comment="数据来源 - market/enhanced/mixed")
    
    created_at = Column(DateTime, default=datetime.datetime.now, 
                       comment="创建时间 - 分析结果生成时间")

class HistoricalData(Base):
    """
    历史行情数据表
    """
    __tablename__ = "historical_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True, 
               comment="主键ID - 自增")
    stock_code = Column(String(10), index=True, comment="股票代码 - 6位数字")
    date = Column(Date, index=True, comment="交易日期 - K线日期")
    
    # OHLC数据
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
    
    created_at = Column(DateTime, default=datetime.datetime.now, 
                       comment="创建时间 - 数据入库时间")

class DividendData(Base):
    """
    分红派息数据表
    """
    __tablename__ = "dividend_data"
    
    id = Column(Integer, primary_key=True, autoincrement=True, 
               comment="主键ID - 自增")
    stock_code = Column(String(10), index=True, comment="股票代码 - 6位数字")
    stock_name = Column(String(50), comment="股票名称 - 中文简称")
    ex_dividend_date = Column(Date, index=True, comment="除权除息日 - 分红生效日期")
    
    # 分红方案
    dividend = Column(String(50), comment="现金分红 - 每10股派现金额(元)")
    bonus_share = Column(String(50), comment="送股 - 每10股送股数量")
    capitalization = Column(String(50), comment="转增股本 - 每10股转增数量")
    physical = Column(String(100), comment="实物分配 - 其他形式分配")
    
    # 其他信息
    exchange = Column(String(20), comment="交易所 - 上交所/深交所")
    report_period = Column(String(20), comment="报告期 - 分红对应财报期")
    
    created_at = Column(DateTime, default=datetime.datetime.now, 
                       comment="创建时间 - 数据入库时间")

class IndexConstituent(Base):
    """
    指数成分股表
    """
    __tablename__ = "index_constituents"
    
    id = Column(Integer, primary_key=True, autoincrement=True, 
               comment="主键ID - 自增")
    date = Column(Date, index=True, comment="生效日期 - 成分股调整生效日期")
    
    # 指数信息
    index_code = Column(String(10), index=True, comment="指数代码 - 6位数字")
    index_name = Column(String(50), comment="指数名称 - 中文名称")
    index_name_eng = Column(String(100), comment="指数英文名称")
    
    # 成分股信息
    constituent_code = Column(String(10), index=True, comment="成份券代码 - 6位股票代码")
    constituent_name = Column(String(50), comment="成份券名称 - 股票中文简称")
    constituent_name_eng = Column(String(100), comment="成份券英文名称")
    
    # 交易所信息
    exchange = Column(String(10), comment="交易所 - 上交所/深交所")
    exchange_eng = Column(String(50), comment="交易所英文名称")
    
    # 权重信息
    weight = Column(Float, comment="权重 - 成分股权重百分比(%)")
    
    # 辅助字段
    industry = Column(String(50), comment="所属行业 - 申万一级行业")
    market_cap = Column(Float, comment="市值 - 成分股总市值(亿元)")
    
    created_at = Column(DateTime, default=datetime.datetime.now, 
                       comment="创建时间 - 数据入库时间")
    updated_at = Column(DateTime, default=datetime.datetime.now, 
                       onupdate=datetime.datetime.now, 
                       comment="更新时间 - 最后修改时间")
    is_active = Column(Integer, default=1, comment="是否有效 - 1:当前成分股, 0:已调出")