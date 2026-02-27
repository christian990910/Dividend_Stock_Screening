from sqlalchemy import Column, Integer, String, Float, DateTime, Date, Text, Boolean
import datetime
from core.database import Base

class UserStockHolding(Base):
    """
    用户持仓表
    """
    __tablename__ = "user_stock_holdings"
    
    id = Column(Integer, primary_key=True, autoincrement=True, 
               comment="主键ID - 自增")
    
    # 关联信息
    user_id = Column(Integer, index=True, nullable=False, 
                    comment="用户ID - 外键关联users.user_id")
    stock_code = Column(String(10), index=True, nullable=False, 
                       comment="股票代码 - 6位数字")
    stock_name = Column(String(50), comment="股票名称 - 冗余字段,方便查询")
    
    # 购买信息
    purchase_quantity = Column(Integer, nullable=False, 
                              comment="购买数量 - 股数(股)")
    purchase_price = Column(Float, nullable=False, 
                           comment="购买单价 - 买入价格(元/股)")
    purchase_amount = Column(Float, comment="购买金额 - 数量*单价(元)")
    purchase_date = Column(Date, nullable=False, 
                          comment="购买日期 - 实际买入日期")
    
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
    trade_type = Column(String(20), default='buy', 
                       comment="交易类型 - buy:买入, sell:卖出, dividend:分红")
    trade_note = Column(Text, comment="交易备注 - 用户自定义备注")
    
    # 状态标记
    is_active = Column(Boolean, default=True, 
                      comment="是否持有 - True:持有中, False:已卖出")
    
    # 时间戳
    created_at = Column(DateTime, default=datetime.datetime.now, 
                       comment="创建时间")
    updated_at = Column(DateTime, default=datetime.datetime.now, 
                       onupdate=datetime.datetime.now, 
                       comment="更新时间")