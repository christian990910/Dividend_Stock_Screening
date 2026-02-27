from pydantic import BaseModel, Field, ConfigDict
from typing import Optional
from datetime import date, datetime

class HoldingBase(BaseModel):
    """持仓基础模型"""
    stock_code: str = Field(..., min_length=6, max_length=10, description="股票代码")
    stock_name: str = Field(..., max_length=50, description="股票名称")
    purchase_quantity: int = Field(..., gt=0, description="购买数量")
    purchase_price: float = Field(..., gt=0, description="购买单价")
    purchase_date: date = Field(..., description="购买日期")

class HoldingCreate(HoldingBase):
    """创建持仓模型"""
    pass

class HoldingUpdate(BaseModel):
    """更新持仓模型"""
    current_price: Optional[float] = Field(None, gt=0, description="当前价格")
    current_quantity: Optional[int] = Field(None, ge=0, description="当前持有数量")
    is_active: Optional[bool] = Field(None, description="是否持有")

class HoldingOut(HoldingBase):
    """持仓输出模型"""
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    user_id: int
    purchase_amount: Optional[float]
    commission: float = 0
    total_cost: Optional[float]
    cost_price: Optional[float]
    current_quantity: Optional[int]
    current_price: Optional[float]
    current_value: Optional[float]
    profit_loss: Optional[float]
    profit_loss_pct: Optional[float]
    trade_type: str = "buy"
    trade_note: Optional[str]
    is_active: bool = True
    created_at: datetime
    updated_at: datetime