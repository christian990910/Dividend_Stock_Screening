from pydantic import BaseModel
from datetime import date
from typing import Optional

class HoldingCreate(BaseModel):
    stock_code: str
    stock_name: str
    purchase_quantity: int
    purchase_price: float
    purchase_date: Optional[date] = None
    commission: float = 5.0

class HoldingOut(BaseModel):
    id: int
    stock_code: str
    stock_name: Optional[str]
    current_price: Optional[float]
    profit_loss: Optional[float]
    profit_loss_pct: Optional[float]
    is_active: bool

    class Config:
        from_attributes = True