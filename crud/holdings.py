from sqlalchemy.orm import Session
from models.holdings import UserStockHolding
from datetime import date
from typing import List

def create_holding(db: Session, holding_data: dict):
    """创建持仓记录"""
    db_holding = UserStockHolding(**holding_data)
    db.add(db_holding)
    db.commit()
    db.refresh(db_holding)
    return db_holding

def get_user_holdings(db: Session, user_id: int):
    """获取用户持仓"""
    return db.query(UserStockHolding).filter(
        UserStockHolding.user_id == user_id,
        UserStockHolding.is_active == True
    ).all()

def update_holding(db: Session, holding_id: int, update_data: dict):
    """更新持仓信息"""
    db_holding = db.query(UserStockHolding).filter(
        UserStockHolding.id == holding_id
    ).first()
    
    if db_holding:
        for key, value in update_data.items():
            setattr(db_holding, key, value)
        db.commit()
        db.refresh(db_holding)
    
    return db_holding

def sell_holding(db: Session, holding_id: int, sell_quantity: int, sell_price: float):
    """卖出持仓"""
    db_holding = db.query(UserStockHolding).filter(
        UserStockHolding.id == holding_id
    ).first()
    
    if db_holding and db_holding.current_quantity >= sell_quantity:
        # 更新当前持仓
        db_holding.current_quantity -= sell_quantity
        db_holding.current_price = sell_price
        db_holding.current_value = db_holding.current_quantity * sell_price
        
        # 计算盈亏
        db_holding.profit_loss = db_holding.current_value - db_holding.total_cost
        db_holding.profit_loss_pct = (
            (sell_price - db_holding.cost_price) / db_holding.cost_price * 100
        )
        
        # 如果全部卖出，标记为非活跃
        if db_holding.current_quantity == 0:
            db_holding.is_active = False
            
        db.commit()
        db.refresh(db_holding)
        
    return db_holding