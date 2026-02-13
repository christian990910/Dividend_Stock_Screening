from sqlalchemy.orm import Session
from models.holdings import UserStockHolding
from schemas.holdings import HoldingCreate
import datetime

def get_user_holdings(db: Session, user_id: int, only_active: bool = True):
    query = db.query(UserStockHolding).filter(UserStockHolding.user_id == user_id)
    if only_active:
        query = query.filter(UserStockHolding.is_active == True)
    return query.all()

def create_holding_record(db: Session, user_id: int, h: HoldingCreate):
    """买入股票记录"""
    # 计算财务指标
    total_amount = h.purchase_quantity * h.purchase_price
    total_cost = total_amount + h.commission
    cost_price = total_cost / h.purchase_quantity
    
    db_holding = UserStockHolding(
        user_id=user_id,
        stock_code=h.stock_code,
        stock_name=h.stock_name,
        purchase_quantity=h.purchase_quantity,
        purchase_price=h.purchase_price,
        purchase_amount=total_amount,
        purchase_date=h.purchase_date or datetime.date.today(),
        commission=h.commission,
        total_cost=total_cost,
        cost_price=cost_price,
        current_quantity=h.purchase_quantity,
        current_price=h.purchase_price, # 初始当前价为买入价
        current_value=total_amount,
        profit_loss=0.0,
        profit_loss_pct=0.0,
        trade_type='buy',
        is_active=True
    )
    db.add(db_holding)
    db.commit()
    db.refresh(db_holding)
    return db_holding

def sell_holding(db: Session, holding_id: int, sell_price: float):
    """清仓处理"""
    holding = db.query(UserStockHolding).filter(UserStockHolding.id == holding_id).first()
    if holding:
        holding.is_active = False
        holding.current_price = sell_price
        holding.current_value = 0
        holding.current_quantity = 0
        holding.trade_type = 'sell'
        # 计算最终盈亏
        final_return = (sell_price * holding.purchase_quantity) - holding.total_cost
        holding.profit_loss = final_return
        holding.profit_loss_pct = (final_return / holding.total_cost) * 100
        db.commit()
        return True
    return False

def update_holding_price(db: Session, holding_id: int, latest_price: float):
    """更新单条持仓的实时盈亏（被定时任务调用）"""
    holding = db.query(UserStockHolding).filter(UserStockHolding.id == holding_id).first()
    if holding and holding.is_active:
        holding.current_price = latest_price
        holding.current_value = holding.current_quantity * latest_price
        holding.profit_loss = holding.current_value - holding.total_cost
        holding.profit_loss_pct = (holding.profit_loss / holding.total_cost) * 100
        # 此时不 commit，由 service 层统一 commit 提高效率