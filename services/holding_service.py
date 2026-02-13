from sqlalchemy.orm import Session
from core.database import SessionLocal
from models.holdings import UserStockHolding
from models.stock import DailyMarketData
import datetime

class HoldingService:
    def update_all_holdings_profit(self, db: Session):
        """更新所有活跃持仓的盈亏状态"""
        print(f"📈 [{datetime.datetime.now()}] 启动持仓盈亏重估...")
        
        # 获取所有持有中的股票
        active_holdings = db.query(UserStockHolding).filter(UserStockHolding.is_active == True).all()
        
        for hold in active_holdings:
            # 获取该股票最新价 (最新的 DailyMarketData 记录)
            latest_price_rec = db.query(DailyMarketData).filter(
                DailyMarketData.code == hold.stock_code
            ).order_by(DailyMarketData.date.desc()).first()
            
            if latest_price_rec and latest_price_rec.latest_price:
                curr_price = latest_price_rec.latest_price
                
                # 重新计算各字段
                hold.current_price = curr_price
                hold.current_value = hold.current_quantity * curr_price
                hold.profit_loss = hold.current_value - hold.total_cost
                
                if hold.total_cost > 0:
                    hold.profit_loss_pct = (hold.profit_loss / hold.total_cost) * 100
                
                hold.updated_at = datetime.datetime.now()
        
        try:
            db.commit()
            print(f"✅ 成功更新 {len(active_holdings)} 条持仓记录")
        except Exception as e:
            db.rollback()
            print(f"❌ 持仓更新失败: {str(e)}")

holding_service = HoldingService()