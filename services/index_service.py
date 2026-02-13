import datetime
import akshare as ak
import pandas as pd
from sqlalchemy.orm import Session
from core.database import SessionLocal
from models.stock import IndexConstituent

class IndexService:
    def __init__(self):
        # 预设需要跟踪的指数：指数代码 -> 指数名称
        self.tracking_indices = {
            "000300": "沪深300",
            "000016": "上证50",
            "000905": "中证500",
            "399006": "创业板指"
        }

    async def sync_index_constituents(self):
        """同步各大指数成分股"""
        db = SessionLocal()
        print(f"📊 [{datetime.datetime.now()}] 正在同步指数成分股数据...")
        
        try:
            for index_code, index_name in self.tracking_indices.items():
                print(f"   ➤ 抓取 {index_name} ({index_code})...")
                try:
                    # 使用 AkShare 获取指数成分股
                    df = ak.index_stock_cons(symbol=index_code)
                    if df.empty:
                        continue

                    # 将该指数旧记录标记为非活跃或删除
                    db.query(IndexConstituent).filter(IndexConstituent.index_code == index_code).delete()

                    for _, row in df.iterrows():
                        cons = IndexConstituent(
                            index_code=index_code,
                            index_name=index_name,
                            constituent_code=row['品种代码'],
                            constituent_name=row['品种名称'],
                            # 部分接口不提供权重，默认设为 0
                            weight=0.0,
                            updated_at=datetime.datetime.now(),
                            is_active=1
                        )
                        db.add(cons)
                    
                    db.commit()
                    print(f"   ✓ {index_name} 同步成功，共 {len(df)} 条记录")
                except Exception as e:
                    print(f"   ✗ {index_name} 同步失败: {str(e)}")
                    db.rollback()
        finally:
            db.close()

index_service = IndexService()