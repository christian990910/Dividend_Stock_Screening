"""
历史数据补充工具 - Historical Data补充方案
支持批量补充、增量更新、数据验证
"""

import os
import sys
import asyncio
import datetime
import pandas as pd
import akshare as ak
import efinance as ef
from sqlalchemy import create_engine, func
from sqlalchemy.orm import sessionmaker
import time

# 添加主程序路径以导入模型
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入数据库配置
SQLALCHEMY_DATABASE_URL = "sqlite:///./stock_advanced_system.db"
engine = create_engine(SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 导入模型定义
from sqlalchemy import Column, String, Float, Date, Integer, DateTime
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class HistoricalData(Base):
    __tablename__ = "historical_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String, index=True)
    date = Column(Date, index=True)
    open = Column(Float)
    close = Column(Float)
    high = Column(Float)
    low = Column(Float)
    volume = Column(Integer)
    amount = Column(Float)
    amplitude = Column(Float)
    change_pct = Column(Float)
    change_amount = Column(Float)
    turnover_rate = Column(Float)
    created_at = Column(DateTime, default=datetime.datetime.now)

class DailyMarketData(Base):
    __tablename__ = "daily_market_data"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, index=True)
    code = Column(String, index=True)
    name = Column(String)

class UserStockWatch(Base):
    __tablename__ = "user_stock_watch"
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(String, index=True)
    stock_code = Column(String, index=True)

class HistoricalDataSupplementer:
    """历史数据补充器"""
    
    def __init__(self):
        self.db = SessionLocal()
        
    def _safe_float(self, val):
        """安全转换为浮点数"""
        try:
            if pd.isna(val) or val == '-' or val is None or val == '':
                return None
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
    
    def get_all_stock_codes(self):
        """获取所有需要补充历史数据的股票代码"""
        # 方案1: 获取所有市场数据中的股票
        market_codes = self.db.query(DailyMarketData.code).distinct().all()
        market_codes = [c[0] for c in market_codes]
        
        # 方案2: 获取所有用户关注的股票
        watch_codes = self.db.query(UserStockWatch.stock_code).distinct().all()
        watch_codes = [c[0] for c in watch_codes]
        
        # 合并去重
        all_codes = list(set(market_codes + watch_codes))
        
        return all_codes
    
    def check_stock_history_status(self, stock_code):
        """检查股票的历史数据状态"""
        count = self.db.query(HistoricalData).filter(
            HistoricalData.stock_code == stock_code
        ).count()
        
        if count == 0:
            return "无数据", 0, None, None
        
        # 获取日期范围
        min_date = self.db.query(func.min(HistoricalData.date)).filter(
            HistoricalData.stock_code == stock_code
        ).scalar()
        
        max_date = self.db.query(func.max(HistoricalData.date)).filter(
            HistoricalData.stock_code == stock_code
        ).scalar()
        
        return "有数据", count, min_date, max_date
    
    async def fetch_history_efinance(self, stock_code, start_date=None, end_date=None):
        """使用efinance获取历史数据(推荐)"""
        try:
            print(f"   📥 使用 efinance 获取 {stock_code}...")
            
            # efinance获取全部历史数据
            df = await asyncio.to_thread(ef.stock.get_quote_history, stock_code)
            
            if df is None or df.empty:
                return None
            
            # 字段映射
            df = df.rename(columns={
                '日期': 'date',
                '股票代码': 'code',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'change_pct',
                '涨跌额': 'change_amount',
                '换手率': 'turnover_rate'
            })
            
            # 日期过滤
            if start_date or end_date:
                df['date'] = pd.to_datetime(df['date'])
                if start_date:
                    df = df[df['date'] >= pd.to_datetime(start_date)]
                if end_date:
                    df = df[df['date'] <= pd.to_datetime(end_date)]
            
            return df
            
        except Exception as e:
            print(f"   ⚠️ efinance 失败: {str(e)[:50]}")
            return None
    
    async def fetch_history_akshare(self, stock_code, start_date=None, end_date=None):
        """使用akshare获取历史数据(备用)"""
        try:
            print(f"   📥 使用 akshare 获取 {stock_code}...")
            
            if not end_date:
                end_date = datetime.date.today().strftime("%Y%m%d")
            if not start_date:
                # 默认获取3年数据
                start_date = (datetime.date.today() - datetime.timedelta(days=1095)).strftime("%Y%m%d")
            
            df = await asyncio.to_thread(
                ak.stock_zh_a_hist,
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"  # 前复权
            )
            
            if df.empty:
                return None
            
            # 字段映射
            df = df.rename(columns={
                '日期': 'date',
                '股票代码': 'code',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'amount',
                '振幅': 'amplitude',
                '涨跌幅': 'change_pct',
                '涨跌额': 'change_amount',
                '换手率': 'turnover_rate'
            })
            
            return df
            
        except Exception as e:
            print(f"   ⚠️ akshare 失败: {str(e)[:50]}")
            return None
    
    async def supplement_single_stock(self, stock_code, mode="full", start_date=None, end_date=None):
        """
        补充单只股票的历史数据
        
        mode:
        - full: 全量更新(删除旧数据,重新获取)
        - incremental: 增量更新(只补充缺失的日期)
        - append: 追加模式(只添加新数据)
        """
        
        status, count, min_date, max_date = self.check_stock_history_status(stock_code)
        
        print(f"\n{'='*60}")
        print(f"📊 股票: {stock_code}")
        print(f"   当前状态: {status}")
        if count > 0:
            print(f"   数据量: {count} 条")
            print(f"   日期范围: {min_date} 至 {max_date}")
        print(f"   补充模式: {mode}")
        print(f"{'='*60}")
        
        # 获取数据 - 优先efinance,失败则用akshare
        df = await self.fetch_history_efinance(stock_code, start_date, end_date)
        
        if df is None or df.empty:
            df = await self.fetch_history_akshare(stock_code, start_date, end_date)
        
        if df is None or df.empty:
            print(f"   ❌ 无法获取数据")
            return {"status": "error", "message": "无法获取数据"}
        
        print(f"   ✅ 获取到 {len(df)} 条数据")
        
        # 根据模式处理数据
        if mode == "full":
            # 全量模式: 删除旧数据
            deleted = self.db.query(HistoricalData).filter(
                HistoricalData.stock_code == stock_code
            ).delete()
            self.db.commit()
            if deleted > 0:
                print(f"   🗑️ 删除旧数据: {deleted} 条")
        
        elif mode == "incremental":
            # 增量模式: 只补充缺失日期
            if count > 0:
                # 获取已有日期
                existing_dates = self.db.query(HistoricalData.date).filter(
                    HistoricalData.stock_code == stock_code
                ).all()
                existing_dates = set([d[0] for d in existing_dates])
                
                # 过滤已存在的日期
                df['date'] = pd.to_datetime(df['date'])
                df = df[~df['date'].dt.date.isin(existing_dates)]
                
                print(f"   📌 增量补充: {len(df)} 条新数据")
        
        elif mode == "append":
            # 追加模式: 只添加比最新日期更新的数据
            if max_date:
                df['date'] = pd.to_datetime(df['date'])
                df = df[df['date'].dt.date > max_date]
                print(f"   📌 追加模式: {len(df)} 条新数据")
        
        if df.empty:
            print(f"   ℹ️ 无需补充")
            return {"status": "skip", "message": "无需补充"}
        
        # 保存数据
        saved = 0
        for _, row in df.iterrows():
            try:
                hist = HistoricalData(
                    stock_code=stock_code,
                    date=pd.to_datetime(row['date']).date(),
                    open=self._safe_float(row.get('open')),
                    close=self._safe_float(row.get('close')),
                    high=self._safe_float(row.get('high')),
                    low=self._safe_float(row.get('low')),
                    volume=self._safe_int(row.get('volume')),
                    amount=self._safe_float(row.get('amount')),
                    amplitude=self._safe_float(row.get('amplitude')),
                    change_pct=self._safe_float(row.get('change_pct')),
                    change_amount=self._safe_float(row.get('change_amount')),
                    turnover_rate=self._safe_float(row.get('turnover_rate'))
                )
                self.db.add(hist)
                saved += 1
                
                # 每100条提交一次
                if saved % 100 == 0:
                    self.db.commit()
                    
            except Exception as e:
                print(f"   ⚠️ 保存失败: {str(e)[:50]}")
                continue
        
        # 最终提交
        self.db.commit()
        
        print(f"   ✅ 成功保存 {saved} 条数据")
        
        return {
            "status": "success",
            "stock_code": stock_code,
            "saved": saved,
            "mode": mode
        }
    
    async def supplement_batch(self, stock_codes=None, mode="full", max_stocks=None, delay=1):
        """
        批量补充历史数据
        
        stock_codes: 股票代码列表,None表示全部
        mode: 补充模式 full/incremental/append
        max_stocks: 最大处理股票数量
        delay: 每只股票之间的延迟(秒)
        """
        
        if stock_codes is None:
            stock_codes = self.get_all_stock_codes()
        
        if max_stocks:
            stock_codes = stock_codes[:max_stocks]
        
        total = len(stock_codes)
        print(f"\n{'='*80}")
        print(f"🚀 批量补充历史数据")
        print(f"{'='*80}")
        print(f"   股票数量: {total}")
        print(f"   补充模式: {mode}")
        print(f"   延迟设置: {delay}秒/股")
        print(f"{'='*80}\n")
        
        success = 0
        failed = 0
        skipped = 0
        
        for i, code in enumerate(stock_codes, 1):
            print(f"\n[{i}/{total}] 处理 {code}...")
            
            try:
                result = await self.supplement_single_stock(code, mode=mode)
                
                if result["status"] == "success":
                    success += 1
                elif result["status"] == "skip":
                    skipped += 1
                else:
                    failed += 1
                    
            except Exception as e:
                print(f"   ❌ 处理失败: {str(e)[:100]}")
                failed += 1
            
            # 延迟
            if i < total:
                await asyncio.sleep(delay)
        
        print(f"\n{'='*80}")
        print(f"📊 批量补充完成")
        print(f"{'='*80}")
        print(f"   ✅ 成功: {success}")
        print(f"   ⏭️ 跳过: {skipped}")
        print(f"   ❌ 失败: {failed}")
        print(f"   📈 总计: {total}")
        print(f"{'='*80}\n")
        
        return {
            "total": total,
            "success": success,
            "skipped": skipped,
            "failed": failed
        }
    
    def generate_report(self):
        """生成历史数据统计报告"""
        
        print(f"\n{'='*80}")
        print(f"📊 历史数据统计报告")
        print(f"{'='*80}\n")
        
        # 总体统计
        total_records = self.db.query(HistoricalData).count()
        total_stocks = self.db.query(HistoricalData.stock_code).distinct().count()
        
        print(f"【总体统计】")
        print(f"   股票数量: {total_stocks}")
        print(f"   数据总量: {total_records:,} 条")
        print(f"   平均每股: {total_records // total_stocks if total_stocks > 0 else 0} 条")
        
        # 日期范围
        min_date = self.db.query(func.min(HistoricalData.date)).scalar()
        max_date = self.db.query(func.max(HistoricalData.date)).scalar()
        
        print(f"\n【日期范围】")
        print(f"   最早日期: {min_date}")
        print(f"   最新日期: {max_date}")
        
        # 字段完整性
        print(f"\n【字段完整性】")
        
        fields = ['open', 'close', 'high', 'low', 'volume', 'amount', 
                 'amplitude', 'change_pct', 'turnover_rate']
        
        for field in fields:
            count = self.db.query(HistoricalData).filter(
                getattr(HistoricalData, field).isnot(None)
            ).count()
            
            pct = (count / total_records * 100) if total_records > 0 else 0
            status = "✅" if pct > 90 else ("⚠️" if pct > 50 else "❌")
            
            print(f"   {status} {field:15s}: {count:8,}/{total_records:8,} ({pct:5.1f}%)")
        
        # 数据覆盖度排名
        print(f"\n【数据覆盖度 TOP 10】")
        
        result = self.db.query(
            HistoricalData.stock_code,
            func.count(HistoricalData.id).label('count'),
            func.min(HistoricalData.date).label('min_date'),
            func.max(HistoricalData.date).label('max_date')
        ).group_by(HistoricalData.stock_code).order_by(
            func.count(HistoricalData.id).desc()
        ).limit(10).all()
        
        for i, (code, count, min_d, max_d) in enumerate(result, 1):
            print(f"   {i:2d}. {code}: {count:4d} 条 ({min_d} ~ {max_d})")
        
        # 需要补充的股票
        print(f"\n【需要补充数据的股票】")
        
        result = self.db.query(
            HistoricalData.stock_code,
            func.count(HistoricalData.id).label('count')
        ).group_by(HistoricalData.stock_code).having(
            func.count(HistoricalData.id) < 100
        ).order_by(func.count(HistoricalData.id).asc()).limit(10).all()
        
        if result:
            for code, count in result:
                print(f"   ⚠️ {code}: 只有 {count} 条数据")
        else:
            print(f"   ✅ 所有股票数据充足")
        
        print(f"\n{'='*80}\n")
    
    def close(self):
        """关闭数据库连接"""
        self.db.close()


# ============================================================
# 命令行工具
# ============================================================

async def main():
    """主函数"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='历史数据补充工具')
    
    parser.add_argument('action', choices=['report', 'single', 'batch', 'watch', 'all'],
                       help='操作类型')
    
    parser.add_argument('--code', type=str, help='股票代码(single模式)')
    parser.add_argument('--mode', type=str, default='full',
                       choices=['full', 'incremental', 'append'],
                       help='补充模式')
    parser.add_argument('--max', type=int, help='最大处理数量(batch模式)')
    parser.add_argument('--delay', type=float, default=1, help='延迟时间(秒)')
    
    args = parser.parse_args()
    
    supplementer = HistoricalDataSupplementer()
    
    try:
        if args.action == 'report':
            # 生成报告
            supplementer.generate_report()
        
        elif args.action == 'single':
            # 补充单只股票
            if not args.code:
                print("❌ 请使用 --code 指定股票代码")
                return
            
            await supplementer.supplement_single_stock(args.code, mode=args.mode)
        
        elif args.action == 'batch':
            # 批量补充(所有股票)
            await supplementer.supplement_batch(
                mode=args.mode,
                max_stocks=args.max,
                delay=args.delay
            )
        
        elif args.action == 'watch':
            # 只补充用户关注的股票
            db = supplementer.db
            watch_codes = db.query(UserStockWatch.stock_code).distinct().all()
            watch_codes = [c[0] for c in watch_codes]
            
            if not watch_codes:
                print("❌ 没有用户关注的股票")
                return
            
            await supplementer.supplement_batch(
                stock_codes=watch_codes,
                mode=args.mode,
                delay=args.delay
            )
        
        elif args.action == 'all':
            # 补充所有市场股票
            all_codes = supplementer.get_all_stock_codes()
            
            await supplementer.supplement_batch(
                stock_codes=all_codes,
                mode=args.mode,
                max_stocks=args.max,
                delay=args.delay
            )
        
    finally:
        supplementer.close()


if __name__ == "__main__":
    print("""
╔══════════════════════════════════════════════════════════════╗
║          历史数据补充工具 v1.0                               ║
╚══════════════════════════════════════════════════════════════╝

使用示例:

1. 生成统计报告
   python supplement_history.py report

2. 补充单只股票 (全量模式)
   python supplement_history.py single --code 600036

3. 补充单只股票 (增量模式)
   python supplement_history.py single --code 600036 --mode incremental

4. 批量补充所有关注股票
   python supplement_history.py watch

5. 批量补充前10只股票 (测试)
   python supplement_history.py batch --max 10

6. 批量补充所有股票 (慢速,延迟2秒)
   python supplement_history.py all --delay 2

参数说明:
  --mode: full(全量), incremental(增量), append(追加)
  --max: 限制处理数量
  --delay: 每只股票延迟(秒),避免请求过快

""")
    
    asyncio.run(main())
