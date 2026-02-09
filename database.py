"""
数据库操作模块 - SQLite
包含：股票基础信息、每日分析数据、历史数据存储
"""
import sqlite3
from typing import List, Dict, Optional
from datetime import datetime
import pandas as pd
import json
import logging

logger = logging.getLogger(__name__)

DB_PATH = "stock_analysis.db"


def get_connection():
    """获取数据库连接"""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """初始化数据库表"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # 股票基础信息表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS stock_list (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            added_date TEXT NOT NULL,
            status TEXT DEFAULT 'active'
        )
    """)
    
    # 每日分析数据明细表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            name TEXT NOT NULL,
            analysis_date TEXT NOT NULL,
            volatility REAL,
            dividend_yield REAL,
            pb_percentile REAL,
            pe_percentile REAL,
            buy_signal INTEGER,
            reason TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(code, analysis_date)
        )
    """)
    
    # 股票历史数据存储表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS historical_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            amount REAL,
            pe REAL,
            pb REAL,
            dividend_yield REAL,
            created_at TEXT NOT NULL,
            UNIQUE(code, trade_date)
        )
    """)
    
    # 创建索引
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_daily_analysis_code_date 
        ON daily_analysis(code, analysis_date)
    """)
    
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_historical_data_code_date 
        ON historical_data(code, trade_date)
    """)
    
    conn.commit()
    conn.close()
    logger.info("数据库初始化完成")


def add_stock_to_list(code: str, name: str) -> bool:
    """添加股票到分析列表"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO stock_list (code, name, added_date)
            VALUES (?, ?, ?)
        """, (code, name, datetime.now().strftime("%Y-%m-%d")))
        
        conn.commit()
        conn.close()
        return True
    except sqlite3.IntegrityError:
        logger.warning(f"股票 {code} 已存在")
        return False
    except Exception as e:
        logger.error(f"添加股票失败: {str(e)}")
        return False


def get_all_stocks() -> List[Dict]:
    """获取所有活跃的股票列表"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT code, name, added_date, status
            FROM stock_list
            WHERE status = 'active'
            ORDER BY added_date DESC
        """)
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"获取股票列表失败: {str(e)}")
        return []


def remove_stock_from_list(code: str) -> bool:
    """从分析列表中删除股票（软删除）"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE stock_list
            SET status = 'deleted'
            WHERE code = ?
        """, (code,))
        
        affected = cursor.rowcount
        conn.commit()
        conn.close()
        
        return affected > 0
    except Exception as e:
        logger.error(f"删除股票失败: {str(e)}")
        return False


def save_analysis_result(result: Dict):
    """保存分析结果"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO daily_analysis 
            (code, name, analysis_date, volatility, dividend_yield, 
             pb_percentile, pe_percentile, buy_signal, reason, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            result['code'],
            result['name'],
            result['date'],
            result['volatility'],
            result['dividend_yield'],
            result['pb_percentile'],
            result['pe_percentile'],
            1 if result['buy_signal'] else 0,
            result['reason'],
            datetime.now().isoformat()
        ))
        
        conn.commit()
        conn.close()
        logger.info(f"保存分析结果: {result['code']} - {result['date']}")
    except Exception as e:
        logger.error(f"保存分析结果失败: {str(e)}")


def get_latest_analysis(limit: int = 100) -> List[Dict]:
    """获取最新的分析结果"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT code, name, analysis_date, volatility, dividend_yield,
                   pb_percentile, pe_percentile, buy_signal, reason, created_at
            FROM daily_analysis
            ORDER BY analysis_date DESC, created_at DESC
            LIMIT ?
        """, (limit,))
        
        rows = cursor.fetchall()
        conn.close()
        
        results = []
        for row in rows:
            results.append({
                'code': row['code'],
                'name': row['name'],
                'date': row['analysis_date'],
                'volatility': row['volatility'],
                'dividend_yield': row['dividend_yield'],
                'pb_percentile': row['pb_percentile'],
                'pe_percentile': row['pe_percentile'],
                'buy_signal': bool(row['buy_signal']),
                'reason': row['reason']
            })
        
        return results
    except Exception as e:
        logger.error(f"获取分析结果失败: {str(e)}")
        return []


def save_historical_data(code: str, df: pd.DataFrame):
    """保存股票历史数据"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT OR REPLACE INTO historical_data
                    (code, trade_date, open, high, low, close, volume, amount,
                     pe, pb, dividend_yield, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    code,
                    row.get('date', row.get('日期', '')),
                    row.get('open', row.get('开盘', 0)),
                    row.get('high', row.get('最高', 0)),
                    row.get('low', row.get('最低', 0)),
                    row.get('close', row.get('收盘', 0)),
                    row.get('volume', row.get('成交量', 0)),
                    row.get('amount', row.get('成交额', 0)),
                    row.get('pe', row.get('市盈率', None)),
                    row.get('pb', row.get('市净率', None)),
                    row.get('dividend_yield', row.get('股息率', None)),
                    datetime.now().isoformat()
                ))
            except Exception as e:
                logger.warning(f"保存单条历史数据失败: {str(e)}")
                continue
        
        conn.commit()
        conn.close()
        logger.info(f"保存历史数据: {code}, 共 {len(df)} 条")
    except Exception as e:
        logger.error(f"保存历史数据失败: {str(e)}")


def get_historical_data(code: str, days: int = 250) -> pd.DataFrame:
    """获取股票历史数据"""
    try:
        conn = get_connection()
        
        query = f"""
            SELECT trade_date, open, high, low, close, volume, amount,
                   pe, pb, dividend_yield
            FROM historical_data
            WHERE code = ?
            ORDER BY trade_date DESC
            LIMIT ?
        """
        
        df = pd.read_sql_query(query, conn, params=(code, days))
        conn.close()
        
        return df
    except Exception as e:
        logger.error(f"获取历史数据失败: {str(e)}")
        return pd.DataFrame()


def get_analysis_history(code: str, days: int = 30) -> List[Dict]:
    """获取股票的历史分析记录"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT analysis_date, volatility, dividend_yield,
                   pb_percentile, pe_percentile, buy_signal, reason
            FROM daily_analysis
            WHERE code = ?
            ORDER BY analysis_date DESC
            LIMIT ?
        """, (code, days))
        
        rows = cursor.fetchall()
        conn.close()
        
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"获取分析历史失败: {str(e)}")
        return []
