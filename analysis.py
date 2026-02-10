"""
股票分析模块
功能：数据获取、波动率计算、估值分析、买入信号判断
"""
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Optional
import logging
import time
from requests.exceptions import ProxyError, ConnectionError, Timeout
import config

logger = logging.getLogger(__name__)

# 请求间隔控制
LAST_REQUEST_TIME = {}
REQUEST_INTERVAL = config.REQUEST_INTERVAL
MAX_RETRIES = config.MAX_RETRIES
RETRY_DELAY = config.RETRY_DELAY


def fetch_stock_data(code: str, adjust: str = "qfq") -> Optional[pd.DataFrame]:
    """
    获取股票数据
    :param code: 股票代码
    :param adjust: 复权类型 qfq-前复权 hfq-后复权
    :return: DataFrame
    """
    try:
        # 控制请求频率
        current_time = time.time()
        if code in LAST_REQUEST_TIME:
            elapsed = current_time - LAST_REQUEST_TIME[code]
            if elapsed < REQUEST_INTERVAL:
                logger.debug(f"股票 {code} 距离上次请求不足30秒，跳过")
                return None
        
        LAST_REQUEST_TIME[code] = current_time
        
        # 获取A股历史数据 - 带重试机制
        logger.info(f"开始获取股票 {code} 的数据...")
        
        for attempt in range(MAX_RETRIES):
            try:
                df = ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date=(datetime.now() - timedelta(days=config.HISTORY_DAYS)).strftime("%Y%m%d"),
                    end_date=datetime.now().strftime("%Y%m%d"),
                    adjust=adjust
                )
                
                if df is not None and not df.empty:
                    logger.info(f"成功获取股票 {code} 数据，共 {len(df)} 条")
                    return df
                else:
                    logger.warning(f"股票 {code} 数据为空")
                    return None
                    
            except (ProxyError, ConnectionError, Timeout) as e:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"获取股票 {code} 数据失败（尝试 {attempt + 1}/{MAX_RETRIES}），{RETRY_DELAY}秒后重试...")
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    logger.error(f"获取股票 {code} 数据失败，已达最大重试次数")
                    return None
            except Exception as e:
                logger.error(f"获取股票 {code} 数据时出现未知错误: {str(e)}")
                return None
        
        return None
            
    except Exception as e:
        logger.error(f"获取股票 {code} 数据失败: {str(e)}")
        return None


def fetch_stock_fundamental(code: str) -> Optional[Dict]:
    """
    获取股票基本面数据（PE、PB、股息率等）
    """
    try:
        # 获取个股信息 - 带重试机制
        for attempt in range(MAX_RETRIES):
            try:
                stock_info = ak.stock_individual_info_em(symbol=code)
                break
            except (ProxyError, ConnectionError, Timeout) as e:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"获取股票 {code} 基本面数据失败（尝试 {attempt + 1}/{MAX_RETRIES}），{RETRY_DELAY}秒后重试...")
                    time.sleep(RETRY_DELAY)
                    continue
                else:
                    logger.error(f"获取股票 {code} 基本面数据失败，已达最大重试次数")
                    return None
        
        result = {}
        if stock_info is not None and not stock_info.empty:
            for _, row in stock_info.iterrows():
                item = row['item']
                value = row['value']
                
                if item == '市盈率-动态':
                    try:
                        result['pe'] = float(value) if value != '-' else None
                    except:
                        result['pe'] = None
                elif item == '市净率':
                    try:
                        result['pb'] = float(value) if value != '-' else None
                    except:
                        result['pb'] = None
                elif item == '股息率':
                    try:
                        # 去除百分号并转换
                        result['dividend_yield'] = float(value.replace('%', '')) if value != '-' else None
                    except:
                        result['dividend_yield'] = None
        
        return result if result else None
        
    except Exception as e:
        logger.error(f"获取股票 {code} 基本面数据失败: {str(e)}")
        return None


def calculate_volatility(df: pd.DataFrame, window: int = 30) -> float:
    """
    计算股价波动率（标准差）
    :param df: 股票数据
    :param window: 计算窗口（天数）
    :return: 波动率（年化）
    """
    try:
        if df is None or df.empty or len(df) < window:
            return 0.0
        
        # 计算日收益率
        df_copy = df.copy()
        df_copy['returns'] = df_copy['收盘'].pct_change()
        
        # 计算最近window天的波动率
        recent_returns = df_copy['returns'].tail(window)
        volatility = recent_returns.std()
        
        # 年化波动率（假设一年250个交易日）
        annual_volatility = volatility * np.sqrt(250)
        
        return round(annual_volatility * 100, 2)  # 转换为百分比
        
    except Exception as e:
        logger.error(f"计算波动率失败: {str(e)}")
        return 0.0


def calculate_percentile(value: float, df: pd.DataFrame, column: str) -> float:
    """
    计算当前值在历史数据中的百分位
    :param value: 当前值
    :param df: 历史数据
    :param column: 列名
    :return: 百分位（0-100）
    """
    try:
        if value is None or df is None or df.empty:
            return 50.0
        
        if column not in df.columns:
            return 50.0
        
        # 过滤掉空值
        valid_data = df[column].dropna()
        
        if len(valid_data) == 0:
            return 50.0
        
        # 计算百分位
        percentile = (valid_data < value).sum() / len(valid_data) * 100
        
        return round(percentile, 2)
        
    except Exception as e:
        logger.error(f"计算百分位失败: {str(e)}")
        return 50.0


def get_pb_pe_from_history(df: pd.DataFrame) -> tuple:
    """
    从历史数据中提取PE和PB（如果存在）
    """
    try:
        # 尝试从最新数据获取
        if '市盈率' in df.columns and '市净率' in df.columns:
            latest = df.iloc[-1]
            pe = latest.get('市盈率', None)
            pb = latest.get('市净率', None)
            return pe, pb
        
        return None, None
    except:
        return None, None


def analyze_stock(code: str, name: str) -> Optional[Dict]:
    """
    分析单个股票
    :param code: 股票代码
    :param name: 股票名称
    :return: 分析结果字典
    """
    try:
        logger.info(f"开始分析股票: {code} - {name}")
        
        # 获取历史数据
        df = fetch_stock_data(code)
        if df is None or df.empty:
            logger.warning(f"股票 {code} 无历史数据")
            return None
        
        # 获取基本面数据
        fundamental = fetch_stock_fundamental(code)
        
        # 计算波动率
        volatility = calculate_volatility(df, window=config.VOLATILITY_WINDOW)
        
        # 获取PE、PB
        pe = None
        pb = None
        dividend_yield = 0.0
        
        if fundamental:
            pe = fundamental.get('pe')
            pb = fundamental.get('pb')
            dividend_yield = fundamental.get('dividend_yield', 0.0)
        
        # 如果基本面数据没有，尝试从历史数据获取
        if pe is None or pb is None:
            hist_pe, hist_pb = get_pb_pe_from_history(df)
            if pe is None:
                pe = hist_pe
            if pb is None:
                pb = hist_pb
        
        # 计算PE、PB百分位（在历史数据中的位置）
        pe_percentile = 50.0
        pb_percentile = 50.0
        
        if pe is not None:
            # 使用过去一年数据计算百分位
            pe_percentile = calculate_percentile(pe, df, '市盈率')
        
        if pb is not None:
            pb_percentile = calculate_percentile(pb, df, '市净率')
        
        # 买入信号判断
        buy_signal, reason = determine_buy_signal(
            volatility=volatility,
            dividend_yield=dividend_yield or 0.0,
            pe_percentile=pe_percentile,
            pb_percentile=pb_percentile,
            pe=pe,
            pb=pb
        )
        
        # 组装结果
        result = {
            'code': code,
            'name': name,
            'date': datetime.now().strftime("%Y-%m-%d"),
            'volatility': volatility,
            'dividend_yield': dividend_yield or 0.0,
            'pb_percentile': pb_percentile,
            'pe_percentile': pe_percentile,
            'buy_signal': buy_signal,
            'reason': reason,
            'pe': pe,
            'pb': pb
        }
        
        logger.info(f"分析完成: {code} - 买入信号: {buy_signal}")
        return result
        
    except Exception as e:
        logger.error(f"分析股票 {code} 失败: {str(e)}")
        return None


def determine_buy_signal(
    volatility: float,
    dividend_yield: float,
    pe_percentile: float,
    pb_percentile: float,
    pe: Optional[float],
    pb: Optional[float]
) -> tuple:
    """
    判断是否适合买入
    条件：
    1. 波动率较低（< 30%）
    2. 股息率较高（> 2%）
    3. PE百分位较低（< 30%，估值低）
    4. PB百分位较低（< 30%，估值低）
    
    :return: (是否买入, 原因说明)
    """
    reasons = []
    score = 0
    
    # 波动率评分（波动越小越好）
    if volatility < 20:
        score += 3
        reasons.append("波动率很低")
    elif volatility < 30:
        score += 2
        reasons.append("波动率较低")
    elif volatility < 40:
        score += 1
    else:
        reasons.append("波动率偏高")
    
    # 股息率评分（股息越高越好）
    if dividend_yield > 4:
        score += 3
        reasons.append("股息率很高")
    elif dividend_yield > 2:
        score += 2
        reasons.append("股息率较高")
    elif dividend_yield > 1:
        score += 1
    else:
        reasons.append("股息率偏低")
    
    # PE百分位评分（百分位越低越好，说明估值低）
    if pe_percentile < 20:
        score += 3
        reasons.append("PE处于历史低位")
    elif pe_percentile < 40:
        score += 2
        reasons.append("PE处于较低水平")
    elif pe_percentile < 60:
        score += 1
    else:
        reasons.append("PE处于较高水平")
    
    # PB百分位评分
    if pb_percentile < 20:
        score += 3
        reasons.append("PB处于历史低位")
    elif pb_percentile < 40:
        score += 2
        reasons.append("PB处于较低水平")
    elif pb_percentile < 60:
        score += 1
    else:
        reasons.append("PB处于较高水平")
    
    # 成长性简单判断（PE和PB都合理）
    if pe is not None and pb is not None:
        if 10 < pe < 30 and 1 < pb < 3:
            score += 1
            reasons.append("估值合理，有成长潜力")
    
    # 判断是否买入（评分 >= 8 分）
    buy_signal = score >= config.BUY_SIGNAL_THRESHOLD
    
    reason_text = "; ".join(reasons) if reasons else "无明显特征"
    reason_text = f"评分: {score}/13 - {reason_text}"
    
    return buy_signal, reason_text
