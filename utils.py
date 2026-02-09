"""
工具模块
功能：CSV导出、数据格式化等
"""
import csv
import os
from datetime import datetime
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

OUTPUT_DIR = "output"


def ensure_output_dir():
    """确保输出目录存在"""
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        logger.info(f"创建输出目录: {OUTPUT_DIR}")


def export_to_csv(results: List[Dict], filename: str = None) -> str:
    """
    导出分析结果到CSV文件
    :param results: 分析结果列表
    :param filename: 文件名（可选）
    :return: 文件路径
    """
    try:
        ensure_output_dir()
        
        if filename is None:
            filename = f"stock_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        # CSV表头
        headers = [
            '股票代码',
            '股票名称',
            '分析日期',
            '波动率(%)',
            '股息率(%)',
            'PB百分位',
            'PE百分位',
            '买入信号',
            '分析说明'
        ]
        
        # 写入CSV
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            
            for result in results:
                row = [
                    result.get('code', ''),
                    result.get('name', ''),
                    result.get('date', ''),
                    f"{result.get('volatility', 0):.2f}",
                    f"{result.get('dividend_yield', 0):.2f}",
                    f"{result.get('pb_percentile', 0):.2f}",
                    f"{result.get('pe_percentile', 0):.2f}",
                    '是' if result.get('buy_signal', False) else '否',
                    result.get('reason', '')
                ]
                writer.writerow(row)
        
        logger.info(f"成功导出CSV文件: {filepath}, 共 {len(results)} 条数据")
        return filepath
        
    except Exception as e:
        logger.error(f"导出CSV失败: {str(e)}")
        raise


def format_number(value, decimals: int = 2) -> str:
    """格式化数字"""
    try:
        if value is None:
            return "N/A"
        return f"{float(value):.{decimals}f}"
    except:
        return "N/A"


def format_percentage(value, decimals: int = 2) -> str:
    """格式化百分比"""
    try:
        if value is None:
            return "N/A"
        return f"{float(value):.{decimals}f}%"
    except:
        return "N/A"


def format_date(date_obj, format_str: str = "%Y-%m-%d") -> str:
    """格式化日期"""
    try:
        if isinstance(date_obj, str):
            return date_obj
        return date_obj.strftime(format_str)
    except:
        return str(date_obj)
