"""
配置文件 - 集中管理系统参数
"""

# ==================== 数据库配置 ====================
DB_PATH = "stock_analysis.db"

# ==================== 定时任务配置 ====================
# 每日分析任务执行时间
DAILY_ANALYSIS_HOUR = 9
DAILY_ANALYSIS_MINUTE = 0

# 数据获取任务间隔（秒）
DATA_FETCH_INTERVAL = 60  # 改为60秒，避免频繁请求

# ==================== 数据获取配置 ====================
# 单个股票请求间隔（秒）
REQUEST_INTERVAL = 30

# 最大重试次数
MAX_RETRIES = 3

# 重试延迟（秒）
RETRY_DELAY = 2

# 历史数据获取天数
HISTORY_DAYS = 365

# ==================== 分析配置 ====================
# 波动率计算窗口（天）
VOLATILITY_WINDOW = 60

# 买入信号触发分数
BUY_SIGNAL_THRESHOLD = 8

# ==================== 输出配置 ====================
# 输出目录
OUTPUT_DIR = "output"

# 日志级别 (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL = "INFO"

# ==================== API配置 ====================
# 服务器配置
HOST = "0.0.0.0"
PORT = 8000
RELOAD = True

# ==================== 波动率评分标准 ====================
VOLATILITY_SCORE = {
    'very_low': {'threshold': 20, 'score': 3, 'desc': '波动率很低'},
    'low': {'threshold': 30, 'score': 2, 'desc': '波动率较低'},
    'medium': {'threshold': 40, 'score': 1, 'desc': '波动率适中'},
    'high': {'threshold': float('inf'), 'score': 0, 'desc': '波动率偏高'}
}

# ==================== 股息率评分标准 ====================
DIVIDEND_SCORE = {
    'very_high': {'threshold': 4, 'score': 3, 'desc': '股息率很高'},
    'high': {'threshold': 2, 'score': 2, 'desc': '股息率较高'},
    'medium': {'threshold': 1, 'score': 1, 'desc': '股息率适中'},
    'low': {'threshold': 0, 'score': 0, 'desc': '股息率偏低'}
}

# ==================== PE/PB百分位评分标准 ====================
PERCENTILE_SCORE = {
    'very_low': {'threshold': 20, 'score': 3, 'desc': '历史低位'},
    'low': {'threshold': 40, 'score': 2, 'desc': '较低水平'},
    'medium': {'threshold': 60, 'score': 1, 'desc': '适中水平'},
    'high': {'threshold': 100, 'score': 0, 'desc': '较高水平'}
}

# ==================== 成长性判断标准 ====================
GROWTH_PE_MIN = 10
GROWTH_PE_MAX = 30
GROWTH_PB_MIN = 1
GROWTH_PB_MAX = 3
GROWTH_SCORE = 1

# ==================== 网络配置 ====================
# 是否使用代理
USE_PROXY = False

# 代理配置（如果需要）
PROXY_HTTP = None
PROXY_HTTPS = None

# 请求超时（秒）
REQUEST_TIMEOUT = 30
