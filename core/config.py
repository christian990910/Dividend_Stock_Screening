import os
from pathlib import Path
from pydantic_settings import BaseSettings
from typing import Optional

# 获取项目根目录 (config.py 的上级目录)
BASE_DIR = Path(__file__).resolve().parent.parent

class Settings(BaseSettings):
    # 数据库配置
    DATABASE_URL: str
     
    # 安全配置
    # 注意：这里不给默认值，强制从 .env 读取，确保加解密一致
    SECRET_KEY: str 
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 1440
    
    # 邮件服务器配置
    SMTP_SERVER: str
    SMTP_PORT: int
    SMTP_USER: str
    SMTP_PASSWORD: str
    SENDER_NAME: str = "价值分析系统"
    
    # 财务数据抓取配置
    FINANCIAL_FETCH_TIMEOUT: int = 20      # 增加超时时间到20秒
    FINANCIAL_RETRY_COUNT: int = 5         # 增加重试次数到5次
    ENABLE_FINANCIAL_FALLBACK: bool = True
    MIN_VALID_FINANCIAL_DATA: float = 0.1
    CACHE_ENABLED: bool = True
    CACHE_TTL_SECONDS: int = 7200          # 增加缓存时间到2小时
    CONCURRENT_LIMIT: int = 2              # 保持并发数2
    QUALITY_THRESHOLD: float = 0.7
    
    # 抓取延迟配置
    FETCH_DELAY_MIN: float = 3.0           # 减少最小延迟
    FETCH_DELAY_MAX: float = 20.0          # 减少最大延迟
    BATCH_SIZE: int = 10                   # 增加批处理大小
    PRIORITY_FIRST: bool = True
    
    # 新增网络稳定性配置
    NETWORK_RETRY_BACKOFF: float = 1.5     # 重试退避因子
    MAX_NETWORK_ERRORS: int = 10           # 最大连续网络错误数
    ADAPTIVE_DELAY_MULTIPLIER: float = 2.0 # 自适应延迟倍数

    class Config:
        # 核心修改：使用绝对路径确保无论从哪里启动都能读到 .env
        env_file = os.path.join(BASE_DIR, ".env")
        env_file_encoding = 'utf-8'
        extra = "ignore"

# 实例化
settings = Settings()

# 调试用：启动时可以在控制台打印一下，确认读到了
# print(f"DEBUG: SECRET_KEY is {settings.SECRET_KEY[:5]}...")