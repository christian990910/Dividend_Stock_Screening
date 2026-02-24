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
    FINANCIAL_FETCH_TIMEOUT: int = 15      # 财务数据获取超时时间(秒)
    FINANCIAL_RETRY_COUNT: int = 3         # 财务数据重试次数
    ENABLE_FINANCIAL_FALLBACK: bool = True # 是否启用降级策略
    MIN_VALID_FINANCIAL_DATA: float = 0.1  # 最小有效财务数据阈值
    CACHE_ENABLED: bool = True             # 启用数据缓存
    CACHE_TTL_SECONDS: int = 3600          # 缓存有效期(秒)
    CONCURRENT_LIMIT: int = 3              # 最大并发数
    QUALITY_THRESHOLD: float = 0.7         # 数据质量阈值
    
    # 抓取延迟配置
    FETCH_DELAY_MIN: float = 5.0           # 最小抓取间隔(秒)
    FETCH_DELAY_MAX: float = 30.0          # 最大抓取间隔(秒)
    BATCH_SIZE: int = 5                    # 批量处理大小
    PRIORITY_FIRST: bool = True            # 优先处理重要股票

    class Config:
        # 核心修改：使用绝对路径确保无论从哪里启动都能读到 .env
        env_file = os.path.join(BASE_DIR, ".env")
        env_file_encoding = 'utf-8'
        extra = "ignore"

# 实例化
settings = Settings()

# 调试用：启动时可以在控制台打印一下，确认读到了
# print(f"DEBUG: SECRET_KEY is {settings.SECRET_KEY[:5]}...")