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

    class Config:
        # 核心修改：使用绝对路径确保无论从哪里启动都能读到 .env
        env_file = os.path.join(BASE_DIR, ".env")
        env_file_encoding = 'utf-8'
        extra = "ignore"

# 实例化
settings = Settings()

# 调试用：启动时可以在控制台打印一下，确认读到了
# print(f"DEBUG: SECRET_KEY is {settings.SECRET_KEY[:5]}...")