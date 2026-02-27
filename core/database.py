# database.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import QueuePool

# ==========================
# 加载环境变量
# ==========================
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
SSL_CA = os.getenv("SSL_CA", "./isrgrootx1.pem")

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL 未设置，请检查 .env 文件")

#print("✅ 当前数据库：", DATABASE_URL)

# ==========================
# 创建 Engine
# ==========================
if DATABASE_URL.startswith("mysql"):

    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_size=10,           # 基础连接池大小
        max_overflow=20,        # 最大溢出连接
        pool_pre_ping=True,     # 自动检测失效连接
        pool_recycle=3600,      # 1小时回收（防止云端断连）
        echo=False,             # 生产环境建议 False
        connect_args={
            "ssl": {
                "ca": SSL_CA
            },
            "ssl_verify_cert": True,
            "ssl_verify_identity": True,
            "charset": "utf8mb4"
        }
    )

else:
    # 如果你真的想保留 SQLite 备用
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=False
    )

# ==========================
# Session & Base
# ==========================
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

Base = declarative_base()

# ==========================
# FastAPI 依赖
# ==========================
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ==========================
# 启动时测试连接
# ==========================
def test_connection():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            print("✅ 数据库连接成功:", result.scalar())

            # 检查是否启用 SSL
            ssl_check = conn.execute(text("SHOW STATUS LIKE 'Ssl_cipher'"))
            print("🔐 SSL 状态:", ssl_check.fetchall())

    except Exception as e:
        print("❌ 数据库连接失败:", e)
        raise

# 如果你希望启动时自动检测
if __name__ == "__main__":
    test_connection()