import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime
from core.database import Base


class User(Base):
    """
    用户表 - 存储系统用户信息
    
    用途: 管理系统用户,实现多用户隔离
    """
    __tablename__ = "users"
    
    # 系统字段
    user_id = Column(Integer, primary_key=True, autoincrement=True, comment="用户ID - 系统自增")
    
    # 登录信息
    account = Column(String, unique=True, nullable=False, comment="登录账号 - 用户名,唯一,用于登录")
    nickname = Column(String, nullable=False, comment="用户昵称 - 显示名称")
    password_hash = Column(String, nullable=False, comment="密码哈希 - 使用bcrypt加密存储")
    
    # 通知设置
    email = Column(String, nullable=False, comment="通知邮箱 - 用于接收分析报告")
    email_verified = Column(Boolean, default=False, comment="邮箱验证状态 - True:已验证, False:未验证")
    enable_daily_report = Column(Boolean, default=True, comment="启用每日报告 - True:发送, False:不发送")
    
    # 其他信息
    avatar_url = Column(String, comment="头像URL - 用户头像地址")
    phone = Column(String, comment="手机号 - 可选")
    
    # 状态信息
    is_active = Column(Boolean, default=True, comment="账号状态 - True:正常, False:禁用")
    last_login_at = Column(DateTime, comment="最后登录时间")
    
    # 时间戳
    created_at = Column(DateTime, default=datetime.datetime.now, comment="注册时间")
    updated_at = Column(DateTime, default=datetime.datetime.now, onupdate=datetime.datetime.now, comment="更新时间")
