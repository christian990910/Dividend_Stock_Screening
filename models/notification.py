from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean
import datetime
from core.database import Base

class EmailNotification(Base):
    """
    邮件通知记录表
    """
    __tablename__ = "email_notifications"
    
    id = Column(Integer, primary_key=True, autoincrement=True, 
               comment="主键ID - 自增")
    
    # 收件信息
    user_id = Column(Integer, index=True, nullable=False, comment="用户ID")
    recipient_email = Column(String(255), nullable=False, comment="收件人邮箱")
    
    # 邮件内容
    email_type = Column(String(50), nullable=False, 
                       comment="邮件类型 - daily_report:每日报告, verify:验证邮件, alert:预警")
    subject = Column(String(255), nullable=False, comment="邮件主题")
    content = Column(Text, comment="邮件内容 - HTML格式")
    
    # 附件信息
    has_attachment = Column(Boolean, default=False, comment="是否有附件")
    attachment_path = Column(String(500), comment="附件路径 - CSV文件路径")
    attachment_name = Column(String(255), comment="附件名称 - 显示的文件名")
    
    # 发送状态
    status = Column(String(20), default='pending', 
                   comment="发送状态 - pending:待发送, sent:已发送, failed:失败")
    send_time = Column(DateTime, comment="发送时间 - 实际发送时间")
    error_message = Column(Text, comment="错误信息 - 发送失败时的错误详情")
    retry_count = Column(Integer, default=0, comment="重试次数")
    
    # 时间戳
    created_at = Column(DateTime, default=datetime.datetime.now, 
                       comment="创建时间")
    updated_at = Column(DateTime, default=datetime.datetime.now, 
                       onupdate=datetime.datetime.now, 
                       comment="更新时间")