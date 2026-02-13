import os
import smtplib
import datetime
import pandas as pd
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

from core.config import settings
from core.database import SessionLocal
from models.user import User
from models.notification import EmailNotification
from crud.stock import get_analysis_by_user

class EmailService:
    def _generate_csv(self, db, user_id):
        """内部方法：为用户生成CSV附件"""
        results = get_analysis_by_user(db, user_id)
        if not results: return None
        
        data = [{
            "代码": r.stock_code, "名称": r.stock_name, "评分": r.total_score,
            "建议": r.suggestion, "最新价": r.latest_price, "股息率%": r.dividend_yield,
            "波动率%": r.volatility_30d, "PE": r.pe_ratio, "日期": r.analysis_date
        } for r in results]
        
        os.makedirs("outputs", exist_ok=True)
        file_path = f"outputs/report_{user_id}_{datetime.date.today()}.csv"
        pd.DataFrame(data).to_csv(file_path, index=False, encoding="utf_8_sig")
        return file_path

    def _send_single_mail(self, db, user, file_path):
        """执行具体的发送逻辑并记录日志"""
        subject = f"【价值分析】今日股票分析报告 - {user.nickname}"
        html = f"""
        <h3>您好，{user.nickname}：</h3>
        <p>系统已完成您关注股票的每日价值评估。详细评分请见附件 CSV 表格。</p>
        <p><b>今日综述：</b>建议优先关注综合评分在 60 分以上的品种。</p>
        <br><hr>
        <p>此邮件为系统自动发送，请勿回复。</p>
        """
        
        # 准备审计记录
        notif = EmailNotification(
            user_id=user.user_id,
            recipient_email=user.email,
            email_type='daily_report',
            subject=subject,
            status='pending'
        )
        db.add(notif)
        db.commit()

        msg = MIMEMultipart()
        msg['From'] = f"{settings.SENDER_NAME} <{settings.SMTP_USER}>"
        msg['To'] = user.email
        msg['Subject'] = subject
        msg.attach(MIMEText(html, 'html'))

        if file_path:
            with open(file_path, "rb") as f:
                part = MIMEApplication(f.read(), Name=os.path.basename(file_path))
                part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
                msg.attach(part)

        try:
            with smtplib.SMTP_SSL(settings.SMTP_SERVER, settings.SMTP_PORT) as server:
                server.login(settings.SMTP_USER, settings.SMTP_PASSWORD)
                server.send_message(msg)
            
            notif.status = 'sent'
            notif.send_time = datetime.datetime.now()
        except Exception as e:
            notif.status = 'failed'
            notif.error_message = str(e)
        finally:
            db.commit()

    async def send_all_daily_reports(self):
        """定时任务主入口"""
        db = SessionLocal()
        try:
            users = db.query(User).filter(
                User.enable_daily_report == True,
                User.is_active == True,
                User.email_verified == True
            ).all()
            
            print(f"📧 开始推送每日报告，目标用户数: {len(users)}")
            for user in users:
                file_path = self._generate_csv(db, user.user_id)
                self._send_single_mail(db, user, file_path)
                
        finally:
            db.close()

email_service = EmailService()