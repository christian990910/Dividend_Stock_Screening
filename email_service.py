"""
邮件通知服务
支持每日报告自动推送
"""

import os
import smtplib
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from pathlib import Path
from typing import List, Optional
import pandas as pd


class EmailConfig:
    """邮件配置"""
    
    # SMTP服务器配置
    SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
    SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
    
    # 发件人信息
    SENDER_EMAIL = os.getenv('SENDER_EMAIL', 'your-email@gmail.com')
    SENDER_PASSWORD = os.getenv('SENDER_PASSWORD', 'your-app-password')
    SENDER_NAME = os.getenv('SENDER_NAME', '股票价值分析系统')
    
    # 邮件模板配置
    ENABLE_HTML = True
    ATTACHMENT_MAX_SIZE = 10 * 1024 * 1024  # 10MB


class EmailService:
    """邮件发送服务"""
    
    def __init__(self, config: EmailConfig = None):
        self.config = config or EmailConfig()
    
    def send_email(
        self,
        to_email: str,
        subject: str,
        content: str,
        attachment_path: Optional[str] = None,
        is_html: bool = True
    ) -> tuple[bool, str]:
        """
        发送邮件
        
        Args:
            to_email: 收件人邮箱
            subject: 邮件主题
            content: 邮件内容
            attachment_path: 附件路径(可选)
            is_html: 是否HTML格式
            
        Returns:
            (成功标志, 错误信息)
        """
        try:
            # 创建邮件
            msg = MIMEMultipart()
            msg['From'] = f"{self.config.SENDER_NAME} <{self.config.SENDER_EMAIL}>"
            msg['To'] = to_email
            msg['Subject'] = subject
            msg['Date'] = datetime.datetime.now().strftime('%a, %d %b %Y %H:%M:%S %z')
            
            # 添加邮件正文
            content_type = 'html' if is_html else 'plain'
            msg.attach(MIMEText(content, content_type, 'utf-8'))
            
            # 添加附件
            if attachment_path and os.path.exists(attachment_path):
                with open(attachment_path, 'rb') as f:
                    attachment = MIMEApplication(f.read())
                    filename = os.path.basename(attachment_path)
                    attachment.add_header('Content-Disposition', 'attachment', filename=filename)
                    msg.attach(attachment)
            
            # 连接SMTP服务器并发送
            with smtplib.SMTP(self.config.SMTP_SERVER, self.config.SMTP_PORT) as server:
                server.starttls()  # 启用TLS加密
                server.login(self.config.SENDER_EMAIL, self.config.SENDER_PASSWORD)
                server.send_message(msg)
            
            return True, ""
            
        except Exception as e:
            return False, str(e)
    
    def send_daily_report(
        self,
        user_email: str,
        user_nickname: str,
        csv_path: str,
        summary_data: dict
    ) -> tuple[bool, str]:
        """
        发送每日分析报告
        
        Args:
            user_email: 用户邮箱
            user_nickname: 用户昵称
            csv_path: CSV文件路径
            summary_data: 摘要数据字典
            
        Returns:
            (成功标志, 错误信息)
        """
        # 生成邮件主题
        today = datetime.date.today().strftime('%Y年%m月%d日')
        subject = f"【股票价值分析】{today} 每日分析报告"
        
        # 生成HTML内容
        content = self._generate_daily_report_html(user_nickname, summary_data, today)
        
        # 发送邮件
        return self.send_email(
            to_email=user_email,
            subject=subject,
            content=content,
            attachment_path=csv_path,
            is_html=True
        )
    
    def _generate_daily_report_html(
        self,
        nickname: str,
        summary: dict,
        date: str
    ) -> str:
        """生成每日报告HTML内容"""
        
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: 'Microsoft YaHei', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            background-color: white;
            border-radius: 8px;
            padding: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .header {{
            text-align: center;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 20px;
            margin-bottom: 30px;
        }}
        .header h1 {{
            color: #4CAF50;
            margin: 0;
            font-size: 28px;
        }}
        .header p {{
            color: #666;
            margin: 10px 0 0 0;
        }}
        .greeting {{
            font-size: 16px;
            color: #555;
            margin-bottom: 20px;
        }}
        .summary {{
            background-color: #f9f9f9;
            border-left: 4px solid #4CAF50;
            padding: 20px;
            margin: 20px 0;
        }}
        .summary h2 {{
            color: #4CAF50;
            margin-top: 0;
            font-size: 20px;
        }}
        .stat-grid {{
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 15px;
            margin-top: 15px;
        }}
        .stat-item {{
            background-color: white;
            padding: 15px;
            border-radius: 5px;
            border: 1px solid #e0e0e0;
        }}
        .stat-label {{
            color: #888;
            font-size: 14px;
            margin-bottom: 5px;
        }}
        .stat-value {{
            color: #333;
            font-size: 24px;
            font-weight: bold;
        }}
        .stat-value.positive {{
            color: #4CAF50;
        }}
        .stat-value.negative {{
            color: #f44336;
        }}
        .top-stocks {{
            margin: 20px 0;
        }}
        .top-stocks h3 {{
            color: #333;
            font-size: 18px;
            margin-bottom: 15px;
        }}
        .stock-item {{
            background-color: #f9f9f9;
            padding: 12px 15px;
            margin-bottom: 10px;
            border-radius: 5px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        .stock-name {{
            font-weight: bold;
            color: #333;
        }}
        .stock-score {{
            background-color: #4CAF50;
            color: white;
            padding: 4px 12px;
            border-radius: 3px;
            font-size: 14px;
        }}
        .attachment-note {{
            background-color: #fff9e6;
            border: 1px solid #ffe58f;
            border-radius: 5px;
            padding: 15px;
            margin: 20px 0;
        }}
        .attachment-note strong {{
            color: #d48806;
        }}
        .footer {{
            text-align: center;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #e0e0e0;
            color: #888;
            font-size: 14px;
        }}
        .button {{
            display: inline-block;
            background-color: #4CAF50;
            color: white;
            padding: 12px 24px;
            text-decoration: none;
            border-radius: 5px;
            margin: 20px 0;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 股票价值分析系统</h1>
            <p>{date} 每日分析报告</p>
        </div>
        
        <div class="greeting">
            <p>尊敬的 <strong>{nickname}</strong>，您好！</p>
            <p>以下是今日为您生成的股票分析报告摘要：</p>
        </div>
        
        <div class="summary">
            <h2>📈 数据概览</h2>
            <div class="stat-grid">
                <div class="stat-item">
                    <div class="stat-label">关注股票数</div>
                    <div class="stat-value">{summary.get('total_stocks', 0)}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">已分析股票</div>
                    <div class="stat-value positive">{summary.get('analyzed_stocks', 0)}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">强烈推荐</div>
                    <div class="stat-value positive">{summary.get('highly_recommended', 0)}</div>
                </div>
                <div class="stat-item">
                    <div class="stat-label">推荐关注</div>
                    <div class="stat-value">{summary.get('recommended', 0)}</div>
                </div>
            </div>
        </div>
        
        <div class="top-stocks">
            <h3>⭐ 评分最高的股票 (TOP 5)</h3>
            {self._generate_top_stocks_html(summary.get('top_stocks', []))}
        </div>
        
        <div class="attachment-note">
            <strong>📎 附件说明：</strong><br>
            完整的分析报告已以CSV格式附在本邮件中，请下载后使用Excel打开查看详细数据。
        </div>
        
        <div class="footer">
            <p>本报告由股票价值分析系统自动生成</p>
            <p>如需修改邮件接收设置，请登录系统进行配置</p>
            <p style="margin-top: 10px; color: #999; font-size: 12px;">
                <strong>免责声明：</strong>本报告仅供参考，不构成投资建议。投资有风险，决策需谨慎。
            </p>
        </div>
    </div>
</body>
</html>
"""
        return html
    
    def _generate_top_stocks_html(self, top_stocks: List[dict]) -> str:
        """生成TOP股票HTML"""
        if not top_stocks:
            return '<p style="color: #888;">暂无数据</p>'
        
        html = ""
        for stock in top_stocks[:5]:
            html += f"""
            <div class="stock-item">
                <div>
                    <span class="stock-name">{stock.get('name', '')} ({stock.get('code', '')})</span>
                    <span style="color: #888; margin-left: 10px;">{stock.get('suggestion', '')}</span>
                </div>
                <div class="stock-score">{stock.get('score', 0)}分</div>
            </div>
            """
        return html
    
    def send_verification_email(self, to_email: str, nickname: str, verify_token: str) -> tuple[bool, str]:
        """发送邮箱验证邮件"""
        
        subject = "【股票价值分析】邮箱验证"
        
        # 生成验证链接 (实际部署时需要真实域名)
        verify_url = f"http://localhost:8000/api/verify-email?token={verify_token}"
        
        content = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; line-height: 1.6; color: #333; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .button {{ 
            display: inline-block; 
            background-color: #4CAF50; 
            color: white; 
            padding: 12px 24px; 
            text-decoration: none; 
            border-radius: 5px; 
            margin: 20px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h2>欢迎注册股票价值分析系统！</h2>
        <p>尊敬的 <strong>{nickname}</strong>，您好！</p>
        <p>感谢您注册我们的服务。请点击下面的按钮验证您的邮箱：</p>
        <p style="text-align: center;">
            <a href="{verify_url}" class="button">验证邮箱</a>
        </p>
        <p>或复制以下链接到浏览器：</p>
        <p style="background-color: #f5f5f5; padding: 10px; word-break: break-all;">
            {verify_url}
        </p>
        <p style="color: #888; font-size: 14px; margin-top: 20px;">
            如果这不是您的操作，请忽略此邮件。
        </p>
    </div>
</body>
</html>
"""
        
        return self.send_email(to_email, subject, content, is_html=True)


class ReportGenerator:
    """报告生成器"""
    
    @staticmethod
    def generate_user_csv(
        user_id: int,
        analysis_results: List[dict],
        output_dir: str = "outputs/reports"
    ) -> str:
        """
        生成用户CSV报告
        
        Returns:
            CSV文件路径
        """
        # 确保输出目录存在
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # 生成文件名
        today = datetime.date.today().strftime('%Y%m%d')
        filename = f"user_{user_id}_analysis_{today}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # 转换为DataFrame
        df = pd.DataFrame(analysis_results)
        
        # 保存CSV
        df.to_csv(filepath, index=False, encoding='utf_8_sig')
        
        return filepath
    
    @staticmethod
    def calculate_summary(analysis_results: List[dict]) -> dict:
        """计算摘要数据"""
        
        if not analysis_results:
            return {
                'total_stocks': 0,
                'analyzed_stocks': 0,
                'highly_recommended': 0,
                'recommended': 0,
                'top_stocks': []
            }
        
        # 统计推荐等级
        highly_recommended = sum(1 for r in analysis_results if r.get('suggestion') == '强烈推荐')
        recommended = sum(1 for r in analysis_results if r.get('suggestion') in ['推荐', '可以关注'])
        
        # 排序获取TOP股票
        sorted_results = sorted(analysis_results, key=lambda x: x.get('total_score', 0), reverse=True)
        top_stocks = [
            {
                'code': r.get('stock_code'),
                'name': r.get('stock_name'),
                'score': r.get('total_score'),
                'suggestion': r.get('suggestion')
            }
            for r in sorted_results[:5]
        ]
        
        return {
            'total_stocks': len(analysis_results),
            'analyzed_stocks': len(analysis_results),
            'highly_recommended': highly_recommended,
            'recommended': recommended,
            'top_stocks': top_stocks
        }

