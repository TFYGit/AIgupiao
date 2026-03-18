"""
邮件发送模块 - Gmail
配置三个环境变量即可：
  EMAIL_SENDER    发件 Gmail 地址
  EMAIL_PASSWORD  Gmail 应用专用密码（App Password）
  EMAIL_RECIPIENT 收件邮箱
"""

import os
import smtplib
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

# Gmail SMTP 固定参数
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT   = 587


def send_report(excel_path: str, themes: list = None) -> bool:
    sender    = os.environ.get("EMAIL_SENDER", "")
    password  = os.environ.get("EMAIL_PASSWORD", "")
    recipient = os.environ.get("EMAIL_RECIPIENT", "")

    if not sender or not password or not recipient:
        print("  [Email] 未配置 EMAIL_SENDER / EMAIL_PASSWORD / EMAIL_RECIPIENT，跳过发送")
        return False

    # 支持多收件人，逗号分隔
    recipients = [r.strip() for r in recipient.split(",") if r.strip()]

    today      = datetime.now().strftime("%Y-%m-%d")
    themes_str = "、".join(themes) if themes else "氢能源、核电、航空航天、算电协同、服务器液冷、太空光伏"

    msg = MIMEMultipart()
    msg["From"]    = sender
    msg["To"]      = ", ".join(recipients)
    msg["Date"]    = formatdate(localtime=True)
    msg["Subject"] = f"A股聪明钱信号报告 {today}"
    msg.attach(MIMEText(
        f"您好，\n\n"
        f"今日（{today}）股票调研报告已生成，请查收附件。\n\n"
        f"关注主题：{themes_str}\n"
        f"生成时间：{datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"本邮件由系统自动发送。",
        "plain", "utf-8"
    ))

    # Excel 附件
    filename = os.path.basename(excel_path)
    with open(excel_path, "rb") as f:
        part = MIMEBase("application",
                        "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{filename}"')
    msg.attach(part)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as srv:
            srv.ehlo()
            srv.starttls()
            srv.login(sender, password)
            srv.sendmail(sender, recipients, msg.as_string())
        print(f"  [Email] ✓ 已发送至 {', '.join(recipients)}  附件: {filename}")
        return True
    except Exception as e:
        print(f"  [Email] ✗ 发送失败: {e}")
        return False
