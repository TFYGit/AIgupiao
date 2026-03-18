"""
邮件发送模块
支持 Gmail / QQ邮箱 / 163邮箱 / 任意 SMTP 服务器
"""

import os
import smtplib
import configparser
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")


def load_config(path: str = CONFIG_PATH) -> dict:
    """读取邮件配置，优先使用环境变量（适配 GitHub Actions）"""
    cfg = {
        "smtp_server":  os.environ.get("SMTP_SERVER", ""),
        "smtp_port":    int(os.environ.get("SMTP_PORT", "465")),
        "use_ssl":      os.environ.get("SMTP_SSL", "true").lower() == "true",
        "sender":       os.environ.get("EMAIL_SENDER", ""),
        "password":     os.environ.get("EMAIL_PASSWORD", ""),
        "recipient":    os.environ.get("EMAIL_RECIPIENT", ""),
        "subject_prefix": os.environ.get("EMAIL_SUBJECT_PREFIX", "A股聪明钱信号报告"),
    }

    # 如果环境变量未设置，则从 config.ini 读取
    if not cfg["sender"] and os.path.exists(path):
        ini = configparser.ConfigParser()
        ini.read(path, encoding="utf-8")
        sec = ini["email"] if "email" in ini else {}
        cfg["smtp_server"]    = sec.get("smtp_server",    cfg["smtp_server"])
        cfg["smtp_port"]      = int(sec.get("smtp_port",  str(cfg["smtp_port"])))
        cfg["use_ssl"]        = sec.get("use_ssl",        "true").lower() == "true"
        cfg["sender"]         = sec.get("sender",         cfg["sender"])
        cfg["password"]       = sec.get("password",       cfg["password"])
        cfg["recipient"]      = sec.get("recipient",      cfg["recipient"])
        cfg["subject_prefix"] = sec.get("subject_prefix", cfg["subject_prefix"])

    return cfg


def send_report(excel_path: str, themes: list = None) -> bool:
    """
    发送报告邮件，Excel 作为附件。
    返回 True 表示发送成功。
    """
    cfg = load_config()

    if not cfg["sender"] or not cfg["password"] or not cfg["recipient"]:
        print("  [Email] ⚠ 未配置邮件信息，跳过发送。请编辑 config.ini")
        return False

    today = datetime.now().strftime("%Y-%m-%d")
    subject = f"{cfg['subject_prefix']} {today}"
    themes_str = "、".join(themes) if themes else "氢能源、核电、航空航天、算电协同、服务器液冷、太空光伏"

    body = f"""您好，

今日（{today}）A股机构动向与聪明钱信号分析报告已生成，请查收附件。

─────────────────────────────
  扫描模块：机构调研 / 行业资金流向 / 大宗交易 / 龙虎榜
  关注主题：{themes_str}
  生成时间：{datetime.now().strftime("%Y-%m-%d %H:%M")}
─────────────────────────────

本邮件由系统自动发送，请勿直接回复。
"""

    msg = MIMEMultipart()
    msg["From"]    = cfg["sender"]
    msg["To"]      = cfg["recipient"]
    msg["Date"]    = formatdate(localtime=True)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    # 添加 Excel 附件
    filename = os.path.basename(excel_path)
    with open(excel_path, "rb") as f:
        part = MIMEBase("application",
                        "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        part.set_payload(f.read())
    encoders.encode_base64(part)
    # RFC 5987 编码，防止中文文件名乱码
    encoded_name = filename.encode("utf-8").decode("latin-1", errors="replace")
    part.add_header("Content-Disposition",
                    f'attachment; filename="{encoded_name}"')
    msg.attach(part)

    try:
        port = cfg["smtp_port"]
        if cfg["use_ssl"]:
            with smtplib.SMTP_SSL(cfg["smtp_server"], port, timeout=30) as srv:
                srv.login(cfg["sender"], cfg["password"])
                srv.sendmail(cfg["sender"], [cfg["recipient"]], msg.as_string())
        else:
            with smtplib.SMTP(cfg["smtp_server"], port, timeout=30) as srv:
                srv.ehlo()
                srv.starttls()
                srv.login(cfg["sender"], cfg["password"])
                srv.sendmail(cfg["sender"], [cfg["recipient"]], msg.as_string())

        print(f"  [Email] ✓ 已发送至 {cfg['recipient']}  附件: {filename}")
        return True

    except Exception as e:
        print(f"  [Email] ✗ 发送失败: {e}")
        return False
