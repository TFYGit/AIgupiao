"""
每日股票调研报告 - 入口脚本

本地运行：
    python run.py              # 采集数据 + 发送邮件
    python run.py --no-email   # 只采集数据，不发邮件

GitHub Actions / 定时任务：
    直接调用 python run.py 即可，邮件配置从环境变量读取
"""

import sys
import os

import smart_money          # 模块顶部已完成代理绕过，import 时自动生效
from send_report import send_report
from smart_money import FOCUS_THEMES


def main():
    no_email = "--no-email" in sys.argv

    # 1. 采集数据、生成 Excel
    result = smart_money.main()
    if result is None:
        print("[run.py] 数据采集失败，退出")
        sys.exit(1)

    _, excel_path = result

    if not os.path.exists(excel_path):
        print(f"[run.py] Excel 文件未生成: {excel_path}")
        sys.exit(1)

    # 2. 发送邮件
    if no_email:
        print(f"[run.py] --no-email 模式，跳过发送。文件: {excel_path}")
    else:
        send_report(excel_path, themes=list(FOCUS_THEMES.keys()))


if __name__ == "__main__":
    main()
