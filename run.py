"""
每日股票调研报告 - 入口脚本
用法：
    python run.py          # 立即运行一次（采集数据 + 发送邮件）
    python run.py --no-email   # 只采集数据，不发送邮件

Windows 任务计划程序配置：
    程序：  C:\Python313\python.exe  （或你的 Python 路径）
    参数：  D:\lianghua\run.py
    起始于：D:\lianghua
    触发器：每天 19:00
"""

import sys
import os

# ── 代理绕过（必须在 import akshare 之前，smart_money 内部已处理，
#    这里提前 import 确保顺序正确）──────────────────────────────
import requests
requests.utils.get_environ_proxies = lambda *a, **kw: {}
_orig_menv = requests.Session.merge_environment_settings
def _no_proxy(self, url, proxies, stream, verify, cert):
    result = _orig_menv(self, url, proxies, stream, verify, cert)
    result["proxies"] = {}
    return result
requests.Session.merge_environment_settings = _no_proxy

# ── 主逻辑 ────────────────────────────────────────────────────
import smart_money
from send_report import send_report
from smart_money import FOCUS_THEMES

def main():
    no_email = "--no-email" in sys.argv

    # 1. 采集数据、生成 Excel
    result = smart_money.main()

    if result is None:
        print("[run.py] 数据采集失败，退出")
        return

    _, excel_path = result

    # 2. 验证文件存在
    if not os.path.exists(excel_path):
        print(f"[run.py] Excel 文件未生成: {excel_path}")
        return

    # 3. 发送邮件
    if no_email:
        print(f"[run.py] --no-email 模式，跳过发送。文件: {excel_path}")
    else:
        send_report(excel_path, themes=list(FOCUS_THEMES.keys()))


if __name__ == "__main__":
    main()
