"""
每日收盘后自动保存行业/概念板块资金流向到 Supabase。
由 GitHub Actions 在每个交易日 15:45 BJ 触发，与 dashboard 完全独立。

环境变量：
    SUPABASE_URL  SUPABASE_KEY
"""

import os, sys, time, threading, datetime, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import requests
requests.utils.get_environ_proxies = lambda *a, **kw: {}
_orig = requests.Session.merge_environment_settings
def _no_proxy(self, url, proxies, stream, verify, cert):
    r = _orig(self, url, proxies, stream, verify, cert)
    r["proxies"] = {}
    return r
requests.Session.merge_environment_settings = _no_proxy

import akshare as ak
import pandas as pd
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()

if not SUPABASE_URL or not SUPABASE_KEY:
    print("错误：请先设置 SUPABASE_URL 和 SUPABASE_KEY")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)
TODAY = datetime.date.today().strftime("%Y-%m-%d")


def fetch_with_timeout(fn, timeout=90):
    result, error = [None], [None]
    def _run():
        try:
            result[0] = fn()
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        raise TimeoutError(f"超时 ({timeout}s)")
    if error[0]:
        raise error[0]
    return result[0]


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    for col_name in ["行业", "概念", "板块名称"]:
        if col_name in df.columns and "行业板块" not in df.columns:
            df = df.rename(columns={col_name: "行业板块"})
            break
    df = df.rename(columns={
        "行业-涨跌幅": "涨跌幅%",
        "净额":        "净流入(亿元)",
        "流入资金":    "流入(亿元)",
        "流出资金":    "流出(亿元)",
    })
    if "净流入(亿元)" in df.columns:
        df["净流入(亿元)"] = pd.to_numeric(df["净流入(亿元)"], errors="coerce")
    return df.drop_duplicates(subset="行业板块")


def upsert(table: str, df: pd.DataFrame, prev_map: dict):
    rows = []
    for _, row in df[["行业板块", "净流入(亿元)"]].iterrows():
        board = str(row["行业板块"])
        net   = row["净流入(亿元)"]
        if pd.isna(net):
            continue
        net = round(float(net), 2)
        prev_val = prev_map.get(board)
        change = round(net - prev_val, 2) if prev_val is not None else None
        rows.append({"date": TODAY, "industry": board,
                     "net_inflow": net, "net_inflow_change": change})
    if not rows:
        print(f"  {table}: 无有效行，跳过")
        return
    # 分批 upsert
    for i in range(0, len(rows), 500):
        sb.table(table).upsert(rows[i:i+500], on_conflict="date,industry").execute()
    print(f"  {table}: 写入 {len(rows)} 条 OK")


def get_prev_map(table: str) -> dict:
    rows = sb.table(table).select("industry,net_inflow").eq("date", TODAY).execute().data or []
    return {r["industry"]: float(r["net_inflow"]) for r in rows if r.get("net_inflow") is not None}


def run():
    print(f"=== 行业/概念板块资金流向存库  {TODAY} ===\n")

    # ── 行业板块 ──────────────────────────────────────────────
    print("拉取行业板块数据...", end=" ", flush=True)
    try:
        raw = fetch_with_timeout(lambda: ak.stock_fund_flow_industry(symbol="即时"))
        if raw is None or raw.empty:
            print("无数据（可能非交易日）")
        else:
            df = normalize(raw)
            prev = get_prev_map("industry_fund_history")
            upsert("industry_fund_history", df, prev)
    except Exception as e:
        print(f"失败: {e}")

    time.sleep(2)

    # ── 概念板块 ──────────────────────────────────────────────
    print("拉取概念板块数据...", end=" ", flush=True)
    try:
        raw = fetch_with_timeout(lambda: ak.stock_fund_flow_concept(symbol="即时"), timeout=120)
        if raw is None or raw.empty:
            print("无数据（可能非交易日）")
        else:
            df = normalize(raw)
            prev = get_prev_map("concept_fund_history")
            upsert("concept_fund_history", df, prev)
    except Exception as e:
        print(f"失败: {e}")

    print("\n完成。")


if __name__ == "__main__":
    run()
