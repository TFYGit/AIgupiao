"""
回填近30日龙虎榜数据到 Supabase lhb_history 表。

运行前设置环境变量：
    set SUPABASE_URL=https://xxxx.supabase.co
    set SUPABASE_KEY=eyJhbGciOi...
然后执行：
    python backfill_lhb.py
"""

import os, sys, time, datetime, io
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
    print("错误：请先设置 SUPABASE_URL 和 SUPABASE_KEY 环境变量")
    sys.exit(1)

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_lhb_range(start: str, end: str) -> pd.DataFrame:
    """拉取指定日期区间的龙虎榜数据（格式 YYYYMMDD）"""
    df = ak.stock_lhb_detail_em(start_date=start, end_date=end)
    if df is None or df.empty:
        return pd.DataFrame()
    for col in ["龙虎榜净买额", "龙虎榜买入额", "龙虎榜卖出额", "龙虎榜成交额", "市场总成交额", "流通市值"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 1e8
    for col in ["涨跌幅", "换手率", "净买额占总成交比", "成交额占总成交比"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def build_rows(df: pd.DataFrame) -> list:
    rows = []
    for _, row in df.iterrows():
        def _f(col):
            v = row.get(col)
            return float(v) if v is not None and not (isinstance(v, float) and pd.isna(v)) else None
        rows.append({
            "date":          str(row.get("上榜日", ""))[:10],
            "code":          str(row.get("代码", "")),
            "name":          str(row.get("名称", "")),
            "reason":        str(row.get("上榜原因", "")),
            "change_pct":    _f("涨跌幅"),
            "net_buy":       _f("龙虎榜净买额"),
            "buy_amount":    _f("龙虎榜买入额"),
            "sell_amount":   _f("龙虎榜卖出额"),
            "turnover":      _f("换手率"),
            "net_buy_ratio": _f("净买额占总成交比"),
        })
    return rows


def upsert_batch(rows: list):
    # Supabase 单次 upsert 上限约 1000 行，分批处理
    batch_size = 500
    for i in range(0, len(rows), batch_size):
        sb.table("lhb_history").upsert(
            rows[i:i+batch_size], on_conflict="date,code,reason"
        ).execute()


def main():
    today     = datetime.date.today()
    start_day = today - datetime.timedelta(days=30)

    # 按周分批拉取，避免单次请求数据量过大
    chunks = []
    cur = start_day
    while cur <= today:
        chunk_end = min(cur + datetime.timedelta(days=6), today)
        chunks.append((cur.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        cur = chunk_end + datetime.timedelta(days=1)

    print(f"回填区间：{start_day} ~ {today}，共 {len(chunks)} 个周次\n")

    total_written = 0
    for s, e in chunks:
        print(f"  拉取 {s} ~ {e} ...", end=" ")
        try:
            df = fetch_lhb_range(s, e)
            if df.empty:
                print("无数据（可能全为休市）")
            else:
                rows = build_rows(df)
                upsert_batch(rows)
                print(f"{len(rows)} 条 OK")
                total_written += len(rows)
        except Exception as ex:
            print(f"失败: {ex}")
        time.sleep(1)

    print(f"\n完成，共写入 {total_written} 条记录。")


if __name__ == "__main__":
    main()
