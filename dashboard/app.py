import streamlit as st
import pandas as pd

# 代理绕过（必须在 import akshare 之前，防止系统代理拦截东方财富请求）
import requests
requests.utils.get_environ_proxies = lambda *a, **kw: {}
_orig_menv = requests.Session.merge_environment_settings
def _no_proxy(self, url, proxies, stream, verify, cert):
    result = _orig_menv(self, url, proxies, stream, verify, cert)
    result["proxies"] = {}
    return result
requests.Session.merge_environment_settings = _no_proxy

import akshare as ak
import plotly.graph_objects as go
from datetime import datetime, timezone, timedelta
from supabase import create_client

BJT = timezone(timedelta(hours=8))

st.set_page_config(
    page_title="行业资金流向",
    page_icon="📊",
    layout="wide",
)

MARKET_OPEN   = (9,  0)
AUCTION_START = (9, 15)
AUCTION_END   = (9, 25)
MARKET_CLOSE  = (15, 30)
AUCTION_INTERVAL = 60        # 集合竞价1分钟刷新


def is_trading_time() -> bool:
    """判断当前是否为A股交易时间（周一至周五 9:00-15:30 北京时间）"""
    now = datetime.now(BJT)
    if now.weekday() >= 5:
        return False
    h, m = now.hour, now.minute
    return (MARKET_OPEN[0] * 60 + MARKET_OPEN[1]
            <= h * 60 + m <=
            MARKET_CLOSE[0] * 60 + MARKET_CLOSE[1])


# 交易时间内5分钟刷新；非交易时间8小时TTL，避免无效请求
REFRESH_INTERVAL = 300 if is_trading_time() else 3600 * 8


@st.cache_resource
def get_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)


def now_bjt():
    return datetime.now(BJT)


def load_history() -> dict:
    """从 Supabase 分页加载近10个交易日所有板块数据，格式: {日期: {行业: 净流入}}"""
    try:
        from datetime import date, timedelta
        sb = get_supabase()
        start = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        history = {}
        page_size = 1000
        offset = 0
        while True:
            rows = (sb.table("industry_fund_history")
                      .select("date,industry,net_inflow")
                      .gte("date", start)
                      .order("date", desc=True)
                      .range(offset, offset + page_size - 1)
                      .execute().data)
            if not rows:
                break
            for r in rows:
                d = str(r["date"])
                history.setdefault(d, {})[r["industry"]] = r["net_inflow"]
            if len(rows) < page_size:
                break
            if len(history) >= 11:
                break
            offset += page_size
        dates = sorted(history.keys())[-10:]
        return {d: history[d] for d in dates}
    except Exception:
        return {}


def history_to_df(history: dict) -> "pd.DataFrame | None":
    """将 load_history / load_concept_history 返回的 dict 转为最新一天的 DataFrame，用于 API 不可用时兜底显示。"""
    if not history:
        return None
    latest_date = max(history.keys())
    sectors = history[latest_date]
    if not sectors:
        return None
    df = pd.DataFrame([{"行业板块": k, "净流入(亿元)": v} for k, v in sectors.items()])
    df["净流入(亿元)"] = pd.to_numeric(df["净流入(亿元)"], errors="coerce")
    df = df.sort_values("净流入(亿元)", ascending=False).reset_index(drop=True)
    df.index += 1
    return df, latest_date


def save_history(df: pd.DataFrame, prev_df: pd.DataFrame = None):
    """把所有板块当天净流入 upsert 到 Supabase"""
    today = now_bjt().strftime("%Y-%m-%d")
    try:
        sb = get_supabase()
        # 优先用 DB 中今日已有记录算环比，没有则退回 session state 的 prev_df
        existing = sb.table("industry_fund_history").select("industry,net_inflow").eq("date", today).execute().data or []
        prev_map = {r["industry"]: float(r["net_inflow"]) for r in existing if r.get("net_inflow") is not None}
        if not prev_map and prev_df is not None and "行业板块" in prev_df.columns:
            prev_map = {str(k): float(v) for k, v in prev_df.set_index("行业板块")["净流入(亿元)"].items()
                        if v is not None and not pd.isna(v)}
        rows = []
        for _, row in df[["行业板块", "净流入(亿元)"]].iterrows():
            board = str(row["行业板块"])
            net   = round(float(row["净流入(亿元)"]), 2)
            prev_val = prev_map.get(board)
            change = round(net - prev_val, 2) if prev_val is not None else None
            rows.append({"date": today, "industry": board,
                         "net_inflow": net, "net_inflow_change": change})
        sb.table("industry_fund_history").upsert(rows, on_conflict="date,industry").execute()
    except Exception as e:
        st.session_state["_save_industry_err"] = str(e)[:200]


def load_concept_history() -> dict:
    """从 Supabase 分页加载近10个交易日所有概念板块数据，格式: {日期: {概念: 净流入}}"""
    try:
        from datetime import date, timedelta
        sb = get_supabase()
        start = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        history = {}
        page_size = 1000
        offset = 0
        while True:
            rows = (sb.table("concept_fund_history")
                      .select("date,industry,net_inflow")
                      .gte("date", start)
                      .order("date", desc=True)
                      .range(offset, offset + page_size - 1)
                      .execute().data)
            if not rows:
                break
            for r in rows:
                d = str(r["date"])
                history.setdefault(d, {})[r["industry"]] = r["net_inflow"]
            if len(rows) < page_size:
                break
            # 已有11个日期说明前10个日期的数据已完整，可以停止
            if len(history) >= 11:
                break
            offset += page_size
        dates = sorted(history.keys())[-10:]
        return {d: history[d] for d in dates}
    except Exception:
        return {}


def save_concept_history(df: pd.DataFrame, prev_df: pd.DataFrame = None):
    """把所有概念板块当天净流入 upsert 到 Supabase"""
    today = now_bjt().strftime("%Y-%m-%d")
    try:
        sb = get_supabase()
        # 优先用 DB 中今日已有记录算环比，没有则退回 session state 的 prev_df
        existing = sb.table("concept_fund_history").select("industry,net_inflow").eq("date", today).execute().data or []
        prev_map = {r["industry"]: float(r["net_inflow"]) for r in existing if r.get("net_inflow") is not None}
        if not prev_map and prev_df is not None and "行业板块" in prev_df.columns:
            prev_map = {str(k): float(v) for k, v in prev_df.set_index("行业板块")["净流入(亿元)"].items()
                        if v is not None and not pd.isna(v)}
        rows = []
        for _, row in df[["行业板块", "净流入(亿元)"]].iterrows():
            board = str(row["行业板块"])
            net   = round(float(row["净流入(亿元)"]), 2)
            prev_val = prev_map.get(board)
            change = round(net - prev_val, 2) if prev_val is not None else None
            rows.append({"date": today, "industry": board,
                         "net_inflow": net, "net_inflow_change": change})
        sb.table("concept_fund_history").upsert(rows, on_conflict="date,industry").execute()
    except Exception as e:
        st.session_state["_save_concept_err"] = str(e)[:200]


def init_prev_from_db(table_name: str) -> "pd.DataFrame | None":
    """页面首次加载时，从 DB 读今日已有记录作为环比基准"""
    try:
        sb = get_supabase()
        today = now_bjt().strftime("%Y-%m-%d")
        rows = sb.table(table_name).select("industry,net_inflow").eq("date", today).execute().data
        if not rows:
            return None
        return pd.DataFrame(rows).rename(columns={"industry": "行业板块", "net_inflow": "净流入(亿元)"})
    except Exception:
        return None


def save_lhb_history(df: pd.DataFrame):
    """把龙虎榜数据 upsert 到 Supabase（以上榜日为准）"""
    try:
        sb = get_supabase()
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
        if rows:
            sb.table("lhb_history").upsert(rows, on_conflict="date,code,reason").execute()
    except Exception as e:
        st.session_state["_save_lhb_err"] = str(e)[:200]


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_lhb_history() -> pd.DataFrame:
    """从 Supabase 加载近30日龙虎榜数据"""
    try:
        from datetime import date, timedelta
        sb = get_supabase()
        start = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        rows = (sb.table("lhb_history")
                  .select("date,code,name,reason,change_pct,net_buy,buy_amount,sell_amount,turnover,net_buy_ratio")
                  .gte("date", start)
                  .order("date", desc=True)
                  .execute().data)
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows).rename(columns={
            "date": "上榜日", "code": "代码", "name": "名称", "reason": "上榜原因",
            "change_pct": "涨跌幅", "net_buy": "龙虎榜净买额",
            "buy_amount": "龙虎榜买入额", "sell_amount": "龙虎榜卖出额",
            "turnover": "换手率", "net_buy_ratio": "净买额占总成交比",
        })
        # 过滤历史数据中已存入的多日累计原因行，保持与实时数据一致
        if "上榜原因" in df.columns:
            df = df[~df["上榜原因"].str.contains("连续|累计", na=False)]
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_zt_dt_history() -> pd.DataFrame:
    """从 Supabase 加载近10个交易日涨停/跌停数"""
    try:
        from datetime import date, timedelta
        sb = get_supabase()
        start = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        rows = (sb.table("zt_dt_history")
                  .select("date,zt_count,dt_count")
                  .gte("date", start)
                  .order("date", desc=False)
                  .execute().data)
        if not rows:
            return pd.DataFrame(columns=["date", "zt_count", "dt_count"])
        df = pd.DataFrame(rows).sort_values("date").tail(10).reset_index(drop=True)
        return df
    except Exception:
        return pd.DataFrame(columns=["date", "zt_count", "dt_count"])


def save_zt_dt_history(zt_count: int, dt_count: int):
    """把今日涨停/跌停数 upsert 到 Supabase"""
    today = now_bjt().strftime("%Y-%m-%d")
    try:
        sb = get_supabase()
        sb.table("zt_dt_history").upsert(
            {"date": today, "zt_count": zt_count, "dt_count": dt_count},
            on_conflict="date"
        ).execute()
    except Exception as e:
        st.session_state["_save_zt_dt_err"] = str(e)[:200]


def is_market_open() -> bool:
    t = (now_bjt().hour, now_bjt().minute)
    return MARKET_OPEN <= t <= MARKET_CLOSE


def is_auction_time() -> bool:
    t = (now_bjt().hour, now_bjt().minute)
    return AUCTION_START <= t <= AUCTION_END


# ---- 数据获取 ----

_EM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://data.eastmoney.com/",
}


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_zt_count(date_str: str) -> dict:
    """用akshare涨停池统计各行业涨停家数"""
    import threading
    result, error = [None], [None]
    def _run():
        try:
            result[0] = ak.stock_zt_pool_em(date=date_str)
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(15)
    if t.is_alive() or error[0] or result[0] is None or result[0].empty:
        return {}
    df = result[0]
    if "所属行业" not in df.columns:
        return {}
    return df["所属行业"].value_counts().to_dict()


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_zt_total(date_str: str) -> int:
    """复用已缓存的fetch_zt_count，避免重复调API"""
    zt_map = fetch_zt_count(date_str)
    return sum(zt_map.values()) if zt_map else 0


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_dt_pool(date_str: str) -> "pd.DataFrame":
    """获取今日跌停池完整数据，供 fetch_dt_count / fetch_dt_sector 复用"""
    import threading
    result, error = [None], [None]
    def _run():
        try:
            result[0] = ak.stock_zt_pool_dtgc_em(date=date_str)
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(15)
    if t.is_alive() or error[0] or result[0] is None or result[0].empty:
        return pd.DataFrame()
    return result[0]


def fetch_dt_count(date_str: str) -> int:
    """今日跌停总家数"""
    return len(fetch_dt_pool(date_str))


def fetch_dt_sector(date_str: str) -> dict:
    """今日各行业跌停家数，返回 {行业: 跌停数}"""
    df = fetch_dt_pool(date_str)
    if df.empty or "所属行业" not in df.columns:
        return {}
    return df["所属行业"].value_counts().to_dict()


def _lhb_fetch_one_day(date_str: str) -> tuple:
    """并发拉取指定日期龙虎榜明细+机构数据，返回 (df, jg_df)；失败返回 (None, DataFrame())。"""
    import threading
    detail_res, detail_err = [None], [None]
    jg_res, jg_err = [None], [None]

    def _run_detail():
        try:
            detail_res[0] = ak.stock_lhb_detail_em(start_date=date_str, end_date=date_str)
        except Exception as e:
            detail_err[0] = e

    def _run_jg():
        try:
            jg_res[0] = ak.stock_lhb_jgmmtj_em(start_date=date_str, end_date=date_str)
        except Exception as e:
            jg_err[0] = e

    t1 = threading.Thread(target=_run_detail, daemon=True)
    t2 = threading.Thread(target=_run_jg, daemon=True)
    t1.start(); t2.start()
    t1.join(30); t2.join(30)

    if t1.is_alive() or detail_err[0] or detail_res[0] is None or detail_res[0].empty:
        return None, pd.DataFrame()
    df = detail_res[0]
    for col in ["龙虎榜净买额", "龙虎榜买入额", "龙虎榜卖出额", "龙虎榜成交额", "市场总成交额", "流通市值"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 1e8
    for col in ["涨跌幅", "换手率", "净买额占总成交比", "成交额占总成交比"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    jg_df = pd.DataFrame()
    if not t2.is_alive() and not jg_err[0] and jg_res[0] is not None and not jg_res[0].empty:
        raw = jg_res[0]
        code_col = next((c for c in ["代码", "股票代码"] if c in raw.columns), None)
        net_col  = next((c for c in ["机构买入净额", "机构净买额", "净买额"] if c in raw.columns), None)
        if code_col and net_col:
            raw[net_col] = pd.to_numeric(raw[net_col], errors="coerce") / 1e8
            raw = raw.drop_duplicates(subset=[code_col])
            jg_df = raw[[code_col, net_col]].rename(columns={code_col: "代码", net_col: "机构净买额_raw"})
    return df, jg_df


@st.cache_data(ttl=300)
def fetch_lhb_data():
    """获取最近有数据的龙虎榜，返回 (df, jg_df, timestamp, fallback_date)。
    今日无数据时自动回退到最近工作日；fallback_date 为 None 表示今日数据。"""
    from datetime import timedelta
    today = now_bjt().date()
    yesterday = today - timedelta(days=1)
    if yesterday.weekday() >= 5:
        yesterday = yesterday - timedelta(days=yesterday.weekday() - 4)
    for d in [today, yesterday]:
        df, jg_df = _lhb_fetch_one_day(d.strftime("%Y%m%d"))
        if df is not None:
            fallback_date = None if d == today else d.strftime("%Y-%m-%d")
            return df, jg_df, now_bjt().strftime("%Y-%m-%d %H:%M:%S"), fallback_date
    return None, pd.DataFrame(), None, None




def _dzjy_fetch_date(date_str: str) -> pd.DataFrame:
    """拉取指定日期大宗交易原始数据，失败或无数据返回空 DataFrame。"""
    import threading
    result, error = [None], [None]
    def _run():
        try:
            result[0] = ak.stock_dzjy_mrmx(symbol="A股", start_date=date_str, end_date=date_str)
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(30)
    if t.is_alive() or error[0] or result[0] is None or result[0].empty:
        return pd.DataFrame()
    df = result[0]
    for col in ["折溢率", "成交额", "成交量", "收盘价", "成交价"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if "成交额" in df.columns:
        df["成交额(亿)"] = (df["成交额"] / 1e8).round(4)
        df = df.drop(columns=["成交额"])
    if "成交额(亿)" in df.columns:
        df = df.sort_values("成交额(亿)", ascending=False)
    return df.reset_index(drop=True)


@st.cache_data(ttl=300)
def fetch_dzjy_data() -> tuple:
    """获取最近有数据的大宗交易，返回 (df, fallback_date)。
    今日无数据时自动回退到最近工作日；fallback_date 为 None 表示是今日数据。"""
    from datetime import timedelta
    today = now_bjt().date()
    yesterday = today - timedelta(days=1)
    if yesterday.weekday() >= 5:
        yesterday = yesterday - timedelta(days=yesterday.weekday() - 4)
    for d in [today, yesterday]:
        df = _dzjy_fetch_date(d.strftime("%Y%m%d"))
        if not df.empty:
            return df, (None if d == today else d.strftime("%Y-%m-%d"))
    return pd.DataFrame(), None


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_data():
    import threading
    result, error = [None], [None]
    def _run():
        try:
            result[0] = ak.stock_fund_flow_industry(symbol="即时")
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(30)
    if t.is_alive():
        raise TimeoutError("行业资金流向接口超时，稍后重试")
    if error[0]:
        raise error[0]
    df = result[0]
    if df is None or df.empty:
        raise ValueError("行业数据为空，稍后重试")

    df = df.rename(columns={
        "行业": "行业板块",
        "行业-涨跌幅": "涨跌幅%",
        "净额": "净流入(亿元)",
        "流入资金": "流入(亿元)",
        "流出资金": "流出(亿元)",
        "领涨股-涨跌幅": "领涨股涨跌幅%",
    })
    for col in ["净流入(亿元)", "流入(亿元)", "流出(亿元)", "涨跌幅%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["成交额(亿元)"] = df["流入(亿元)"] + df["流出(亿元)"]
    df["净流入率%"] = (df["净流入(亿元)"] / df["成交额(亿元)"].replace(0, float("nan")) * 100).round(2)

    zt_map = fetch_zt_count(now_bjt().strftime("%Y%m%d"))
    df["涨停数"] = df["行业板块"].map(zt_map).fillna(0).astype(int)

    df = pd.DataFrame(df).drop_duplicates(subset="行业板块")
    for col in ["涨跌幅%", "净流入率%", "领涨股涨跌幅%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.sort_values("净流入(亿元)", ascending=False).reset_index(drop=True)
    df.index = df.index + 1
    # 全市场成交额：上证综指 + 深证成指 + 北证50
    try:
        hdrs = {"User-Agent": "Mozilla/5.0"}
        secids = ["1.000001", "0.399106", "0.899050"]
        total = 0
        for s in secids:
            try:
                val = requests.get(
                    f"https://push2.eastmoney.com/api/qt/stock/get?secid={s}&fields=f48",
                    headers=hdrs, timeout=8
                ).json().get("data", {}).get("f48")
                if isinstance(val, (int, float)) and val > 0:
                    total += val
            except Exception:
                pass
        turnover = f"{total / 1e8:.0f} 亿元" if total > 0 else "—"
    except Exception:
        turnover = "—"
    updated_at = now_bjt().strftime("%Y-%m-%d %H:%M:%S")
    return df, updated_at, turnover


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_concept_data():
    """获取概念板块资金流向，失败时返回 (None, None) 而非抛异常（保证结果被缓存，避免重复阻塞）"""
    import threading
    result, error = [None], [None]
    def _run():
        try:
            result[0] = ak.stock_fund_flow_concept(symbol="即时")
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(90)
    if t.is_alive() or error[0] or result[0] is None or result[0].empty:
        return None, None
    df = result[0]

    # 兼容 akshare 返回的列名（行业/概念/板块名称 三选一）
    for col_name in ["行业", "概念", "板块名称"]:
        if col_name in df.columns and "行业板块" not in df.columns:
            df = df.rename(columns={col_name: "行业板块"})
            break

    df = df.rename(columns={
        "行业-涨跌幅": "涨跌幅%",
        "净额":        "净流入(亿元)",
        "流入资金":    "流入(亿元)",
        "流出资金":    "流出(亿元)",
        "领涨股-涨跌幅": "领涨股涨跌幅%",
    })
    for col in ["净流入(亿元)", "流入(亿元)", "流出(亿元)", "涨跌幅%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    df["成交额(亿元)"] = df["流入(亿元)"] + df["流出(亿元)"]
    df["净流入率%"] = (df["净流入(亿元)"] / df["成交额(亿元)"].replace(0, float("nan")) * 100).round(2)
    df = df.drop_duplicates(subset="行业板块")
    for col in ["涨跌幅%", "净流入率%", "领涨股涨跌幅%"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.sort_values("净流入(亿元)", ascending=False).reset_index(drop=True)
    df.index = df.index + 1
    updated_at = now_bjt().strftime("%Y-%m-%d %H:%M:%S")
    return df, updated_at


@st.cache_data(ttl=AUCTION_INTERVAL)
def fetch_auction_data():
    """集合竞价期间：优先用akshare同花顺行业数据，失败则回退东方财富API"""
    import threading

    df = None
    # 优先 akshare（与主交易数据源一致，确保行业名相同）
    ak_result, ak_err = [None], [None]
    def _run_ak():
        try:
            ak_result[0] = ak.stock_fund_flow_industry(symbol="即时")
        except Exception as e:
            ak_err[0] = e
    t = threading.Thread(target=_run_ak, daemon=True)
    t.start()
    t.join(20)

    if not t.is_alive() and ak_result[0] is not None and not ak_result[0].empty:
        df = ak_result[0].rename(columns={
            "行业": "行业板块",
            "行业-涨跌幅": "涨跌幅%",
        })
        df["涨跌幅%"] = pd.to_numeric(df.get("涨跌幅%", 0), errors="coerce").fillna(0)
        for col in ["上涨家数", "下跌家数"]:
            if col not in df.columns:
                df[col] = 0

    # 回退：东方财富直接接口
    if df is None or df.empty:
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        params = {
            "pn": 1, "pz": 200, "po": 1, "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2, "invt": 2, "fid": "f3",
            "fs": "m:90+t:2+f:!50",
            "fields": "f14,f3,f104,f105",
        }
        resp = requests.get(url, params=params,
                            headers={"User-Agent": "Mozilla/5.0"}, timeout=15)
        items = resp.json().get("data", {}).get("diff", []) or []
        if not items:
            raise ValueError("集合竞价数据为空")
        df = pd.DataFrame(items).rename(columns={
            "f14": "行业板块", "f3": "涨跌幅%",
            "f104": "上涨家数", "f105": "下跌家数",
        })
        df["涨跌幅%"] = pd.to_numeric(df["涨跌幅%"], errors="coerce").fillna(0)

    df = df.drop_duplicates(subset="行业板块")

    def classify(v):
        if v >= 1.0:   return "高开(≥1%)"
        elif v <= -1.0: return "低开(≤-1%)"
        elif v > 0:    return "小幅高开"
        elif v < 0:    return "小幅低开"
        else:          return "平开"

    df["开盘状态"] = df["涨跌幅%"].apply(classify)
    df = df.sort_values("涨跌幅%", ascending=False).reset_index(drop=True)
    df.index = df.index + 1
    return df


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_brent_oil() -> dict:
    """布伦特原油期货（新浪外盘 OIL），失败时回退日线收盘价"""
    import threading

    rt_result, rt_err = [None], [None]

    def _run_rt():
        try:
            rt_result[0] = ak.futures_foreign_commodity_realtime(symbol="OIL")
        except Exception as e:
            rt_err[0] = e

    t = threading.Thread(target=_run_rt, daemon=True)
    t.start()
    t.join(20)

    hist = ak.futures_foreign_hist(symbol="OIL")
    if hist is None or hist.empty:
        raise ValueError("布伦特原油历史数据为空")
    hist = hist.copy()
    hist["date"] = pd.to_datetime(hist["date"], errors="coerce")
    hist = hist.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)
    for col in ["open", "high", "low", "close"]:
        hist[col] = pd.to_numeric(hist[col], errors="coerce")

    prev_close = float(hist["close"].iloc[-2]) if len(hist) >= 2 else float(hist["close"].iloc[-1])
    updated_at = now_bjt().strftime("%Y-%m-%d %H:%M:%S")
    source = "hist"

    if not t.is_alive() and rt_result[0] is not None and not rt_result[0].empty:
        row = rt_result[0].iloc[0]
        price = pd.to_numeric(row.get("current_price"), errors="coerce")
        if pd.notna(price) and float(price) > 0:
            price = float(price)
            settle = pd.to_numeric(row.get("last_settle_price"), errors="coerce")
            base = float(settle) if pd.notna(settle) and float(settle) > 0 else prev_close
            change = price - base
            change_pct = change / base * 100 if base else 0.0
            quote_time = str(row.get("time", "") or "")
            quote_date = str(row.get("date", "") or "")
            if quote_time or quote_date:
                updated_at = f"{quote_date} {quote_time}".strip()
            return {
                "price": price, "change": change, "change_pct": change_pct,
                "high": pd.to_numeric(row.get("high"), errors="coerce"),
                "low":  pd.to_numeric(row.get("low"), errors="coerce"),
                "open": pd.to_numeric(row.get("open"), errors="coerce"),
                "updated_at": updated_at, "hist": hist.tail(60), "source": "realtime",
            }
        source = "hist"

    last = hist.iloc[-1]
    price = float(last["close"])
    change = price - prev_close
    change_pct = change / prev_close * 100 if prev_close else 0.0
    return {
        "price": price, "change": change, "change_pct": change_pct,
        "high": float(last["high"]), "low": float(last["low"]), "open": float(last["open"]),
        "updated_at": f"{last['date'].strftime('%Y-%m-%d')}（日线）",
        "hist": hist.tail(60), "source": source,
    }


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_us_treasury() -> dict:
    """美债 10 年期 / 30 年期收益率（东方财富）"""
    import threading
    from datetime import date, timedelta

    result, error = [None], [None]

    def _run():
        try:
            start = (date.today() - timedelta(days=400)).strftime("%Y%m%d")
            result[0] = ak.bond_zh_us_rate(start_date=start)
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(45)
    if t.is_alive():
        raise TimeoutError("美债收益率接口超时，稍后重试")
    if error[0]:
        raise error[0]
    df = result[0]
    if df is None or df.empty:
        raise ValueError("美债收益率数据为空")

    df = df.copy()
    df["日期"] = pd.to_datetime(df["日期"], errors="coerce")
    df = df.dropna(subset=["日期"]).sort_values("日期").reset_index(drop=True)
    col_10 = "美国国债收益率10年"
    col_30 = "美国国债收益率30年"
    for col in [col_10, col_30]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[col_10, col_30])
    if df.empty:
        raise ValueError("美债收益率解析失败")

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last
    y10 = float(last[col_10])
    y30 = float(last[col_30])
    return {
        "date": last["日期"].strftime("%Y-%m-%d"),
        "y10": y10, "y30": y30,
        "y10_chg": y10 - float(prev[col_10]),
        "y30_chg": y30 - float(prev[col_30]),
        "hist": df.tail(120).rename(columns={col_10: "10年期", col_30: "30年期"}),
        "updated_at": now_bjt().strftime("%Y-%m-%d %H:%M:%S"),
    }


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_us_inflation() -> dict:
    """美国 CPI 同比（通胀率），来源：金十 / 东方财富宏观"""
    import threading

    result, error = [None], [None]

    def _run():
        try:
            result[0] = ak.macro_usa_cpi_yoy()
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(30)
    if t.is_alive():
        raise TimeoutError("美国CPI数据接口超时，稍后重试")
    if error[0]:
        raise error[0]
    df = result[0]
    if df is None or df.empty:
        raise ValueError("美国CPI数据为空")

    df = df.copy()
    df["现值"] = pd.to_numeric(df["现值"], errors="coerce")
    df["前值"] = pd.to_numeric(df["前值"], errors="coerce")
    df["发布日期"] = pd.to_datetime(df["发布日期"], errors="coerce")
    df = df.dropna(subset=["现值", "发布日期"]).sort_values("发布日期").reset_index(drop=True)
    if df.empty:
        raise ValueError("美国CPI解析失败")

    last = df.iloc[-1]
    cpi = float(last["现值"])
    prev_cpi = float(last["前值"]) if pd.notna(last["前值"]) else cpi
    period = str(last.get("时间", ""))
    if len(period) >= 7:
        period = period[:7]
    return {
        "cpi_yoy": cpi,
        "cpi_chg": cpi - prev_cpi,
        "prev_cpi": prev_cpi,
        "release_date": last["发布日期"].strftime("%Y-%m-%d"),
        "period": period,
        "hist": df.tail(48),
        "updated_at": now_bjt().strftime("%Y-%m-%d %H:%M:%S"),
    }


def build_brent_chart(hist: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hist["date"], y=hist["close"], name="收盘价",
        line=dict(color="#ef5350", width=2),
        hovertemplate="%{x|%Y-%m-%d}<br>收盘: %{y:.2f} USD<extra></extra>",
    ))
    fig.update_layout(
        title="布伦特原油 · 近60日收盘价",
        yaxis_title="USD/桶",
        height=400,
        margin=dict(t=50, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
    )
    return fig


def build_treasury_chart(hist: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hist["日期"], y=hist["10年期"], name="10年期",
        line=dict(color="#ef5350", width=2),
        hovertemplate="%{x|%Y-%m-%d}<br>10年期: %{y:.2f}%<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=hist["日期"], y=hist["30年期"], name="30年期",
        line=dict(color="#42a5f5", width=2),
        hovertemplate="%{x|%Y-%m-%d}<br>30年期: %{y:.2f}%<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text="美债收益率", font=dict(size=14)),
        yaxis_title="收益率 %",
        height=360,
        margin=dict(t=40, b=30, l=50, r=20),
        plot_bgcolor="rgba(248,249,250,0.5)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        hovermode="x unified",
    )
    return fig


def build_inflation_chart(hist: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=hist["发布日期"], y=hist["现值"], name="CPI同比",
        mode="lines+markers",
        line=dict(color="#ff9800", width=2),
        marker=dict(size=5),
        fill="tozeroy",
        fillcolor="rgba(255,152,0,0.12)",
        hovertemplate="%{x|%Y-%m-%d}<br>CPI同比: %{y:.1f}%<extra></extra>",
    ))
    fig.update_layout(
        title=dict(text="CPI 同比（通胀率）", font=dict(size=14)),
        yaxis_title="同比 %",
        height=360,
        margin=dict(t=40, b=30, l=50, r=20),
        plot_bgcolor="rgba(248,249,250,0.5)",
        paper_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
    )
    return fig


def render_brent_tab():
    try:
        data = fetch_brent_oil()
    except Exception as e:
        st.error(f"布伦特原油数据获取失败：{e}")
        return

    src_label = "新浪外盘实时" if data["source"] == "realtime" else "日线收盘（实时暂不可用）"
    st.caption(f"合约代码 OIL（布伦特原油）　数据来源：{src_label}　更新：{data['updated_at']}")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("最新价", f"{data['price']:.2f} USD")
    c2.metric("涨跌", f"{data['change']:+.2f}", delta=f"{data['change_pct']:+.2f}%")
    if pd.notna(data.get("open")):
        c3.metric("今开", f"{float(data['open']):.2f}")
    if pd.notna(data.get("high")):
        c4.metric("最高", f"{float(data['high']):.2f}")
    if pd.notna(data.get("low")):
        c5.metric("最低", f"{float(data['low']):.2f}")

    st.plotly_chart(build_brent_chart(data["hist"]), use_container_width=True)

    show = data["hist"][["date", "open", "high", "low", "close"]].copy()
    show["date"] = show["date"].dt.strftime("%Y-%m-%d")
    show = show.iloc[::-1].reset_index(drop=True)
    show.index += 1
    show.columns = ["日期", "开盘", "最高", "最低", "收盘"]
    st.subheader("近60日行情")
    st.dataframe(
        show.style.format({"开盘": "{:.2f}", "最高": "{:.2f}", "最低": "{:.2f}", "收盘": "{:.2f}"}),
        use_container_width=True,
        height=400,
    )


def render_treasury_tab():
    st.markdown("##### 美国宏观 · 利率与通胀")
    st.caption("美债收益率（东方财富）　·　CPI 同比（金十宏观）")

    treasury, inflation = None, None
    err_t, err_i = None, None
    try:
        treasury = fetch_us_treasury()
    except Exception as e:
        err_t = str(e)
    try:
        inflation = fetch_us_inflation()
    except Exception as e:
        err_i = str(e)

    if treasury is None and inflation is None:
        st.error(f"数据加载失败：美债 — {err_t}；通胀 — {err_i}")
        return

    # ── 指标卡片：美债 | 通胀 ──
    left, right = st.columns(2, gap="medium")

    with left:
        st.markdown("**美债收益率**")
        if treasury:
            spread = treasury["y30"] - treasury["y10"]
            r1a, r1b, r1c = st.columns(3)
            r1a.metric("10 年期", f"{treasury['y10']:.2f}%", delta=f"{treasury['y10_chg']:+.2f}%")
            r1b.metric("30 年期", f"{treasury['y30']:.2f}%", delta=f"{treasury['y30_chg']:+.2f}%")
            r1c.metric("30Y−10Y", f"{spread:.2f}%")
            st.caption(f"数据日 {treasury['date']}")
        else:
            st.warning(f"美债：{err_t}")

    with right:
        st.markdown("**通胀（CPI 同比）**")
        if inflation:
            r2a, r2b, r2c = st.columns(3)
            r2a.metric("最新同比", f"{inflation['cpi_yoy']:.1f}%", delta=f"{inflation['cpi_chg']:+.1f}%")
            r2b.metric("前值", f"{inflation['prev_cpi']:.1f}%")
            r2c.metric("统计月", inflation["period"] or "—")
            st.caption(f"发布日 {inflation['release_date']}")
        else:
            st.warning(f"通胀：{err_i}")

    if treasury or inflation:
        updated = (treasury or inflation)["updated_at"]
        st.caption(f"页面刷新：{updated}")

    st.divider()

    # ── 双图并排 ──
    chart_l, chart_r = st.columns(2, gap="medium")
    with chart_l:
        if treasury:
            st.plotly_chart(build_treasury_chart(treasury["hist"]), use_container_width=True)
        else:
            st.info("美债走势图暂不可用")
    with chart_r:
        if inflation:
            st.plotly_chart(build_inflation_chart(inflation["hist"]), use_container_width=True)
        else:
            st.info("通胀走势图暂不可用")

    st.divider()

    # ── 明细表 ──
    tab_yield, tab_cpi = st.tabs(["📋 美债历史", "📋 CPI 同比历史"])
    with tab_yield:
        if treasury:
            show = treasury["hist"][["日期", "10年期", "30年期"]].copy()
            show["日期"] = show["日期"].dt.strftime("%Y-%m-%d")
            show = show.iloc[::-1].reset_index(drop=True)
            show.index += 1
            st.dataframe(
                show.style.format({"10年期": "{:.2f}%", "30年期": "{:.2f}%"}),
                use_container_width=True,
                height=360,
            )
        else:
            st.info("暂无美债明细")
    with tab_cpi:
        if inflation:
            show = inflation["hist"][["发布日期", "时间", "现值", "前值"]].copy()
            show["发布日期"] = show["发布日期"].dt.strftime("%Y-%m-%d")
            show = show.iloc[::-1].reset_index(drop=True)
            show.index += 1
            show.columns = ["发布日期", "统计月", "CPI同比%", "前值%"]
            st.dataframe(
                show.style.format({"CPI同比%": "{:.1f}%", "前值%": "{:.1f}%"}),
                use_container_width=True,
                height=360,
            )
        else:
            st.info("暂无通胀明细")


# ---- 图表 ----

def build_fund_flow_chart(df):
    top20 = df.nlargest(20, "净流入(亿元)").reset_index(drop=True)
    bot20 = df[~df["行业板块"].isin(top20["行业板块"])].nsmallest(20, "净流入(亿元)").iloc[::-1].reset_index(drop=True)
    chart_df = pd.concat([top20, bot20], ignore_index=True)
    colors = ["#ef5350" if v >= 0 else "#26a69a" for v in chart_df["净流入(亿元)"]]

    total_inflow  = df.loc[df["净流入(亿元)"] > 0, "净流入(亿元)"].sum()
    total_outflow = df.loc[df["净流入(亿元)"] < 0, "净流入(亿元)"].sum()
    title_text = (
        f"净流入TOP20 · 净流出TOP20　　"
        f"<span style='color:#ef5350'>净流入合计: {total_inflow:+.2f} 亿元</span>　　"
        f"<span style='color:#26a69a'>净流出合计: {total_outflow:+.2f} 亿元</span>"
    )

    fig = go.Figure(go.Bar(
        x=chart_df["行业板块"],
        y=chart_df["净流入(亿元)"],
        marker_color=colors,
        text=chart_df["净流入(亿元)"].apply(lambda x: f"{x:+.2f}"),
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>净流入: %{y:.2f} 亿元<br><extra></extra>",
    ))
    fig.update_layout(
        title=dict(text=title_text, font=dict(size=14)),
        xaxis_tickangle=-45,
        yaxis_title="净流入(亿元)",
        height=520,
        margin=dict(t=60, b=130),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.add_hline(y=0, line_color="gray", line_width=1)
    return fig


def build_auction_chart(df):
    colors = []
    for v in df["涨跌幅%"]:
        if v >= 1.0:
            colors.append("#ef5350")
        elif v > 0:
            colors.append("#ff8a80")
        elif v <= -1.0:
            colors.append("#26a69a")
        else:
            colors.append("#80cbc4")
    fig = go.Figure(go.Bar(
        x=df["行业板块"],
        y=df["涨跌幅%"],
        marker_color=colors,
        text=df["涨跌幅%"].apply(lambda x: f"{x:+.2f}%"),
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>竞价涨跌幅: %{y:.2f}%<br><extra></extra>",
    ))
    fig.update_layout(
        title="集合竞价 · 各板块涨跌幅",
        xaxis_tickangle=-45,
        yaxis_title="涨跌幅%",
        height=520,
        margin=dict(t=50, b=130),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.add_hline(y=0, line_color="gray", line_width=1)
    return fig


# ---- 页面渲染 ----

def render_auction(df):
    counts = df["开盘状态"].value_counts()
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("高开(≥1%)",  f"{counts.get('高开(≥1%)', 0)} 个", delta="↑", delta_color="normal")
    c2.metric("小幅高开",   f"{counts.get('小幅高开', 0)} 个")
    c3.metric("平开",       f"{counts.get('平开', 0)} 个",      delta_color="off")
    c4.metric("小幅低开",   f"{counts.get('小幅低开', 0)} 个")
    c5.metric("低开(≤-1%)", f"{counts.get('低开(≤-1%)', 0)} 个", delta="↓", delta_color="inverse")

    st.caption(f"集合竞价中（09:15-09:25）　数据每分钟刷新　当前北京时间：{now_bjt().strftime('%H:%M:%S')}")

    st.plotly_chart(build_auction_chart(df), use_container_width=True)

    st.subheader("板块竞价详情")
    show_cols = [c for c in ["行业板块", "涨跌幅%", "上涨家数", "下跌家数", "开盘状态"] if c in df.columns]
    st.dataframe(
        df[show_cols].style.format({
            "涨跌幅%": "{:+.2f}%",
        }),
        use_container_width=True,
        height=600,
    )


def render_fund_flow(df, updated_at, is_open, prev_df=None, turnover="—", zt_total=None, dt_total=None, snapshots=None, load_fn=None):
    import numpy as np
    show_df = df.copy()

    # 盘中斜率：基于当日每5分钟快照做线性回归（至少3个点）
    n_snaps = len(snapshots) if snapshots else 0
    if n_snaps >= 3:
        def _slope(sector):
            vals = [s[sector] for s in snapshots if sector in s and s[sector] is not None]
            if len(vals) < 3:
                return None
            return round(float(np.polyfit(range(len(vals)), vals, 1)[0]), 2)
        show_df["斜率(亿/5min)"] = show_df["行业板块"].apply(_slope)
        slope_up   = int((show_df["斜率(亿/5min)"] > 0).sum())
        slope_down = int((show_df["斜率(亿/5min)"] < 0).sum())
    else:
        show_df["斜率(亿/5min)"] = None  # 列始终存在，数据不足时为空
        slope_up = slope_down = None

    col1, col2, col3, col4, col5, col6, col7, col8 = st.columns(8)
    inflow_count  = int((df["净流入(亿元)"] > 0).sum())
    outflow_count = int((df["净流入(亿元)"] < 0).sum())
    total_count   = len(df)
    top_industry  = df.iloc[0]["行业板块"] if not df.empty else "—"

    d_inflow = d_outflow = None
    if prev_df is not None:
        d_inflow  = int(inflow_count)  - int((prev_df["净流入(亿元)"] > 0).sum())
        d_outflow = int(outflow_count) - int((prev_df["净流入(亿元)"] < 0).sum())

    col1.metric(f"流入板块数（共{total_count}个）", f"{inflow_count} 个",
                delta=f"{d_inflow:+d} 个" if d_inflow is not None else None)
    col2.metric("流出板块数", f"{outflow_count} 个",
                delta=f"{d_outflow:+d} 个" if d_outflow is not None else None,
                delta_color="inverse")
    col3.metric("趋势上升板块", f"{slope_up} 个" if slope_up is not None else "—")
    col4.metric("趋势下降板块", f"{slope_down} 个" if slope_down is not None else "—", delta_color="off")
    col5.metric("今日市场成交额总计", turnover)
    col6.metric("最强板块", top_industry)
    col7.metric("今日涨停", f"{zt_total} 只" if zt_total is not None else "—")
    col8.metric("今日跌停", f"{dt_total} 只" if dt_total is not None else "—")

    slope_hint = f"　　斜率已积累 {n_snaps}/3 个快照{'，计算中' if n_snaps < 3 else ''}" if n_snaps < 3 else ""
    if is_open:
        st.caption(f"最后更新：{updated_at}　　每 {REFRESH_INTERVAL // 60} 分钟自动刷新{slope_hint}")
    else:
        st.caption(f"数据截止：{updated_at}　　非交易时段（09:00-15:30），已停止刷新{slope_hint}")

    st.plotly_chart(build_fund_flow_chart(df), use_container_width=True)

    # 计算每个板块的10日净流入合计
    try:
        history = (load_fn or load_history)()
        today_str = now_bjt().strftime("%Y-%m-%d")
        is_weekday = now_bjt().weekday() < 5
        hist_dates = [d for d in history.keys() if d != today_str]
        def _10d_sum(sector):
            total = sum((history[d].get(sector) or 0) for d in hist_dates)
            if is_weekday:
                cur = show_df.loc[show_df["行业板块"] == sector, "净流入(亿元)"]
                if len(cur) > 0:
                    total += float(cur.values[0])
            return round(total, 2)
        show_df["10日净流入(亿)"] = show_df["行业板块"].apply(_10d_sum)
    except Exception:
        show_df["10日净流入(亿)"] = None

    st.subheader("详细数据")

    display_cols = [c for c in [
        "行业板块", "10日净流入(亿)", "涨跌幅%", "成交额(亿元)", "净流入(亿元)", "净流入率%", "斜率(亿/5min)",
        "流入(亿元)", "流出(亿元)", "涨停数", "领涨股", "领涨股涨跌幅%"
    ] if c in show_df.columns]
    fmt = {
        "涨跌幅%":        "{:+.2f}%",
        "净流入率%":      "{:+.2f}%",
        "成交额(亿元)":   "{:.2f}",
        "净流入(亿元)":   "{:+.2f}",
        "10日净流入(亿)": "{:+.2f}",
        "斜率(亿/5min)":  lambda x: f"{x:+.2f}" if x is not None and not pd.isna(x) else "—",
        "流入(亿元)":     "{:.2f}",
        "流出(亿元)":     "{:.2f}",
        "领涨股涨跌幅%":  "{:+.2f}%",
    }
    st.dataframe(
        show_df[display_cols].style.format({k: v for k, v in fmt.items() if k in display_cols}, na_rep="—"),
        use_container_width=True,
        height=600,
    )


@st.fragment(run_every=AUCTION_INTERVAL)
def show_main_content():
    is_open    = is_market_open()
    is_auction = is_auction_time()

    tab_industry, tab_concept, tab_ztdt, tab_lhb, tab_freq, tab_brent, tab_treasury = st.tabs([
        "📈 行业板块", "💡 概念板块", "🔴 涨停 / 跌停", "🐉 龙虎榜", "🏆 强势板块统计",
        "🛢️ 布伦特原油", "🇺🇸 美债·通胀",
    ])

    # ── 行业板块 Tab ──────────────────────────────────────────
    with tab_industry:
        if "prev_df" not in st.session_state:
            st.session_state["prev_df"] = init_prev_from_db("industry_fund_history")
        if is_auction:
            try:
                df = fetch_auction_data()
                render_auction(df)
            except Exception as e:
                st.error(f"竞价数据获取失败：{e}")
        else:
            try:
                try:
                    new_df, updated_at, turnover = fetch_data()
                    last_df = st.session_state.get("last_df")
                    orig_len = len(new_df)
                    if last_df is not None and orig_len < len(last_df) - 2:
                        missing = last_df[~last_df["行业板块"].isin(new_df["行业板块"])]
                        new_df = pd.concat([new_df, missing]).sort_values("净流入(亿元)", ascending=False).reset_index(drop=True)
                        new_df.index += 1
                        st.caption(f"ℹ️ 新数据 {orig_len} 个板块，缺失 {len(missing)} 个已从缓存补全")
                    today_str = now_bjt().strftime("%Y-%m-%d")
                    if updated_at != st.session_state.get("last_update"):
                        st.session_state["prev_df"]     = last_df
                        st.session_state["last_df"]     = new_df
                        st.session_state["last_update"] = updated_at
                        st.session_state["turnover"]    = turnover
                        # 盘中快照：换日清零，追加当前5分钟数据
                        if st.session_state.get("intraday_snap_date") != today_str:
                            st.session_state["intraday_snapshots"]  = []
                            st.session_state["intraday_snap_date"]  = today_str
                        snap = new_df.set_index("行业板块")["净流入(亿元)"].to_dict()
                        st.session_state["intraday_snapshots"].append(snap)
                        # 交易时段存库；闭市后仅补存一次（当日未存过时）；周末不存
                        is_weekday = now_bjt().weekday() < 5
                        if is_open:
                            save_history(new_df, prev_df=last_df)
                            st.session_state["last_saved_industry_date"] = today_str
                            zt_snap = fetch_zt_total(today_str.replace("-", ""))
                            dt_snap = fetch_dt_count(today_str.replace("-", ""))
                            if zt_snap > 0 or dt_snap > 0:
                                save_zt_dt_history(zt_snap, dt_snap)
                        elif is_weekday and st.session_state.get("last_saved_industry_date") != today_str:
                            save_history(new_df, prev_df=last_df)
                            st.session_state["last_saved_industry_date"] = today_str
                except Exception as fetch_err:
                    if st.session_state.get("last_df") is None:
                        fallback = history_to_df(load_history())
                        if fallback is not None:
                            fb_df, fb_date = fallback
                            st.session_state["last_df"] = fb_df
                            st.session_state["last_update"] = fb_date
                            st.caption(f"⚠️ 实时数据暂不可用，显示 Supabase 历史数据（{fb_date}）")
                        else:
                            st.error(f"数据获取失败且无缓存：{fetch_err}")
                    else:
                        st.caption(f"⚠️ 数据刷新失败（{fetch_err}），显示上次缓存")

                df = st.session_state.get("last_df")
                if df is None:
                    st.warning("暂无行业板块数据")
                else:
                    prev_df    = st.session_state.get("prev_df")
                    updated_at = st.session_state.get("last_update", "—")
                    turnover   = st.session_state.get("turnover", "—")
                    _today = now_bjt().strftime("%Y%m%d")
                    zt_total   = fetch_zt_total(_today)
                    dt_total   = fetch_dt_count(_today)
                    render_fund_flow(df, updated_at, is_open, prev_df, turnover,
                                     zt_total=zt_total, dt_total=dt_total,
                                     snapshots=st.session_state.get("intraday_snapshots", []))
                    show_consecutive_two_day_inflow(df)
            except Exception as e:
                st.error(f"数据获取失败：{e}")

    # ── 概念板块 Tab ──────────────────────────────────────────
    with tab_concept:
        if "prev_concept_df" not in st.session_state:
            st.session_state["prev_concept_df"] = init_prev_from_db("concept_fund_history")
        try:
            try:
                new_df, updated_at = fetch_concept_data()
                if new_df is None:
                    # 接口暂时不可用（结果已缓存，5分钟后自动重试），沿用旧数据
                    if st.session_state.get("last_concept_df") is None:
                        # 兜底：从 Supabase 历史加载最新一天数据
                        fallback = history_to_df(load_concept_history())
                        if fallback is not None:
                            fb_df, fb_date = fallback
                            st.session_state["last_concept_df"] = fb_df
                            st.session_state["last_concept_update"] = fb_date
                            st.caption(f"⚠️ 实时数据暂不可用，显示 Supabase 历史数据（{fb_date}）")
                        else:
                            st.warning("概念板块数据暂时不可用，稍后自动重试")
                    else:
                        st.caption("⚠️ 概念数据暂时不可用，显示上次缓存")
                else:
                    last_df = st.session_state.get("last_concept_df")
                    orig_len = len(new_df)
                    if last_df is not None and orig_len < len(last_df) - 2:
                        missing = last_df[~last_df["行业板块"].isin(new_df["行业板块"])]
                        new_df = pd.concat([new_df, missing]).sort_values("净流入(亿元)", ascending=False).reset_index(drop=True)
                        new_df.index += 1
                        st.caption(f"ℹ️ 新数据 {orig_len} 个板块，缺失 {len(missing)} 个已从缓存补全")
                    today_str = now_bjt().strftime("%Y-%m-%d")
                    if updated_at != st.session_state.get("last_concept_update"):
                        st.session_state["prev_concept_df"]     = last_df
                        st.session_state["last_concept_df"]     = new_df
                        st.session_state["last_concept_update"] = updated_at
                        # 概念板块盘中快照
                        if st.session_state.get("concept_snap_date") != today_str:
                            st.session_state["concept_snapshots"] = []
                            st.session_state["concept_snap_date"] = today_str
                        snap = new_df.set_index("行业板块")["净流入(亿元)"].to_dict()
                        st.session_state["concept_snapshots"].append(snap)
                        if is_open:
                            save_concept_history(new_df, prev_df=last_df)
                            st.session_state["last_saved_concept_date"] = today_str
                        elif is_weekday and st.session_state.get("last_saved_concept_date") != today_str:
                            save_concept_history(new_df, prev_df=last_df)
                            st.session_state["last_saved_concept_date"] = today_str
            except Exception as fetch_err:
                if st.session_state.get("last_concept_df") is None:
                    st.error(f"概念数据获取失败且无缓存：{fetch_err}")
                else:
                    st.caption(f"⚠️ 概念数据刷新失败（{fetch_err}），显示上次缓存")

            df = st.session_state.get("last_concept_df")
            if df is None:
                st.warning("暂无概念板块数据")
            else:
                prev_df    = st.session_state.get("prev_concept_df")
                updated_at = st.session_state.get("last_concept_update", "—")
                turnover   = st.session_state.get("turnover", "—")
                _today = now_bjt().strftime("%Y%m%d")
                zt_total   = fetch_zt_total(_today)
                dt_total   = fetch_dt_count(_today)
                render_fund_flow(df, updated_at, is_open, prev_df, turnover,
                                 zt_total=zt_total, dt_total=dt_total,
                                 snapshots=st.session_state.get("concept_snapshots", []),
                                 load_fn=load_concept_history)
                show_consecutive_two_day_inflow(df, load_fn=load_concept_history)
        except Exception as e:
            st.error(f"概念数据获取失败：{e}")

    # ── 龙虎榜 Tab ────────────────────────────────────────────
    with tab_lhb:
        lhb_df, lhb_jg_stock, lhb_updated, lhb_fallback_date = fetch_lhb_data()

        sub_today, sub_history = st.tabs(["🐉 龙虎榜", "📋 大宗交易"])

        # ── 今日龙虎榜 ─────────────────────────────────────────
        with sub_today:
            if lhb_df is None:
                st.info("龙虎榜数据暂不可用（通常在收盘后更新）")
            else:
                if lhb_fallback_date:
                    st.caption(f"⚠️ 今日实时数据暂不可用，显示最近一期数据（{lhb_fallback_date}）")
                try:
                    # 今日数据存库（仅非回退状态时，每日只存一次）
                    today_str = now_bjt().strftime("%Y-%m-%d")
                    if not lhb_fallback_date and st.session_state.get("lhb_saved_date") != today_str:
                        save_lhb_history(lhb_df)
                        st.session_state["lhb_saved_date"] = today_str

                    # 区分单日行（金额可信）和连续/累计行（金额是多日累计，不可信）
                    is_cumul = lhb_df["上榜原因"].str.contains("连续|累计", na=False) if "上榜原因" in lhb_df.columns else pd.Series(False, index=lhb_df.index)
                    single_df = lhb_df[~is_cumul]
                    cumul_only_codes = set(lhb_df[is_cumul]["代码"]) - set(single_df["代码"]) if "代码" in lhb_df.columns else set()

                    # 单日行按代码去重，保留净买额最大的一行
                    sort_col = "龙虎榜净买额" if "龙虎榜净买额" in single_df.columns else "代码"
                    deduped_single = (single_df
                                      .sort_values(sort_col, ascending=False)
                                      .drop_duplicates(subset=["代码"], keep="first"))

                    # 连续/累计唯一行（只有累计原因的股票）：保留基本信息，金额置空
                    cumul_only_df = (lhb_df[lhb_df["代码"].isin(cumul_only_codes)]
                                     .drop_duplicates(subset=["代码"], keep="first")
                                     .copy())
                    for _col in ["龙虎榜净买额", "龙虎榜买入额", "龙虎榜卖出额", "净买额占总成交比"]:
                        if _col in cumul_only_df.columns:
                            cumul_only_df[_col] = float("nan")

                    deduped_df = pd.concat([deduped_single, cumul_only_df], ignore_index=True)

                    # 合并机构净买额（原始数值）
                    if not lhb_jg_stock.empty:
                        deduped_df = deduped_df.merge(lhb_jg_stock, on="代码", how="left")
                    else:
                        deduped_df["机构净买额_raw"] = float("nan")
                    deduped_df = deduped_df.rename(columns={"机构净买额_raw": "机构净买额(亿)"})

                    # 汇总指标只用单日行（金额可信）
                    total_buy  = deduped_single["龙虎榜买入额"].sum() if "龙虎榜买入额" in deduped_single.columns else float("nan")
                    total_sell = deduped_single["龙虎榜卖出额"].sum() if "龙虎榜卖出额" in deduped_single.columns else float("nan")
                    total_net  = deduped_single["龙虎榜净买额"].sum() if "龙虎榜净买额" in deduped_single.columns else float("nan")
                    jg_net_sum = deduped_df["机构净买额(亿)"].sum() if "机构净买额(亿)" in deduped_df.columns else float("nan")

                    c1, c2, c3, c4, c5, c6 = st.columns(6)
                    c1.metric("上榜股票", f"{len(deduped_df)} 只")
                    c2.metric("龙虎榜买入", f"{total_buy:.2f} 亿")
                    c3.metric("龙虎榜卖出", f"{total_sell:.2f} 亿")
                    c4.metric("龙虎榜净买", f"{total_net:+.2f} 亿")
                    c5.metric("机构净买", f"{jg_net_sum:+.2f} 亿" if pd.notna(jg_net_sum) else "—")
                    c6.metric("数据时间", lhb_updated or "—")

                    # 原始数据表格（东财数据直接展示，不做二次推算）
                    show_cols = [c for c in [
                        "代码", "名称", "涨跌幅",
                        "龙虎榜买入额", "龙虎榜卖出额", "龙虎榜净买额", "机构净买额(亿)",
                        "换手率", "净买额占总成交比", "上榜原因",
                    ] if c in deduped_df.columns]
                    lhb_fmt = {
                        "涨跌幅":           "{:+.2f}%",
                        "龙虎榜买入额":     "{:.2f}",
                        "龙虎榜卖出额":     "{:.2f}",
                        "龙虎榜净买额":     "{:+.2f}",
                        "机构净买额(亿)":   "{:+.2f}",
                        "换手率":           "{:.2f}%",
                        "净买额占总成交比": "{:.2f}%",
                    }
                    fmt = {k: v for k, v in lhb_fmt.items() if k in show_cols}
                    st.dataframe(
                        deduped_df[show_cols].style.format(fmt, na_rep="—"),
                        use_container_width=True, height=600,
                    )

                except Exception as lhb_today_err:
                    st.error(f"龙虎榜今日数据展示失败：{lhb_today_err}")

        # ── 大宗交易子Tab ──────────────────────────────────────
        with sub_history:
            try:
                dzjy_df, dzjy_fallback_date = fetch_dzjy_data()
                if dzjy_df.empty:
                    st.info("大宗交易数据暂不可用（通常在收盘后更新）")
                else:
                    if dzjy_fallback_date:
                        st.caption(f"⚠️ 今日实时数据暂不可用，显示最近一期数据（{dzjy_fallback_date}）")
                    ca, cb = st.columns(2)
                    ca.metric("今日大宗交易", f"{len(dzjy_df)} 笔")
                    if "成交额(亿)" in dzjy_df.columns:
                        cb.metric("合计成交额", f"{dzjy_df['成交额(亿)'].sum():.2f} 亿")

                    # 机构 / 游资交易次数 Top5
                    code_col = "证券代码" if "证券代码" in dzjy_df.columns else "代码"
                    name_col = "证券简称" if "证券简称" in dzjy_df.columns else "名称"
                    if code_col in dzjy_df.columns and name_col in dzjy_df.columns:
                        def _top5(df_sub):
                            if df_sub.empty:
                                return pd.DataFrame()
                            agg = {"交易次数": (code_col, "count")}
                            if "成交额(亿)" in df_sub.columns:
                                agg["合计成交额(亿)"] = ("成交额(亿)", "sum")
                            return (df_sub.groupby([code_col, name_col])
                                         .agg(**agg)
                                         .reset_index()
                                         .sort_values("交易次数", ascending=False)
                                         .head(5)
                                         .reset_index(drop=True))

                        mask_jg = (
                            dzjy_df.get("买方营业部", pd.Series("", index=dzjy_df.index)).str.contains("机构专用", na=False) |
                            dzjy_df.get("卖方营业部", pd.Series("", index=dzjy_df.index)).str.contains("机构专用", na=False)
                        )
                        top5_fmt = {"合计成交额(亿)": "{:.2f}"}
                        col_jg, col_yj = st.columns(2)
                        with col_jg:
                            st.subheader("机构交易次数 Top 5")
                            t = _top5(dzjy_df[mask_jg])
                            if t.empty:
                                st.info("今日无机构参与的大宗交易")
                            else:
                                st.dataframe(t.style.format(top5_fmt), use_container_width=True, hide_index=True)
                        with col_yj:
                            st.subheader("游资交易次数 Top 5")
                            t = _top5(dzjy_df[~mask_jg])
                            if t.empty:
                                st.info("今日无游资参与的大宗交易")
                            else:
                                st.dataframe(t.style.format(top5_fmt), use_container_width=True, hide_index=True)

                    st.subheader("全部明细")
                    dzjy_fmt = {}
                    if "收盘价" in dzjy_df.columns:    dzjy_fmt["收盘价"]    = "{:.2f}"
                    if "成交价" in dzjy_df.columns:    dzjy_fmt["成交价"]    = "{:.2f}"
                    if "折溢率" in dzjy_df.columns:    dzjy_fmt["折溢率"]    = "{:.2f}"
                    if "成交额(亿)" in dzjy_df.columns: dzjy_fmt["成交额(亿)"] = "{:.4f}"
                    st.dataframe(
                        dzjy_df.style.format(dzjy_fmt),
                        use_container_width=True,
                        height=600,
                    )
            except Exception as dzjy_err:
                st.error(f"大宗交易数据加载失败：{dzjy_err}")

    # ── 强势板块统计 Tab ──────────────────────────────────────
    with tab_freq:
        try:
            ind_hist = load_history()
            con_hist = load_concept_history()
            st.caption(f"行业历史：{len(ind_hist)} 个交易日　概念历史：{len(con_hist)} 个交易日")
            st.subheader("行业板块")
            show_top20_frequency(ind_hist, "行业板块")
            st.subheader("概念板块")
            show_top20_frequency(con_hist, "概念板块")
        except Exception as e:
            st.error(f"强势板块统计加载失败：{e}")

    # ── 布伦特原油 Tab ────────────────────────────────────────
    with tab_brent:
        render_brent_tab()

    # ── 美债利率 Tab ──────────────────────────────────────────
    with tab_treasury:
        render_treasury_tab()

    # ── 涨停 / 跌停 Tab ───────────────────────────────────────
    with tab_ztdt:
        _today = now_bjt().strftime("%Y%m%d")
        zt_total = fetch_zt_total(_today)
        dt_total = fetch_dt_count(_today)
        c1, c2 = st.columns(2)
        c1.metric("今日涨停", f"{zt_total} 只")
        c2.metric("今日跌停", f"{dt_total} 只")
        if zt_total == 0 and is_market_open():
            st.warning("涨停池接口暂时无法获取数据，图表已回退到数据库历史值")
        show_zt_dt_trend(zt_total, dt_total)

        # 板块明细
        zt_map = fetch_zt_count(_today)
        dt_map = fetch_dt_sector(_today)
        all_sectors = sorted(set(zt_map) | set(dt_map))
        if all_sectors:
            sector_rows = []
            for s in all_sectors:
                zt_n = zt_map.get(s, 0)
                dt_n = dt_map.get(s, 0)
                sector_rows.append({"板块": s, "涨停": zt_n, "跌停": dt_n})
            sector_df = (pd.DataFrame(sector_rows)
                           .sort_values(["涨停", "跌停"], ascending=[False, False])
                           .reset_index(drop=True))
            sector_df.index += 1
            st.divider()
            st.subheader("今日涨停 / 跌停板块分布（行业）")
            st.dataframe(
                sector_df,
                use_container_width=True,
                height=min(40 * len(sector_df) + 40, 500),
            )



def show_consecutive_two_day_inflow(current_df: pd.DataFrame, load_fn=None):
    """展示最近两个交易日净流入均大于 0 的板块。"""
    history = (load_fn or load_history)()
    today = now_bjt().strftime("%Y-%m-%d")

    # 工作日使用实时数据覆盖数据库中的今日快照；非工作日仅使用历史交易日。
    daily_data = {d: dict(values) for d, values in history.items() if values}
    if now_bjt().weekday() < 5 and current_df is not None and not current_df.empty:
        current = (
            current_df.drop_duplicates(subset="行业板块")
            .set_index("行业板块")["净流入(亿元)"]
        )
        daily_data[today] = {
            str(sector): round(float(value), 2)
            for sector, value in current.items()
            if pd.notna(value)
        }

    trading_dates = sorted(daily_data.keys(), reverse=True)
    st.divider()
    st.subheader("连续2个交易日净流入为正的板块（亿元）")

    if len(trading_dates) < 2:
        st.info("历史数据不足两个交易日，暂时无法判断连续净流入。")
        return

    latest_date, previous_date = trading_dates[:2]
    latest = daily_data[latest_date]
    previous = daily_data[previous_date]
    sectors = sorted(set(latest) & set(previous))

    rows = []
    for sector in sectors:
        latest_value = pd.to_numeric(latest.get(sector), errors="coerce")
        previous_value = pd.to_numeric(previous.get(sector), errors="coerce")
        if pd.notna(latest_value) and pd.notna(previous_value) and latest_value > 0 and previous_value > 0:
            rows.append({
                "板块名称": sector,
                latest_date + ("（实时）" if latest_date == today else ""): round(float(latest_value), 2),
                previous_date: round(float(previous_value), 2),
                "两日合计": round(float(latest_value + previous_value), 2),
            })

    if not rows:
        st.info(f"{previous_date} 与 {latest_date} 没有连续净流入为正的板块。")
        return

    result_df = (
        pd.DataFrame(rows)
        .sort_values("两日合计", ascending=False)
        .reset_index(drop=True)
    )
    result_df.index += 1
    st.caption(f"共 {len(result_df)} 个板块，按两日净流入合计从高到低排列。")
    value_cols = [c for c in result_df.columns if c != "板块名称"]
    st.dataframe(
        result_df.style.format({c: "{:+.2f}" for c in value_cols}),
        use_container_width=True,
        height=min(40 * len(result_df) + 40, 700),
    )



def show_top20_frequency(history: dict, title_prefix: str = "行业板块"):
    """展示历史上净流入TOP5中出现频率最高的板块（竖向柱状图 + 明细表）"""
    if not history:
        st.info("暂无足够历史数据")
        return

    from collections import Counter
    total_days = len(history)
    counter: Counter = Counter()
    net_sum: dict = {}

    for sectors in history.values():
        if not sectors:
            continue
        top20 = sorted(sectors.items(), key=lambda x: x[1] if x[1] is not None else -999, reverse=True)[:5]
        for s, v in top20:
            counter[s] += 1
            net_sum[s] = net_sum.get(s, 0) + (v or 0)

    if not counter:
        return

    freq_df = pd.DataFrame([
        {
            "板块名称":         s,
            "上榜次数(天)":     cnt,
            "上榜率%":          round(cnt / total_days * 100, 1),
            "平均净流入(亿元)": round(net_sum[s] / cnt, 2),
        }
        for s, cnt in counter.most_common(30)
    ])

    st.divider()
    st.subheader(f"{title_prefix} · 净流入TOP5出现频率（近 {total_days} 个交易日）")

    top_df = freq_df.head(20).reset_index(drop=True)
    fig = go.Figure(go.Bar(
        x=top_df["板块名称"],
        y=top_df["上榜次数(天)"],
        marker_color="#ef5350",
        text=top_df.apply(lambda r: f"{int(r['上榜次数(天)'])}天 ({r['上榜率%']}%)", axis=1),
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>上榜次数: %{y} 天<extra></extra>",
    ))
    fig.update_layout(
        height=500,
        margin=dict(t=50, b=10, l=10, r=10),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        yaxis_title="出现天数",
        xaxis_tickangle=-30,
    )
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        freq_df.style.format({"上榜率%": "{:.1f}%", "平均净流入(亿元)": "{:+.2f}"}),
        use_container_width=True,
        hide_index=True,
        height=460,
    )


def show_zt_dt_trend(zt_today: int, dt_today: int):
    """展示近10日涨停/跌停家数趋势"""
    history = load_zt_dt_history()
    today = now_bjt().strftime("%Y-%m-%d")

    # 实时返回0时，优先用数据库已保存的今日数据（避免API失败显示为0）
    today_db = history[history["date"] == today]
    if zt_today == 0 and not today_db.empty and int(today_db["zt_count"].iloc[0]) > 0:
        zt_today = int(today_db["zt_count"].iloc[0])
        dt_today = int(today_db["dt_count"].iloc[0])

    today_row = pd.DataFrame([{"date": today, "zt_count": zt_today, "dt_count": dt_today}])
    df = pd.concat([history[history["date"] != today], today_row], ignore_index=True)
    df = df.sort_values("date", ascending=False).head(10).reset_index(drop=True)
    if df.empty:
        return

    df["date_label"] = df["date"].str[5:]   # 只显示 MM-DD

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["date_label"], y=df["zt_count"], name="涨停家数",
        marker_color="#ef5350",
        text=df["zt_count"], textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=df["date_label"], y=df["dt_count"], name="跌停家数",
        marker_color="#26a69a",
        text=df["dt_count"], textposition="outside",
    ))
    fig.update_layout(
        title="近10日涨停 / 跌停家数",
        barmode="group",
        xaxis_type="category",
        height=360,
        margin=dict(t=50, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.08),
    )
    st.divider()
    st.subheader("近10日涨停 / 跌停统计")
    st.plotly_chart(fig, use_container_width=True)

    table = df[["date", "zt_count", "dt_count"]].rename(
        columns={"date": "日期", "zt_count": "涨停家数", "dt_count": "跌停家数"}
    ).set_index("日期").T
    st.dataframe(table, use_container_width=True)




# ---- 页面入口 ----
st.title("📊 板块资金流向 · 实时")
if is_trading_time():
    st.caption(f"交易时间中，数据每 {REFRESH_INTERVAL // 60} 分钟自动刷新")
else:
    now_str = datetime.now(BJT).strftime("%H:%M")
    st.caption(f"非交易时间（当前 {now_str}），显示最近一次交易日数据，不自动刷新")
show_main_content()

# 存库错误提示（调试用，正常运行时不会出现）；显示后立即清除，防止残留
for _key, _label in [("_save_industry_err", "行业存库"), ("_save_concept_err", "概念存库"), ("_save_zt_dt_err", "涨跌停存库"), ("_save_lhb_err", "龙虎榜存库")]:
    if st.session_state.get(_key):
        st.error(f"[{_label}错误] {st.session_state[_key]}")
        del st.session_state[_key]
