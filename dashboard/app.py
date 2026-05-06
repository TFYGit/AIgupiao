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

REFRESH_INTERVAL = 300       # 5分钟
AUCTION_INTERVAL = 60        # 集合竞价1分钟刷新
MARKET_OPEN   = (9,  0)
AUCTION_START = (9, 15)
AUCTION_END   = (9, 25)
MARKET_CLOSE  = (15, 30)


@st.cache_resource
def get_supabase():
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)


def now_bjt():
    return datetime.now(BJT)


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_history() -> dict:
    """从 Supabase 加载近10个交易日所有板块数据，格式: {日期: {行业: 净流入}}"""
    try:
        from datetime import date, timedelta
        sb = get_supabase()
        start = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        rows = (sb.table("industry_fund_history")
                  .select("date,industry,net_inflow")
                  .gte("date", start)
                  .order("date", desc=False)
                  .execute().data)
        history = {}
        for r in rows:
            d = str(r["date"])
            history.setdefault(d, {})[r["industry"]] = r["net_inflow"]
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


@st.cache_data(ttl=REFRESH_INTERVAL)
def load_concept_history() -> dict:
    """从 Supabase 加载近10个交易日所有概念板块数据，格式: {日期: {概念: 净流入}}"""
    try:
        from datetime import date, timedelta
        sb = get_supabase()
        start = (date.today() - timedelta(days=30)).strftime("%Y-%m-%d")
        rows = (sb.table("concept_fund_history")
                  .select("date,industry,net_inflow")
                  .gte("date", start)
                  .order("date", desc=False)
                  .execute().data)
        history = {}
        for r in rows:
            d = str(r["date"])
            history.setdefault(d, {})[r["industry"]] = r["net_inflow"]
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
        return pd.DataFrame(rows).rename(columns={
            "date": "上榜日", "code": "代码", "name": "名称", "reason": "上榜原因",
            "change_pct": "涨跌幅", "net_buy": "龙虎榜净买额",
            "buy_amount": "龙虎榜买入额", "sell_amount": "龙虎榜卖出额",
            "turnover": "换手率", "net_buy_ratio": "净买额占总成交比",
        })
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
def fetch_zt_count() -> dict:
    """用akshare涨停池统计各行业涨停家数"""
    import threading
    result, error = [None], [None]
    def _run():
        try:
            today = now_bjt().strftime("%Y%m%d")
            result[0] = ak.stock_zt_pool_em(date=today)
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
def fetch_zt_total() -> int:
    """直接查涨停池，返回今日涨停总家数"""
    import threading
    result, error = [None], [None]
    def _run():
        try:
            today = now_bjt().strftime("%Y%m%d")
            result[0] = ak.stock_zt_pool_em(date=today)
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(15)
    if t.is_alive() or error[0] or result[0] is None or result[0].empty:
        return 0
    return len(result[0])


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_dt_count() -> int:
    """直接查跌停池，返回今日跌停总家数"""
    import threading
    result, error = [None], [None]
    def _run():
        try:
            today = now_bjt().strftime("%Y%m%d")
            result[0] = ak.stock_zt_pool_dtgc_em(date=today)
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(15)
    if t.is_alive() or error[0] or result[0] is None or result[0].empty:
        return 0
    return len(result[0])



@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_lhb_data():
    """获取今日龙虎榜数据，无数据时返回 (None, None) 而非抛异常（保证结果被缓存，避免重复阻塞）"""
    import threading
    result, error = [None], [None]
    def _run():
        try:
            today = now_bjt().strftime("%Y%m%d")
            result[0] = ak.stock_lhb_detail_em(start_date=today, end_date=today)
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(30)
    if t.is_alive() or error[0] or result[0] is None or result[0].empty:
        return None, None
    df = result[0]
    for col in ["龙虎榜净买额", "龙虎榜买入额", "龙虎榜卖出额", "龙虎榜成交额", "市场总成交额", "流通市值"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce") / 1e8
    for col in ["涨跌幅", "换手率", "净买额占总成交比", "成交额占总成交比"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df, now_bjt().strftime("%Y-%m-%d %H:%M:%S")


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_lhb_jg_flow() -> pd.DataFrame:
    """获取近30日龙虎榜机构买卖统计，按日期汇总返回（亿元）。失败返回空 DataFrame。"""
    import threading
    from datetime import timedelta
    result, error = [None], [None]
    def _run():
        try:
            end   = now_bjt().strftime("%Y%m%d")
            start = (now_bjt() - timedelta(days=30)).strftime("%Y%m%d")
            result[0] = ak.stock_lhb_jgmmtj_em(start_date=start, end_date=end)
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(30)
    if t.is_alive() or error[0] or result[0] is None or result[0].empty:
        return pd.DataFrame()
    df = result[0]
    date_col = next((c for c in ["上榜日期", "上榜日", "日期"] if c in df.columns), None)
    net_col  = next((c for c in ["机构买入净额", "机构净买额", "净买额"] if c in df.columns), None)
    if not date_col or not net_col:
        return pd.DataFrame()
    # 同一股票同天可能多行（多原因），按 (日期, 代码) 去重再汇总
    code_col = next((c for c in ["代码", "股票代码"] if c in df.columns), None)
    if code_col:
        df = df.drop_duplicates(subset=[date_col, code_col])
    df[net_col] = pd.to_numeric(df[net_col], errors="coerce") / 1e8
    daily = df.groupby(date_col)[net_col].sum().reset_index()
    daily.columns = ["date", "jg_net"]
    daily["date"] = daily["date"].astype(str).str[:10]
    return daily.sort_values("date")


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

    zt_map = fetch_zt_count()
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


def render_fund_flow(df, updated_at, is_open, prev_df=None, turnover="—", zt_total=None, dt_total=None, snapshots=None):
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

    st.subheader("详细数据")

    display_cols = [c for c in [
        "行业板块", "涨跌幅%", "成交额(亿元)", "净流入(亿元)", "净流入率%", "斜率(亿/5min)",
        "流入(亿元)", "流出(亿元)", "涨停数", "领涨股", "领涨股涨跌幅%"
    ] if c in show_df.columns]
    fmt = {
        "涨跌幅%":        "{:+.2f}%",
        "净流入率%":      "{:+.2f}%",
        "成交额(亿元)":   "{:.2f}",
        "净流入(亿元)":   "{:+.2f}",
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

    tab_industry, tab_concept, tab_ztdt, tab_lhb, tab_freq = st.tabs(["📈 行业板块", "💡 概念板块", "🔴 涨停 / 跌停", "🐉 龙虎榜", "🏆 强势板块统计"])

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
                        if is_open:
                            save_history(new_df, prev_df=last_df)
                            st.session_state["last_saved_industry_date"] = today_str
                            zt_snap = fetch_zt_total()
                            dt_snap = fetch_dt_count()
                            save_zt_dt_history(zt_snap, dt_snap)
                    elif is_open and st.session_state.get("last_saved_industry_date") != today_str:
                        # 保底：当日尚未存过则补存一次（应对 Streamlit 重启后缓存 updated_at 未变化的情况）
                        df_to_save = st.session_state.get("last_df")
                        if df_to_save is not None:
                            save_history(df_to_save)
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
                    zt_total   = fetch_zt_total()
                    dt_total   = fetch_dt_count()
                    render_fund_flow(df, updated_at, is_open, prev_df, turnover,
                                     zt_total=zt_total, dt_total=dt_total,
                                     snapshots=st.session_state.get("intraday_snapshots", []))
                    show_top5_history(df)
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
                    elif is_open and st.session_state.get("last_saved_concept_date") != today_str:
                        df_to_save = st.session_state.get("last_concept_df")
                        if df_to_save is not None:
                            save_concept_history(df_to_save)
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
                zt_total   = fetch_zt_total()
                dt_total   = fetch_dt_count()
                render_fund_flow(df, updated_at, is_open, prev_df, turnover,
                                 zt_total=zt_total, dt_total=dt_total,
                                 snapshots=st.session_state.get("concept_snapshots", []))
                show_top5_history(df, load_fn=load_concept_history)
        except Exception as e:
            st.error(f"概念数据获取失败：{e}")

    # ── 龙虎榜 Tab ────────────────────────────────────────────
    with tab_lhb:
        lhb_df, lhb_updated = fetch_lhb_data()

        lhb_fmt = {
            "涨跌幅":           "{:+.2f}%",
            "换手率":           "{:.2f}%",
            "龙虎榜净买额":     "{:+.2f}",
            "龙虎榜买入额":     "{:.2f}",
            "龙虎榜卖出额":     "{:.2f}",
            "净买额占总成交比": "{:.2f}%",
            "成交额占总成交比": "{:.2f}%",
        }

        st.subheader("机构 vs 游资资金动向")
        show_lhb_flow_breakdown()
        st.divider()

        sub_today, sub_history = st.tabs(["📅 今日", "📂 历史记录（近30日）"])

        # ── 今日子Tab ─────────────────────────────────────────
        with sub_today:
            if lhb_df is None:
                st.info("今日暂无龙虎榜数据（通常在收盘后更新）")
            else:
                try:
                    # 今日数据存库（每日只存一次）
                    today_str = now_bjt().strftime("%Y-%m-%d")
                    if st.session_state.get("lhb_saved_date") != today_str:
                        save_lhb_history(lhb_df)
                        st.session_state["lhb_saved_date"] = today_str

                    total_stocks  = lhb_df["代码"].nunique()
                    net_buy_col   = "龙虎榜净买额" if "龙虎榜净买额" in lhb_df.columns else None
                    top_code = top_name = "—"
                    if net_buy_col:
                        total_net_buy = lhb_df.groupby("代码")[net_buy_col].sum()
                        if not total_net_buy.empty:
                            top_code = total_net_buy.idxmax()
                            matched = lhb_df.loc[lhb_df["代码"] == top_code, "名称"]
                            top_name = matched.iloc[0] if not matched.empty else "—"

                    c1, c2, c3, c4 = st.columns(4)
                    c1.metric("上榜股票数", f"{total_stocks} 只")
                    c2.metric("上榜记录数", f"{len(lhb_df)} 条")
                    c3.metric("净买入最强", f"{top_name}({top_code})")
                    c4.metric("数据时间", lhb_updated)
                    st.caption("同一股票可能因多个原因多次上榜，每条原因单独一行")

                    show_cols = [c for c in ["代码", "名称", "上榜原因", "涨跌幅", "龙虎榜净买额",
                                 "龙虎榜买入额", "龙虎榜卖出额", "换手率", "净买额占总成交比"] if c in lhb_df.columns]
                    all_reasons = sorted(lhb_df["上榜原因"].dropna().unique().tolist())
                    sel_reasons = st.multiselect("筛选上榜原因（不选则显示全部）", options=all_reasons, default=[], key="lhb_today_reason")
                    filtered = lhb_df if not sel_reasons else lhb_df[lhb_df["上榜原因"].isin(sel_reasons)]
                    sort_col = net_buy_col or "代码"
                    st.dataframe(
                        filtered[show_cols].sort_values(sort_col, ascending=False)
                            .reset_index(drop=True)
                            .style.format({k: v for k, v in lhb_fmt.items() if k in show_cols}),
                        use_container_width=True, height=520,
                    )

                    st.subheader("上榜原因分布")
                    reason_counts = lhb_df["上榜原因"].value_counts().reset_index()
                    reason_counts.columns = ["上榜原因", "上榜次数"]
                    st.dataframe(reason_counts, use_container_width=True, hide_index=True)
                except Exception as lhb_today_err:
                    st.error(f"龙虎榜今日数据展示失败：{lhb_today_err}")

            # 上榜原因详解
            with st.expander("📖 上榜原因详解（点击展开）"):
                st.markdown("""
**交易所规定以下情形的证券当日收盘后纳入龙虎榜，公开披露前五大买入/卖出席位：**

---

#### 一、单日涨跌幅异常
| 原因 | 触发条件 |
|------|---------|
| 日涨幅偏离值达到7% | 当日收盘涨幅偏离值 ≥ +7%，取前5只（涨幅最高） |
| 日跌幅偏离值达到7% | 当日收盘跌幅偏离值 ≥ -7%，取前5只（跌幅最深） |
| 日振幅值达到15% | 当日振幅（最高-最低/昨收）≥ 15%，取前5只 |
| 日涨幅达到15% | 当日收盘涨幅 ≥ 15%（主要针对20cm股/科创板/创业板） |
| 日跌幅达到15% | 当日收盘跌幅 ≥ -15% |

#### 二、连续多日累计偏离异常
| 原因 | 触发条件 |
|------|---------|
| 连续3日累计涨幅偏离 ≥ 20% | 普通证券3日内涨幅偏离值累计 ≥ 20% |
| 连续3日累计涨幅偏离 ≥ 30% | 普通证券3日内涨幅偏离值累计 ≥ 30% |
| 连续3日累计涨幅偏离 ≥ 12% | ST / *ST 证券3日内累计 ≥ 12% |
| 连续3日累计跌幅偏离 ≥ 12% | ST / *ST 证券3日内跌幅累计 ≥ 12% |

#### 三、换手率异常
| 原因 | 触发条件 |
|------|---------|
| 日换手率达到20% | 当日换手率 ≥ 20%，取前5只 |
| 日换手率达到30% | 当日换手率 ≥ 30%，取前5只 |

#### 四、可转债
| 原因 | 触发条件 |
|------|---------|
| 日涨幅达到15%的可转债 | 可转债当日涨幅 ≥ 15% |
| 连续3日累计涨幅偏离 ≥ 30% 的可转债 | 可转债3日内偏离 ≥ 30% |

---

> **偏离值说明**：偏离值 = 个股涨跌幅 - 同期上证综指/深证成指涨跌幅。
> 反映的是"相对市场"的异常涨跌，而非绝对涨跌幅。
> **上榜 ≠ 利好/利空**，龙虎榜只是异常交易的预警，需结合基本面和机构席位方向综合判断。
""")

        # ── 历史记录子Tab ──────────────────────────────────────
        with sub_history:
            hist_df = load_lhb_history()
            if hist_df.empty:
                st.info("暂无历史数据，请先运行回填脚本或等待今日数据存入。")
            else:
                hist_show_cols = [c for c in ["上榜日", "代码", "名称", "上榜原因", "涨跌幅",
                                  "龙虎榜净买额", "龙虎榜买入额", "龙虎榜卖出额",
                                  "换手率", "净买额占总成交比"] if c in hist_df.columns]

                fa, fb, fc = st.columns([2, 2, 3])
                with fa:
                    date_options = ["全部"] + sorted(hist_df["上榜日"].dropna().unique().tolist(), reverse=True)
                    sel_date = st.selectbox("按日期筛选", date_options, key="lhb_hist_date")
                with fb:
                    kw = st.text_input("股票代码 / 名称搜索", key="lhb_hist_kw")
                with fc:
                    hist_reasons = sorted(hist_df["上榜原因"].dropna().unique().tolist())
                    sel_hist_reasons = st.multiselect("上榜原因", hist_reasons, default=[], key="lhb_hist_reason")

                view = hist_df.copy()
                if sel_date != "全部":
                    view = view[view["上榜日"] == sel_date]
                if kw:
                    view = view[view["代码"].str.contains(kw, na=False) | view["名称"].str.contains(kw, na=False)]
                if sel_hist_reasons:
                    view = view[view["上榜原因"].isin(sel_hist_reasons)]

                st.caption(f"共 {len(view)} 条记录")
                st.dataframe(
                    view[hist_show_cols].reset_index(drop=True)
                        .style.format({k: v for k, v in lhb_fmt.items() if k in hist_show_cols}),
                    use_container_width=True,
                    height=600,
                )

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

    # ── 涨停 / 跌停 Tab ───────────────────────────────────────
    with tab_ztdt:
        zt_total = fetch_zt_total()
        dt_total = fetch_dt_count()
        c1, c2 = st.columns(2)
        c1.metric("今日涨停", f"{zt_total} 只")
        c2.metric("今日跌停", f"{dt_total} 只")
        show_zt_dt_trend(zt_total, dt_total)


def show_top5_history(current_df: pd.DataFrame, load_fn=None):
    """页面底部展示近10日净流入TOP5趋势"""
    current_df = current_df.drop_duplicates(subset="行业板块")
    today = now_bjt().strftime("%Y-%m-%d")
    history = (load_fn or load_history)()

    # 今日TOP5行业
    industries = current_df.nlargest(5, "净流入(亿元)")["行业板块"].tolist()

    # 历史日期（不含今日，避免重复）
    hist_dates = sorted(d for d in history.keys() if d != today)

    # 构建表格：行=行业，列=历史日期+今日实时
    rows = []
    for ind in industries:
        row = {"行业板块": ind}
        for d in hist_dates:
            val = history[d].get(ind)
            row[d] = val
        cur = current_df.loc[current_df["行业板块"] == ind, "净流入(亿元)"]
        row[today + "（实时）"] = round(float(cur.values[0]), 2) if len(cur) > 0 else None
        rows.append(row)

    table_df = pd.DataFrame(rows).set_index("行业板块")
    # 最新数据（实时）放第一列，历史日期降序排列
    today_col = today + "（实时）"
    hist_cols = sorted(hist_dates, reverse=True)
    ordered_cols = [c for c in [today_col] + hist_cols if c in table_df.columns]
    table_df = table_df[ordered_cols]

    # 5日净值合计列
    table_df["10日合计"] = table_df[ordered_cols].apply(
        lambda row: round(row.dropna().sum(), 2), axis=1
    )

    st.divider()
    st.subheader("净流入TOP5 · 近10日统计（亿元）")

    def fmt(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—"
        return f"{v:+.2f}"

    st.dataframe(
        table_df.style.format(fmt),
        use_container_width=True,
    )

    # 净流出TOP5
    bot_industries = current_df.nsmallest(5, "净流入(亿元)")["行业板块"].tolist()
    bot_rows = []
    for ind in bot_industries:
        row = {"行业板块": ind}
        for d in hist_dates:
            val = history[d].get(ind)
            row[d] = val
        cur = current_df.loc[current_df["行业板块"] == ind, "净流入(亿元)"]
        row[today_col] = round(float(cur.values[0]), 2) if len(cur) > 0 else None
        bot_rows.append(row)

    bot_df = pd.DataFrame(bot_rows).set_index("行业板块")
    bot_df = bot_df[[c for c in [today_col] + hist_cols if c in bot_df.columns]]
    bot_df["10日合计"] = bot_df.apply(lambda row: round(row.dropna().sum(), 2), axis=1)

    st.subheader("净流出TOP5 · 近10日统计（亿元）")
    st.dataframe(
        bot_df.style.format(fmt),
        use_container_width=True,
    )

    # 近10日合计流入TOP5：基于历史+实时数据，取合计最大的5个行业
    all_industries = current_df["行业板块"].tolist()
    sum_rows = []
    for ind in all_industries:
        row = {"行业板块": ind}
        for d in hist_dates:
            val = history[d].get(ind)
            row[d] = val
        cur = current_df.loc[current_df["行业板块"] == ind, "净流入(亿元)"]
        row[today_col] = round(float(cur.values[0]), 2) if len(cur) > 0 else None
        sum_rows.append(row)

    sum_df = pd.DataFrame(sum_rows).set_index("行业板块")
    sum_df = sum_df[[c for c in [today_col] + hist_cols if c in sum_df.columns]]
    sum_df["10日合计"] = sum_df.apply(lambda row: round(row.dropna().sum(), 2), axis=1)
    top5_sum_df = sum_df.nlargest(5, "10日合计")

    st.subheader("近10日合计净流入TOP5（亿元）")
    st.dataframe(
        top5_sum_df.style.format(fmt),
        use_container_width=True,
    )


def show_top20_frequency(history: dict, title_prefix: str = "行业板块"):
    """展示历史上净流入TOP20中出现频率最高的板块（横向柱状图 + 明细表）"""
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
        top20 = sorted(sectors.items(), key=lambda x: x[1] if x[1] is not None else -999, reverse=True)[:20]
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
    st.subheader(f"{title_prefix} · 净流入TOP20出现频率（全量历史，共 {total_days} 个交易日）")

    top_df = freq_df.head(20).iloc[::-1].reset_index(drop=True)
    fig = go.Figure(go.Bar(
        y=top_df["板块名称"],
        x=top_df["上榜次数(天)"],
        orientation="h",
        marker_color="#ef5350",
        text=top_df.apply(lambda r: f"{int(r['上榜次数(天)'])}天 ({r['上榜率%']}%)", axis=1),
        textposition="outside",
        hovertemplate="<b>%{y}</b><br>上榜次数: %{x} 天<extra></extra>",
    ))
    fig.update_layout(
        height=600,
        margin=dict(t=30, b=30, l=10, r=120),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis_title="出现天数",
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


def show_lhb_flow_breakdown():
    """龙虎榜 机构 vs 游资净买入近期趋势图"""
    jg_df   = fetch_lhb_jg_flow()
    hist_df = load_lhb_history()

    if jg_df.empty or hist_df.empty:
        st.info("暂无足够数据生成机构/游资对比图")
        return

    net_col = "龙虎榜净买额"
    if net_col not in hist_df.columns:
        st.info("历史数据中缺少净买额字段")
        return
    deduped     = hist_df.drop_duplicates(subset=["上榜日", "代码"])
    daily_total = (deduped.groupby("上榜日")[net_col]
                          .sum().reset_index()
                          .rename(columns={"上榜日": "date", net_col: "total_net"}))

    # 今日数据用实时接口覆盖，避免 Supabase 快照与机构实时数据时间不同步
    today = now_bjt().strftime("%Y-%m-%d")
    rt_df, _ = fetch_lhb_data()
    if rt_df is not None and not rt_df.empty and net_col in rt_df.columns:
        date_col_rt = next((c for c in ["上榜日", "上榜日期", "日期"] if c in rt_df.columns), None)
        code_col_rt = next((c for c in ["代码", "股票代码"] if c in rt_df.columns), None)
        if date_col_rt and code_col_rt:
            rt_deduped = rt_df.drop_duplicates(subset=[date_col_rt, code_col_rt])
            today_total = rt_deduped[net_col].sum()
            daily_total = daily_total[daily_total["date"] != today]
            daily_total = pd.concat(
                [daily_total, pd.DataFrame([{"date": today, "total_net": today_total}])],
                ignore_index=True,
            )

    merged = pd.merge(daily_total, jg_df, on="date", how="inner")
    if merged.empty:
        st.info("机构数据与历史数据无交集，请稍后重试")
        return

    merged["yj_net"] = merged["total_net"] - merged["jg_net"]
    merged = merged.sort_values("date", ascending=False).head(20).reset_index(drop=True)
    merged["label"] = merged["date"].str[5:]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=merged["label"], y=merged["jg_net"], name="机构净买入",
        marker_color="#FF8C00",
        text=merged["jg_net"].apply(lambda v: f"{v:+.1f}"),
        textposition="outside",
    ))
    fig.add_trace(go.Bar(
        x=merged["label"], y=merged["yj_net"], name="游资净买入",
        marker_color="#1E90FF",
        text=merged["yj_net"].apply(lambda v: f"{v:+.1f}"),
        textposition="outside",
    ))
    fig.update_layout(
        title="龙虎榜 · 机构 vs 游资净买入（亿元，最新在左）",
        barmode="group",
        xaxis_type="category",
        height=420,
        margin=dict(t=55, b=40),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        legend=dict(orientation="h", y=1.1),
        yaxis_title="净买入(亿元)",
    )
    fig.add_hline(y=0, line_color="gray", line_width=1)
    st.plotly_chart(fig, use_container_width=True)

    tbl = merged[["date", "jg_net", "yj_net", "total_net"]].rename(columns={
        "date": "日期", "jg_net": "机构净买(亿)", "yj_net": "游资净买(亿)", "total_net": "龙虎榜合计(亿)"
    }).set_index("日期")
    st.dataframe(tbl.style.format("{:+.2f}"), use_container_width=True)
    st.caption("机构 = 龙虎榜机构专用席位合计；游资 = 龙虎榜营业部席位合计（总净买 − 机构净买）")


# ---- 页面入口 ----
st.title("📊 板块资金流向 · 实时")
show_main_content()

# 存库错误提示（调试用，正常运行时不会出现）；显示后立即清除，防止残留
for _key, _label in [("_save_industry_err", "行业存库"), ("_save_concept_err", "概念存库"), ("_save_zt_dt_err", "涨跌停存库"), ("_save_lhb_err", "龙虎榜存库")]:
    if st.session_state.get(_key):
        st.error(f"[{_label}错误] {st.session_state[_key]}")
        del st.session_state[_key]
