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
MARKET_CLOSE  = (15, 30)
PAGE_REFRESH_INTERVAL = 60
ROTATION_REFRESH_INTERVAL = 120  # 行业轮动2分钟请求一次


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


@st.cache_data(ttl=ROTATION_REFRESH_INTERVAL)
def fetch_industry_rotation():
    """获取东方财富 APP 行业轮动：最近60个交易日主力净流入 TOP10。"""
    url = "https://datacenter.eastmoney.com/securities/api/data/v1/get"
    params = {
        "reportName": "RPT_BOARD_WHEEL",
        "columns": "BOARD_CODE,BOARD_NAME,TRADE_DATE,INDICATORID,INDICATORID_RANK,COMMON_TYPE3",
        "filter": '(COMMON_TYPE1="003")(COMMON_TYPE2="2")(COMMON_TYPE3="01")(INDICATORID_RANK<=10)',
        "source": "SECURITIES",
        "client": "APP",
        "sortColumns": "TRADE_DATE,INDICATORID_RANK,COMMON_TYPE3",
        "sortTypes": "1,1,1",
    }
    resp = requests.get(
        url,
        params=params,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://emdata.eastmoney.com/appdc/bkld/index.html",
        },
        timeout=30,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not payload.get("success"):
        raise ValueError(payload.get("message") or "行业轮动接口返回失败")

    rows = ((payload.get("result") or {}).get("data") or [])
    if not rows:
        raise ValueError("行业轮动数据为空")

    df = pd.DataFrame(rows).rename(columns={
        "BOARD_CODE": "板块代码",
        "BOARD_NAME": "行业板块",
        "TRADE_DATE": "交易日期",
        "INDICATORID": "主力净流入(元)",
        "INDICATORID_RANK": "排名",
    })
    required = ["板块代码", "行业板块", "交易日期", "主力净流入(元)", "排名"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"行业轮动接口缺少字段：{', '.join(missing)}")

    df["交易日期"] = pd.to_datetime(df["交易日期"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["主力净流入(元)"] = pd.to_numeric(df["主力净流入(元)"], errors="coerce")
    df["主力净流入(亿元)"] = (df["主力净流入(元)"] / 1e8).round(2)
    df["排名"] = pd.to_numeric(df["排名"], errors="coerce")
    df = df.dropna(subset=["交易日期", "行业板块", "主力净流入(亿元)", "排名"])
    df["排名"] = df["排名"].astype(int)

    live_status = "历史报表"
    try:
        live_params = {
            "ut": "a1f568701ae683c054e7fa0b13833a5d",
            "fltt": 2,
            "invt": 2,
            "fields": "f62,f12,f14,f124,f223",
            "pn": 1,
            "pz": 200,
            "po": 1,
            "np": 1,
            "fs": "m:90+s:4",
            "fid": "f62",
        }
        live_headers = {
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://emdata.eastmoney.com/appdc/bkld/index.html",
        }
        live_payload = None
        for live_host in [
            "80.push2.eastmoney.com",
            "81.push2.eastmoney.com",
            "79.push2.eastmoney.com",
            "82.push2.eastmoney.com",
            "push2delay.eastmoney.com",
            "push2.eastmoney.com",
        ]:
            try:
                live_resp = requests.get(
                    f"https://{live_host}/api/qt/clist/get",
                    params=live_params,
                    headers=live_headers,
                    timeout=12,
                )
                live_resp.raise_for_status()
                candidate = live_resp.json()
                if (candidate.get("data") or {}).get("diff"):
                    live_payload = candidate
                    break
            except Exception:
                continue
        if live_payload is None:
            raise ValueError("实时行业行情接口暂不可用")
        live_diff = ((live_payload.get("data") or {}).get("diff") or [])
        live_rows = list(live_diff.values()) if isinstance(live_diff, dict) else live_diff
        live_df = pd.DataFrame(live_rows).rename(columns={
            "f12": "板块代码",
            "f14": "行业板块",
            "f62": "主力净流入(元)",
            "f124": "行情时间戳",
        })
        if not live_df.empty and all(col in live_df.columns for col in ["板块代码", "行业板块", "主力净流入(元)"]):
            live_df["主力净流入(元)"] = pd.to_numeric(live_df["主力净流入(元)"], errors="coerce")
            live_df["行情时间戳"] = pd.to_numeric(live_df.get("行情时间戳"), errors="coerce")
            live_df = live_df.dropna(subset=["行业板块", "主力净流入(元)"])
            live_df = live_df.sort_values("主力净流入(元)", ascending=False).head(10).reset_index(drop=True)
            quote_ts = live_df["行情时间戳"].dropna().max()
            quote_date = (datetime.fromtimestamp(float(quote_ts), tz=BJT).strftime("%Y-%m-%d")
                          if pd.notna(quote_ts) else now_bjt().strftime("%Y-%m-%d"))
            if quote_date >= df["交易日期"].max():
                live_df["交易日期"] = quote_date
                live_df["排名"] = range(1, len(live_df) + 1)
                live_df["主力净流入(亿元)"] = (live_df["主力净流入(元)"] / 1e8).round(2)
                df = pd.concat(
                    [df[df["交易日期"] != quote_date], live_df[required + ["主力净流入(亿元)"]]],
                    ignore_index=True,
                )
                live_status = "历史报表 + 当日实时行情"
    except Exception:
        # 实时接口偶发限流时仍展示完整60日历史，并在页面标明当前使用的来源。
        live_status = "历史报表（实时行情暂不可用）"

    dates = sorted(df["交易日期"].unique(), reverse=True)[:60]
    df = df[df["交易日期"].isin(dates)].copy()
    df = df.sort_values(
        ["交易日期", "主力净流入(亿元)", "排名"],
        ascending=[False, False, True],
    ).reset_index(drop=True)
    return df, now_bjt().strftime("%Y-%m-%d %H:%M:%S"), live_status


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

# ---- 页面渲染 ----

def render_industry_rotation(df: pd.DataFrame, updated_at: str, source_status: str):
    """展示按主力净流入排序的近60个交易日行业轮动。"""
    if df is None or df.empty:
        st.info("暂无行业轮动数据")
        return

    dates = sorted(df["交易日期"].unique(), reverse=True)
    latest_date = dates[0]
    latest = (df[df["交易日期"] == latest_date]
              .sort_values(["主力净流入(亿元)", "排名"], ascending=[False, True])
              .reset_index(drop=True))
    top = latest.iloc[0]

    st.subheader("行业轮动 · 主力净流入 TOP10")
    st.caption(
        f"数据源：东方财富 APP 板块轮动（{source_status}）　　覆盖 {len(dates)} 个交易日　　"
        f"最近请求：{updated_at}　　每 {ROTATION_REFRESH_INTERVAL // 60} 分钟自动请求最新数据"
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("最新交易日", latest_date)
    c2.metric("当日净流入第一", str(top["行业板块"]), f"{top['主力净流入(亿元)']:+.2f} 亿元")
    c3.metric("TOP10 净流入合计", f"{latest['主力净流入(亿元)'].sum():+.2f} 亿元")
    c4.metric("历史记录", f"{len(df)} 条")

    colors = ["#ef5350" if value >= 0 else "#26a69a" for value in latest["主力净流入(亿元)"]]
    fig = go.Figure(go.Bar(
        x=latest["行业板块"],
        y=latest["主力净流入(亿元)"],
        marker_color=colors,
        text=latest["主力净流入(亿元)"].map(lambda value: f"{value:+.2f}"),
        textposition="outside",
        customdata=latest[["排名", "板块代码"]],
        hovertemplate=(
            "<b>%{x}</b><br>排名：%{customdata[0]}<br>"
            "主力净流入：%{y:.2f} 亿元<br>板块代码：%{customdata[1]}<extra></extra>"
        ),
    ))
    fig.update_layout(
        title=f"{latest_date} 主力净流入排名",
        xaxis_tickangle=-35,
        yaxis_title="主力净流入(亿元)",
        height=460,
        margin=dict(t=55, b=110),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.add_hline(y=0, line_color="gray", line_width=1)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("最近60个交易日轮动明细")
    matrix_source = df.copy()
    matrix_source["名次"] = matrix_source["排名"].map(lambda value: f"第{value}名")
    matrix_source["板块 / 净流入"] = matrix_source.apply(
        lambda row: f"{row['行业板块']}  {row['主力净流入(亿元)']:+.2f}亿", axis=1
    )
    matrix = matrix_source.pivot_table(
        index="交易日期",
        columns="名次",
        values="板块 / 净流入",
        aggfunc="first",
    )
    rank_columns = [f"第{rank}名" for rank in range(1, 11)]
    matrix = matrix.reindex(index=dates, columns=rank_columns).reset_index()
    matrix.columns.name = None

    board_frequency = (
        df.groupby("行业板块")["交易日期"]
        .nunique()
        .sort_values(ascending=False)
    )
    no_highlight = "— 不高亮 —"
    selected_board = st.selectbox(
        "选择要高亮的行业板块",
        options=[no_highlight, *board_frequency.index.tolist()],
        key="rotation_highlight_board",
        help="选中后，下方60个交易日明细里所有相同板块都会高亮显示。",
    )
    matrix_view = matrix
    if selected_board != no_highlight:
        highlight_prefix = f"{selected_board}  "

        def highlight_board(value):
            if isinstance(value, str) and value.startswith(highlight_prefix):
                return (
                    "background-color: #7a1f1f; color: #ffffff; font-weight: 700; "
                    "border: 2px solid #ff4b4b;"
                )
            return ""

        matrix_view = matrix.style
        if hasattr(matrix_view, "map"):
            matrix_view = matrix_view.map(highlight_board)
        else:
            matrix_view = matrix_view.applymap(highlight_board)
        st.caption(
            f"已高亮「{selected_board}」：最近{len(dates)}个交易日共进入TOP10 "
            f"{int(board_frequency[selected_board])} 次。"
        )
    st.dataframe(matrix_view, use_container_width=True, hide_index=True, height=900)

    def render_rotation_stats(period: int):
        stats_dates = dates[:period]
        stats_df = df[df["交易日期"].isin(stats_dates)]
        count_col = f"{period}日轮动次数"
        rate_col = f"{period}日入榜率"
        st.subheader(f"当日TOP10 · 近{period}个交易日轮动次数")
        st.caption(f"统计当天前10个行业在最近{len(stats_dates)}个交易日进入主力净流入TOP10的天数。")
        rotation_counts = (
            stats_df.groupby("板块代码", as_index=False)
            .agg(
                **{
                    count_col: ("交易日期", "nunique"),
                    "期间最佳排名": ("排名", "min"),
                }
            )
        )
        latest_rotation = (
            latest[["排名", "板块代码", "行业板块", "主力净流入(亿元)"]]
            .merge(rotation_counts, on="板块代码", how="left")
            .rename(columns={"排名": "当日排名", "主力净流入(亿元)": "当日主力净流入(亿元)"})
            .sort_values("当日排名")
        )
        latest_rotation[rate_col] = (
            latest_rotation[count_col] / len(stats_dates) * 100
        ).round(1)
        latest_rotation[count_col] = latest_rotation[count_col].fillna(0).astype(int)
        latest_rotation["期间最佳排名"] = latest_rotation["期间最佳排名"].fillna(0).astype(int)
        st.dataframe(
            latest_rotation,
            column_order=[
                "当日排名", "行业板块", "板块代码", "当日主力净流入(亿元)",
                count_col, rate_col, "期间最佳排名",
            ],
            column_config={
                "当日主力净流入(亿元)": st.column_config.NumberColumn(format="%+.2f"),
                count_col: st.column_config.ProgressColumn(
                    count_col,
                    help=f"最近{len(stats_dates)}个交易日进入主力净流入TOP10的天数",
                    min_value=0,
                    max_value=len(stats_dates),
                    format="%d 次",
                ),
                rate_col: st.column_config.NumberColumn(format="%.1f%%"),
            },
            use_container_width=True,
            hide_index=True,
            height=388,
        )

    hot_period = 20
    hot_dates = dates[:hot_period]
    hot_df = df[df["交易日期"].isin(hot_dates)]
    hot_count_col = f"{hot_period}日轮动次数"
    hot_rate_col = f"{hot_period}日入榜率"
    hot_ranking = (
        hot_df.groupby(["板块代码", "行业板块"], as_index=False)
        .agg(
            **{
                hot_count_col: ("交易日期", "nunique"),
                "期间最佳排名": ("排名", "min"),
                "期间平均排名": ("排名", "mean"),
                "TOP10净流入合计(亿元)": ("主力净流入(亿元)", "sum"),
            }
        )
        .sort_values(
            [hot_count_col, "期间平均排名", "TOP10净流入合计(亿元)"],
            ascending=[False, True, False],
        )
        .head(10)
        .reset_index(drop=True)
    )
    hot_ranking.insert(0, "热度排名", range(1, len(hot_ranking) + 1))
    hot_ranking[hot_rate_col] = (hot_ranking[hot_count_col] / len(hot_dates) * 100).round(1)
    leader = hot_ranking.iloc[0]

    st.subheader(f"近{hot_period}个交易日 · 轮动次数最多的板块")
    st.caption(
        f"统计最近{len(hot_dates)}个交易日所有进入过主力净流入TOP10的行业；"
        "次数相同时按平均排名、净流入合计排序。"
    )
    leader_c1, leader_c2, leader_c3 = st.columns(3)
    leader_c1.metric("轮动最强板块", str(leader["行业板块"]))
    leader_c2.metric("进入TOP10", f"{int(leader[hot_count_col])} 次")
    leader_c3.metric(f"{hot_period}日入榜率", f"{leader[hot_rate_col]:.1f}%")
    st.dataframe(
        hot_ranking,
        column_order=[
            "热度排名", "行业板块", "板块代码", hot_count_col, hot_rate_col,
            "期间最佳排名", "期间平均排名", "TOP10净流入合计(亿元)",
        ],
        column_config={
            hot_count_col: st.column_config.ProgressColumn(
                hot_count_col,
                min_value=0,
                max_value=len(hot_dates),
                format="%d 次",
            ),
            hot_rate_col: st.column_config.NumberColumn(format="%.1f%%"),
            "期间平均排名": st.column_config.NumberColumn(format="%.2f"),
            "TOP10净流入合计(亿元)": st.column_config.NumberColumn(format="%+.2f"),
        },
        use_container_width=True,
        hide_index=True,
        height=388,
    )

    render_rotation_stats(20)
    render_rotation_stats(60)


@st.fragment(run_every=PAGE_REFRESH_INTERVAL)
def show_main_content():
    tab_ztdt, tab_lhb, tab_brent, tab_treasury, tab_rotation = st.tabs([
        "🔴 涨停 / 跌停", "🐉 龙虎榜", "🛢️ 布伦特原油", "🇺🇸 美债·通胀", "🔄 行业轮动",
    ])

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

    # ── 布伦特原油 Tab ────────────────────────────────────────
    with tab_brent:
        render_brent_tab()

    # ── 美债利率 Tab ──────────────────────────────────────────
    with tab_treasury:
        render_treasury_tab()

    # ── 行业轮动 Tab ──────────────────────────────────────────
    with tab_rotation:
        try:
            rotation_df, rotation_updated, rotation_source = fetch_industry_rotation()
            st.session_state["industry_rotation_df"] = rotation_df
            st.session_state["industry_rotation_updated"] = rotation_updated
            st.session_state["industry_rotation_source"] = rotation_source
        except Exception as rotation_err:
            rotation_df = st.session_state.get("industry_rotation_df")
            rotation_updated = st.session_state.get("industry_rotation_updated", "—")
            rotation_source = st.session_state.get("industry_rotation_source", "上次缓存")
            if rotation_df is None:
                st.error(f"行业轮动数据获取失败：{rotation_err}")
            else:
                st.caption(f"⚠️ 最新数据请求失败（{rotation_err}），显示上次缓存")
        if rotation_df is not None:
            render_industry_rotation(rotation_df, rotation_updated, rotation_source)

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
        zt_dt_snapshot = (_today, zt_total, dt_total)
        if (zt_total > 0 or dt_total > 0) and st.session_state.get("last_saved_zt_dt") != zt_dt_snapshot:
            save_zt_dt_history(zt_total, dt_total)
            st.session_state["last_saved_zt_dt"] = zt_dt_snapshot
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
for _key, _label in [("_save_zt_dt_err", "涨跌停存库"), ("_save_lhb_err", "龙虎榜存库")]:
    if st.session_state.get(_key):
        st.error(f"[{_label}错误] {st.session_state[_key]}")
        del st.session_state[_key]
