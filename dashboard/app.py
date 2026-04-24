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


def load_history() -> dict:
    """从 Supabase 加载近10个交易日所有板块数据，格式: {日期: {行业: 净流入}}"""
    try:
        sb = get_supabase()
        rows = (sb.table("industry_fund_history")
                  .select("date,industry,net_inflow")
                  .order("date", desc=False)
                  .execute().data)
        history = {}
        for r in rows:
            d = str(r["date"])
            history.setdefault(d, {})[r["industry"]] = r["net_inflow"]
        # 只取最近10个交易日
        dates = sorted(history.keys())[-10:]
        return {d: history[d] for d in dates}
    except Exception:
        return {}


def save_history(df: pd.DataFrame, prev_df: pd.DataFrame = None):
    """把所有板块当天净流入 upsert 到 Supabase"""
    today = now_bjt().strftime("%Y-%m-%d")
    try:
        sb = get_supabase()
        prev_map = {}
        if prev_df is not None and "行业板块" in prev_df.columns:
            prev_map = prev_df.set_index("行业板块")["净流入(亿元)"].to_dict()
        rows = []
        for _, row in df[["行业板块", "净流入(亿元)"]].iterrows():
            board = row["行业板块"]
            net   = round(float(row["净流入(亿元)"]), 2)
            change = round(net - float(prev_map[board]), 2) if board in prev_map else None
            rows.append({"date": today, "industry": board,
                         "net_inflow": net, "net_inflow_change": change})
        sb.table("industry_fund_history").upsert(rows, on_conflict="date,industry").execute()
    except Exception:
        pass


def load_concept_history() -> dict:
    """从 Supabase 加载近10个交易日所有概念板块数据，格式: {日期: {概念: 净流入}}"""
    try:
        sb = get_supabase()
        rows = (sb.table("concept_fund_history")
                  .select("date,industry,net_inflow")
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
        prev_map = {}
        if prev_df is not None and "行业板块" in prev_df.columns:
            prev_map = prev_df.set_index("行业板块")["净流入(亿元)"].to_dict()
        rows = []
        for _, row in df[["行业板块", "净流入(亿元)"]].iterrows():
            board = row["行业板块"]
            net   = round(float(row["净流入(亿元)"]), 2)
            change = round(net - float(prev_map[board]), 2) if board in prev_map else None
            rows.append({"date": today, "industry": board,
                         "net_inflow": net, "net_inflow_change": change})
        sb.table("concept_fund_history").upsert(rows, on_conflict="date,industry").execute()
    except Exception:
        pass


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
_EM_URL = "https://push2.eastmoney.com/api/qt/clist/get"
_EM_BASE = {
    "pn": 1, "pz": 200, "po": 1, "np": 1,
    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
    "fltt": 2, "invt": 2, "fid": "f62",
    "fs": "m:90+t:2+f:!50",
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
        secids = ["1.000001", "0.399001", "0.899050"]
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
    import threading
    result, error = [None], [None]
    def _run():
        try:
            result[0] = ak.stock_fund_flow_concept(symbol="即时")
        except Exception as e:
            error[0] = e
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(60)
    if t.is_alive():
        raise TimeoutError("概念板块资金流向接口超时，稍后重试")
    if error[0]:
        raise error[0]
    df = result[0]
    if df is None or df.empty:
        raise ValueError("概念板块数据为空，稍后重试")

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


def render_fund_flow(df, updated_at, is_open, prev_df=None, turnover="—"):
    # 提前计算环比，供 metric 和表格共用
    show_df = df.copy()
    if prev_df is not None and "行业板块" in prev_df.columns:
        prev_map = prev_df.set_index("行业板块")["净流入(亿元)"].to_dict()
        show_df["环比(亿元)"] = show_df["行业板块"].map(
            lambda x: show_df.loc[show_df["行业板块"] == x, "净流入(亿元)"].values[0] - prev_map.get(x, float("nan"))
            if x in prev_map else float("nan")
        )
        qob_up   = int((show_df["环比(亿元)"] > 0).sum())
        qob_down = int((show_df["环比(亿元)"] < 0).sum())
    else:
        qob_up = qob_down = None

    col1, col2, col3, col4, col5, col6 = st.columns(6)
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
    col3.metric("环比上升板块", f"{qob_up} 个" if qob_up is not None else "—")
    col4.metric("环比下降板块", f"{qob_down} 个" if qob_down is not None else "—", delta_color="off")
    col5.metric("今日市场成交额总计", turnover)
    col6.metric("最强板块", top_industry)

    if is_open:
        st.caption(f"最后更新：{updated_at}　　每 {REFRESH_INTERVAL // 60} 分钟自动刷新")
    else:
        st.caption(f"数据截止：{updated_at}　　非交易时段（09:00-15:30），已停止刷新")

    st.plotly_chart(build_fund_flow_chart(df), use_container_width=True)

    st.subheader("详细数据")

    display_cols = [c for c in [
        "行业板块", "涨跌幅%", "成交额(亿元)", "净流入(亿元)", "净流入率%", "环比(亿元)",
        "流入(亿元)", "流出(亿元)", "涨停数", "领涨股", "领涨股涨跌幅%"
    ] if c in show_df.columns]
    fmt = {
        "涨跌幅%":      "{:+.2f}%",
        "净流入率%":    "{:+.2f}%",
        "成交额(亿元)": "{:.2f}",
        "净流入(亿元)": "{:+.2f}",
        "环比(亿元)":   "{:+.2f}",
        "流入(亿元)":   "{:.2f}",
        "流出(亿元)":   "{:.2f}",
        "领涨股涨跌幅%":"{:+.2f}%",
    }
    st.dataframe(
        show_df[display_cols].style.format({k: v for k, v in fmt.items() if k in display_cols}),
        use_container_width=True,
        height=600,
    )


@st.fragment(run_every=AUCTION_INTERVAL)
def show_main_content():
    is_open    = is_market_open()
    is_auction = is_auction_time()

    tab_industry, tab_concept = st.tabs(["📈 行业板块", "💡 概念板块"])

    # ── 行业板块 Tab ──────────────────────────────────────────
    with tab_industry:
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
                    if updated_at != st.session_state.get("last_update"):
                        st.session_state["prev_df"]     = last_df
                        st.session_state["last_df"]     = new_df
                        st.session_state["last_update"] = updated_at
                        st.session_state["turnover"]    = turnover
                        if is_open:
                            save_history(new_df, prev_df=last_df)
                except Exception as fetch_err:
                    if st.session_state.get("last_df") is None:
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
                    render_fund_flow(df, updated_at, is_open, prev_df, turnover)
                    show_top5_history(df)
            except Exception as e:
                st.error(f"数据获取失败：{e}")

    # ── 概念板块 Tab ──────────────────────────────────────────
    with tab_concept:
        try:
            try:
                new_df, updated_at = fetch_concept_data()
                last_df = st.session_state.get("last_concept_df")
                orig_len = len(new_df)
                if last_df is not None and orig_len < len(last_df) - 2:
                    missing = last_df[~last_df["行业板块"].isin(new_df["行业板块"])]
                    new_df = pd.concat([new_df, missing]).sort_values("净流入(亿元)", ascending=False).reset_index(drop=True)
                    new_df.index += 1
                    st.caption(f"ℹ️ 新数据 {orig_len} 个板块，缺失 {len(missing)} 个已从缓存补全")
                if updated_at != st.session_state.get("last_concept_update"):
                    st.session_state["prev_concept_df"]     = last_df
                    st.session_state["last_concept_df"]     = new_df
                    st.session_state["last_concept_update"] = updated_at
                    if is_open:
                        save_concept_history(new_df, prev_df=last_df)
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
                render_fund_flow(df, updated_at, is_open, prev_df, turnover)
                show_top5_history(df, load_fn=load_concept_history)
        except Exception as e:
            st.error(f"概念数据获取失败：{e}")


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


# ---- 页面入口 ----
st.title("📊 板块资金流向 · 实时")
show_main_content()
