import json
import os
import streamlit as st
import akshare as ak
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timezone, timedelta

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
HISTORY_FILE  = os.path.join(os.path.dirname(__file__), "top5_history.json")


def now_bjt():
    return datetime.now(BJT)


def load_history() -> dict:
    """加载历史TOP5数据，格式: {日期: {行业: 净流入}}"""
    try:
        if os.path.exists(HISTORY_FILE):
            with open(HISTORY_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def save_history(df: pd.DataFrame):
    """把当前数据的TOP5净流入存入历史文件"""
    today = now_bjt().strftime("%Y-%m-%d")
    history = load_history()
    top5 = df.nlargest(5, "净流入(亿元)")[["行业板块", "净流入(亿元)"]].copy()
    history[today] = {row["行业板块"]: round(float(row["净流入(亿元)"]), 2)
                      for _, row in top5.iterrows()}
    # 只保留最近5个交易日
    dates = sorted(history.keys())[-5:]
    history = {d: history[d] for d in dates}
    try:
        with open(HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def is_market_open() -> bool:
    t = (now_bjt().hour, now_bjt().minute)
    return MARKET_OPEN <= t <= MARKET_CLOSE


def is_auction_time() -> bool:
    t = (now_bjt().hour, now_bjt().minute)
    return AUCTION_START <= t <= AUCTION_END


# ---- 数据获取 ----

@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_market_turnover() -> str:
    import requests
    try:
        hdrs = {"User-Agent": "Mozilla/5.0"}
        sh = requests.get("https://push2.eastmoney.com/api/qt/stock/get?secid=1.000001&fields=f48",
                          headers=hdrs, timeout=8).json()["data"]["f48"]
        sz = requests.get("https://push2.eastmoney.com/api/qt/stock/get?secid=0.399001&fields=f48",
                          headers=hdrs, timeout=8).json()["data"]["f48"]
        return f"{(sh + sz) / 1e8:.0f} 亿元"
    except Exception:
        return "—"


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_data():
    df = ak.stock_fund_flow_industry(symbol="即时")
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
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["成交额(亿元)"] = df["流入(亿元)"].fillna(0) + df["流出(亿元)"].fillna(0)
    df["净流入率%"] = (df["净流入(亿元)"] / df["成交额(亿元)"].replace(0, float("nan")) * 100).round(2)
    df = df.sort_values("净流入(亿元)", ascending=False).reset_index(drop=True)
    df.index = df.index + 1
    updated_at = now_bjt().strftime("%Y-%m-%d %H:%M:%S")
    return df, updated_at


@st.cache_data(ttl=AUCTION_INTERVAL)
def fetch_auction_data():
    """集合竞价期间：各板块高开/低开/平开分布"""
    df = ak.stock_board_industry_spot_em()
    # 统一列名
    rename = {}
    for col in df.columns:
        if "名称" in col or col == "板块名称":
            rename[col] = "行业板块"
        elif "涨跌幅" in col:
            rename[col] = "涨跌幅%"
        elif "上涨" in col:
            rename[col] = "上涨家数"
        elif "下跌" in col:
            rename[col] = "下跌家数"
    df = df.rename(columns=rename)

    if "涨跌幅%" not in df.columns:
        # 尝试找第一个数值列作为涨跌幅
        num_cols = df.select_dtypes(include="number").columns
        if len(num_cols) > 0:
            df = df.rename(columns={num_cols[0]: "涨跌幅%"})

    df["涨跌幅%"] = pd.to_numeric(df.get("涨跌幅%", 0), errors="coerce").fillna(0)

    # 分类
    def classify(v):
        if v >= 1.0:
            return "高开(≥1%)"
        elif v <= -1.0:
            return "低开(≤-1%)"
        elif v > 0:
            return "小幅高开"
        elif v < 0:
            return "小幅低开"
        else:
            return "平开"

    df["开盘状态"] = df["涨跌幅%"].apply(classify)
    df = df.sort_values("涨跌幅%", ascending=False).reset_index(drop=True)
    df.index = df.index + 1
    return df


# ---- 图表 ----

def build_fund_flow_chart(df):
    top20 = df.nlargest(20, "净流入(亿元)")
    bot20 = df.nsmallest(20, "净流入(亿元)").iloc[::-1]
    chart_df = pd.concat([top20, bot20])
    colors = ["#ef5350" if v >= 0 else "#26a69a" for v in chart_df["净流入(亿元)"]]
    fig = go.Figure(go.Bar(
        x=chart_df["行业板块"],
        y=chart_df["净流入(亿元)"],
        marker_color=colors,
        text=chart_df["净流入(亿元)"].apply(lambda x: f"{x:+.2f}"),
        textposition="outside",
        hovertemplate="<b>%{x}</b><br>净流入: %{y:.2f} 亿元<br><extra></extra>",
    ))
    fig.update_layout(
        title="净流入TOP20 · 净流出TOP20",
        xaxis_tickangle=-45,
        yaxis_title="净流入(亿元)",
        height=520,
        margin=dict(t=50, b=130),
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


def render_fund_flow(df, updated_at, is_open, prev_df=None):
    col1, col2, col3, col4 = st.columns(4)
    inflow_count  = (df["净流入(亿元)"] > 0).sum()
    outflow_count = (df["净流入(亿元)"] < 0).sum()
    top_industry  = df.iloc[0]["行业板块"] if not df.empty else "—"
    turnover = fetch_market_turnover()

    # 环比delta
    d_inflow = d_outflow = None
    if prev_df is not None:
        d_inflow  = int(inflow_count)  - int((prev_df["净流入(亿元)"] > 0).sum())
        d_outflow = int(outflow_count) - int((prev_df["净流入(亿元)"] < 0).sum())

    col1.metric("流入行业数", f"{inflow_count} 个",
                delta=f"{d_inflow:+d} 个" if d_inflow is not None else None)
    col2.metric("流出行业数", f"{outflow_count} 个",
                delta=f"{d_outflow:+d} 个" if d_outflow is not None else None,
                delta_color="inverse")
    col3.metric("沪深成交额", turnover)
    col4.metric("最强行业",   top_industry)

    if is_open:
        st.caption(f"最后更新：{updated_at}　　每 5 分钟自动刷新")
    else:
        st.caption(f"数据截止：{updated_at}　　非交易时段（09:00-15:30），已停止刷新")

    st.plotly_chart(build_fund_flow_chart(df), use_container_width=True)

    # 表格加净流入环比列
    st.subheader("详细数据")
    show_df = df.copy()
    if prev_df is not None:
        prev_map = prev_df.set_index("行业板块")["净流入(亿元)"].to_dict() if "行业板块" in prev_df.columns else {}
        show_df["环比(亿元)"] = show_df["行业板块"].map(
            lambda x: show_df.loc[show_df["行业板块"] == x, "净流入(亿元)"].values[0] - prev_map.get(x, float("nan"))
            if x in prev_map else float("nan")
        )

    display_cols = [c for c in [
        "行业板块", "涨跌幅%", "成交额(亿元)", "净流入(亿元)", "净流入率%", "环比(亿元)",
        "流入(亿元)", "流出(亿元)", "领涨股", "领涨股涨跌幅%"
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

    # 集合竞价时段
    if is_auction:
        try:
            df = fetch_auction_data()
            render_auction(df)
        except Exception as e:
            st.error(f"竞价数据获取失败：{e}")
        return

    # 正常交易/收盘展示资金流向
    try:
        if is_open:
            new_df, updated_at = fetch_data()
            # 如果拿到了新数据（时间戳变了），把旧数据存为prev
            if updated_at != st.session_state.get("last_update"):
                st.session_state["prev_df"]     = st.session_state.get("last_df")
                st.session_state["last_df"]     = new_df
                st.session_state["last_update"] = updated_at
                save_history(new_df)
            df = st.session_state["last_df"]
        elif "last_df" in st.session_state:
            df         = st.session_state["last_df"]
            updated_at = st.session_state.get("last_update", "—")
        else:
            df, updated_at = fetch_data()
            st.session_state["last_df"]     = df
            st.session_state["last_update"] = updated_at

        prev_df    = st.session_state.get("prev_df")
        updated_at = st.session_state.get("last_update", "—")
        render_fund_flow(df, updated_at, is_open, prev_df)
        show_top5_history(df)

    except Exception as e:
        st.error(f"数据获取失败：{e}")


def show_top5_history(current_df: pd.DataFrame):
    """页面底部展示近5日净流入TOP5趋势"""
    history = load_history()
    if not history:
        return

    today = now_bjt().strftime("%Y-%m-%d")
    # 用当前实时数据覆盖今日记录（每次执行脚本刷新）
    top5_today = current_df.nlargest(5, "净流入(亿元)")["行业板块"].tolist()

    # 确定要展示的行业：今日TOP5
    industries = top5_today

    # 构建表格：行=行业，列=日期
    dates = sorted(history.keys())
    rows = []
    for ind in industries:
        row = {"行业板块": ind}
        for d in dates:
            val = history[d].get(ind)
            row[d] = val if val is not None else None
        # 今日实时值
        cur = current_df.loc[current_df["行业板块"] == ind, "净流入(亿元)"]
        row[today + "（实时）"] = round(float(cur.values[0]), 2) if len(cur) > 0 else None
        rows.append(row)

    table_df = pd.DataFrame(rows).set_index("行业板块")

    st.divider()
    st.subheader("净流入TOP5 · 近5日统计（亿元）")
    # 格式化：有值显示+xx，None显示—
    def fmt(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—"
        return f"{v:+.2f}"

    st.dataframe(
        table_df.style.format(fmt),
        use_container_width=True,
    )


# ---- 页面入口 ----
st.title("📊 行业资金流向 · 实时")
show_main_content()
