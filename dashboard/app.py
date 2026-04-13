import time
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

REFRESH_INTERVAL = 300  # 5分钟

INDEX_CODES = {
    "000001": "上证指数",
    "399001": "深证成指",
    "399006": "创业板指",
    "000300": "沪深300",
}


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_indices():
    import requests
    # 1=上交所 0=深交所
    secids = "1.000001,0.399001,0.399006,1.000300"
    url = (
        "https://push2.eastmoney.com/api/qt/ulist.np/get"
        f"?secids={secids}&fields=f12,f14,f2,f3,f4"
        "&ut=bd1d9ddb04089700cf9c27f6f7426281"
    )
    resp = requests.get(url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
    items = resp.json()["data"]["diff"]
    rows = []
    for item in items:
        rows.append({
            "代码": str(item["f12"]),
            "名称": item["f14"],
            "最新价": item["f2"] / 100,
            "涨跌幅": item["f3"] / 100,
            "涨跌额": item["f4"] / 100,
        })
    df = pd.DataFrame(rows)
    # 按指定顺序排列
    order = list(INDEX_CODES.keys())
    df["sort"] = df["代码"].map({c: i for i, c in enumerate(order)})
    df = df.sort_values("sort").drop(columns="sort")
    return df.set_index("代码")


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
    df = df.sort_values("净流入(亿元)", ascending=False).reset_index(drop=True)
    df.index = df.index + 1  # 从1开始
    return df


def build_chart(df):
    colors = ["#ef5350" if v >= 0 else "#26a69a" for v in df["净流入(亿元)"]]
    fig = go.Figure(go.Bar(
        x=df["行业板块"],
        y=df["净流入(亿元)"],
        marker_color=colors,
        text=df["净流入(亿元)"].apply(lambda x: f"{x:+.2f}"),
        textposition="outside",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "净流入: %{y:.2f} 亿元<br>"
            "<extra></extra>"
        ),
    ))
    fig.update_layout(
        title="各行业净流入排行（亿元）",
        xaxis_tickangle=-45,
        yaxis_title="净流入(亿元)",
        height=500,
        margin=dict(t=50, b=120),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.add_hline(y=0, line_color="gray", line_width=1)
    return fig


# ---- 页面 ----
st.title("📊 行业资金流向 · 实时")

try:
    df = fetch_data()
    updated_at = datetime.now(BJT).strftime("%Y-%m-%d %H:%M:%S")

    # 大盘指数
    try:
        idx_df = fetch_indices()
        idx_cols = st.columns(len(INDEX_CODES))
        for i, (code, name) in enumerate(INDEX_CODES.items()):
            if code in idx_df.index:
                row = idx_df.loc[code]
                delta = f"{row['涨跌额']:+.2f}  ({row['涨跌幅']:+.2f}%)"
                idx_cols[i].metric(name, f"{row['最新价']:.2f}", delta)
    except Exception:
        pass

    st.divider()

    # 行业统计指标
    col1, col2, col3, col4 = st.columns(4)
    inflow_count = (df["净流入(亿元)"] > 0).sum()
    outflow_count = (df["净流入(亿元)"] < 0).sum()
    total_vol = df["成交额(亿元)"].sum()
    top_industry = df.iloc[0]["行业板块"] if not df.empty else "—"

    col1.metric("流入行业数", f"{inflow_count} 个")
    col2.metric("流出行业数", f"{outflow_count} 个")
    col3.metric("全市场成交额", f"{total_vol:.0f} 亿元")
    col4.metric("最强行业", top_industry)

    st.caption(f"最后更新：{updated_at}　　每 5 分钟自动刷新")

    # 柱状图
    st.plotly_chart(build_chart(df), use_container_width=True)

    # 数据表格
    st.subheader("详细数据")
    display_cols = [c for c in [
        "行业板块", "涨跌幅%", "成交额(亿元)", "净流入(亿元)",
        "流入(亿元)", "流出(亿元)", "领涨股", "领涨股涨跌幅%"
    ] if c in df.columns]

    st.dataframe(
        df[display_cols].style.format({
            "涨跌幅%": "{:+.2f}%",
            "成交额(亿元)": "{:.2f}",
            "净流入(亿元)": "{:+.2f}",
            "流入(亿元)": "{:.2f}",
            "流出(亿元)": "{:.2f}",
            "领涨股涨跌幅%": "{:+.2f}%",
        }),
        use_container_width=True,
        height=600,
    )

except Exception as e:
    st.error(f"数据获取失败：{e}")

# 5分钟后自动刷新
time.sleep(REFRESH_INTERVAL)
st.rerun()
