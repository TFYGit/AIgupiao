import time
import streamlit as st
import akshare as ak
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(
    page_title="行业资金流向",
    page_icon="📊",
    layout="wide",
)

REFRESH_INTERVAL = 300  # 5分钟


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
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 顶部指标
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
        }).background_gradient(subset=["净流入(亿元)"], cmap="RdYlGn"),
        use_container_width=True,
        height=600,
    )

except Exception as e:
    st.error(f"数据获取失败：{e}")

# 5分钟后自动刷新
time.sleep(REFRESH_INTERVAL)
st.rerun()
