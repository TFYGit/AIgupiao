import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests
from datetime import datetime, timezone, timedelta

BJT = timezone(timedelta(hours=8))
REFRESH_INTERVAL = 300

st.set_page_config(
    page_title="个股资金流向",
    page_icon="💹",
    layout="wide",
)


def now_bjt():
    return datetime.now(BJT)


def parse_amount(s) -> float:
    """把 '1.72亿' / '-879.94万' 之类的字符串转成亿元"""
    if s is None or s == "" or s == "-":
        return float("nan")
    s = str(s).strip()
    try:
        if "亿" in s:
            return float(s.replace("亿", ""))
        elif "万" in s:
            return float(s.replace("万", "")) / 10000
        else:
            return float(s) / 1e8
    except Exception:
        return float("nan")


@st.cache_data(ttl=REFRESH_INTERVAL)
def fetch_stock_flow():
    """东方财富个股资金流向（主力+超大单+大单），含所属行业"""
    url = (
        "https://push2.eastmoney.com/api/qt/clist/get"
        "?pn=1&pz=200&po=1&np=1"
        "&ut=bd1d9ddb04089700cf9c27f6f7426281"
        "&fltt=2&invt=2&fid=f62"
        "&fs=m:0+t:6,m:0+t:13,m:1+t:2,m:1+t:23,m:1+t:23+f:!50"
        "&fields=f12,f14,f2,f3,f62,f184,f66,f69,f72,f75,f78,f81,f100,f6"
    )
    resp = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://data.eastmoney.com/",
    }, timeout=10)
    raw = resp.json()["data"]["diff"]

    rows = []
    for item in raw:
        rows.append({
            "股票代码":    str(item.get("f12", "")).zfill(6),
            "股票名称":    item.get("f14", "—"),
            "最新价":      item.get("f2", 0) if item.get("f2") else 0,
            "涨跌幅%":     item.get("f3", 0) if item.get("f3") else 0,
            "所属行业":    item.get("f100", "—") or "—",
            "主力净流入":  (item.get("f62", 0) or 0) / 1e8,
            "超大单净流入":(item.get("f66", 0) or 0) / 1e8,
            "大单净流入":  (item.get("f72", 0) or 0) / 1e8,
            "中单净流入":  (item.get("f78", 0) or 0) / 1e8,
            "小单净流入":  (item.get("f84", 0) or 0) / 1e8,
            "成交额(亿)":  (item.get("f6",  0) or 0) / 1e8,
        })

    df = pd.DataFrame(rows)
    df = df.sort_values("主力净流入", ascending=False).reset_index(drop=True)
    df.index = df.index + 1
    updated_at = now_bjt().strftime("%Y-%m-%d %H:%M:%S")
    return df, updated_at


def build_bar(df, col, title):
    colors = ["#ef5350" if v >= 0 else "#26a69a" for v in df[col]]
    fig = go.Figure(go.Bar(
        x=df["股票名称"],
        y=df[col],
        marker_color=colors,
        text=df[col].apply(lambda x: f"{x:+.2f}"),
        textposition="outside",
        hovertemplate=(
            "<b>%{x}</b><br>"
            f"{col}: %{{y:.2f}} 亿元<br>"
            "<extra></extra>"
        ),
    ))
    fig.update_layout(
        title=title,
        xaxis_tickangle=-45,
        yaxis_title="亿元",
        height=420,
        margin=dict(t=50, b=100),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    fig.add_hline(y=0, line_color="gray", line_width=1)
    return fig


@st.fragment(run_every=REFRESH_INTERVAL)
def show_content():
    try:
        df, updated_at = fetch_stock_flow()
    except Exception as e:
        st.error(f"数据获取失败：{e}")
        return

    st.caption(f"最后更新：{updated_at}　　每 5 分钟自动刷新　　数据来源：东方财富")

    # ── 顶部汇总指标 ──
    c1, c2, c3, c4 = st.columns(4)
    inflow_n  = (df["主力净流入"] > 0).sum()
    outflow_n = (df["主力净流入"] < 0).sum()
    top_stock = df.iloc[0]["股票名称"] if not df.empty else "—"
    top_ind   = df.iloc[0]["所属行业"] if not df.empty else "—"
    c1.metric("主力净流入股票数", f"{inflow_n} 只")
    c2.metric("主力净流出股票数", f"{outflow_n} 只")
    c3.metric("主力净流入最强", top_stock)
    c4.metric("所属行业", top_ind)

    st.divider()

    # ── Tab 切换 ──
    tab1, tab2, tab3 = st.tabs(["📊 主力净流入 TOP30", "🔍 行业筛选", "📋 完整数据"])

    with tab1:
        top30 = df.head(30)
        bot30 = df.tail(30).iloc[::-1]

        col_l, col_r = st.columns(2)
        with col_l:
            st.plotly_chart(build_bar(top30, "主力净流入", "主力净流入 TOP30"), use_container_width=True)
        with col_r:
            st.plotly_chart(build_bar(bot30, "主力净流入", "主力净流出 TOP30"), use_container_width=True)

        st.subheader("净流入 TOP30 明细")
        fmt = {
            "涨跌幅%":    "{:+.2f}%",
            "主力净流入":  "{:+.2f}",
            "超大单净流入":"{:+.2f}",
            "大单净流入":  "{:+.2f}",
            "中单净流入":  "{:+.2f}",
            "小单净流入":  "{:+.2f}",
            "成交额(亿)":  "{:.2f}",
            "最新价":      "{:.2f}",
        }
        show_cols = ["股票代码", "股票名称", "所属行业", "最新价", "涨跌幅%",
                     "主力净流入", "超大单净流入", "大单净流入", "成交额(亿)"]
        st.dataframe(
            top30[show_cols].style.format({k: v for k, v in fmt.items() if k in show_cols}),
            use_container_width=True, height=600,
        )

    with tab2:
        industries = sorted(df["所属行业"].dropna().unique())
        selected = st.selectbox("选择行业", ["全部"] + industries)
        filtered = df if selected == "全部" else df[df["所属行业"] == selected]
        filtered = filtered.sort_values("主力净流入", ascending=False).reset_index(drop=True)
        filtered.index = filtered.index + 1

        if not filtered.empty:
            st.plotly_chart(build_bar(filtered.head(20), "主力净流入", f"{selected} · 主力净流入排行"), use_container_width=True)
            show_cols2 = ["股票代码", "股票名称", "所属行业", "最新价", "涨跌幅%",
                          "主力净流入", "超大单净流入", "大单净流入", "成交额(亿)"]
            st.dataframe(
                filtered[show_cols2].style.format({k: v for k, v in fmt.items() if k in show_cols2}),
                use_container_width=True, height=600,
            )

    with tab3:
        all_cols = ["股票代码", "股票名称", "所属行业", "最新价", "涨跌幅%",
                    "主力净流入", "超大单净流入", "大单净流入", "中单净流入", "小单净流入", "成交额(亿)"]
        st.dataframe(
            df[all_cols].style.format({k: v for k, v in fmt.items() if k in all_cols}),
            use_container_width=True, height=700,
        )


# ── 页面入口 ──
st.title("💹 个股资金流向 · 实时")
show_content()
