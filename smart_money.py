# ============================================================
# 机构动向 & 聪明钱信号捕捉系统
# Author: Quant Pro
# 数据源：AKShare（机构调研 / 大宗交易 / 北向资金 / 龙虎榜）
# ============================================================

import os, sys, io, re, time, warnings, threading
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

# ── 代理绕过（必须在 import akshare 之前）──────────────────
import requests
requests.utils.get_environ_proxies = lambda *a, **kw: {}
_orig_menv = requests.Session.merge_environment_settings
def _no_proxy(self, url, proxies, stream, verify, cert):
    result = _orig_menv(self, url, proxies, stream, verify, cert)
    result["proxies"] = {}
    return result
requests.Session.merge_environment_settings = _no_proxy

import akshare as ak

# ── 输出目录 ──────────────────────────────────────────────
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)

TODAY      = datetime.now().strftime("%Y%m%d")
TODAY_DASH = datetime.now().strftime("%Y-%m-%d")
WEEK_AGO   = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
THREE_DAYS = (datetime.now() - timedelta(days=3)).strftime("%Y%m%d")

# ── 关注主题关键词 ────────────────────────────────────────
FOCUS_THEMES = {
    "氢能源":   ["氢能", "氢燃料", "氢电池", "绿氢", "制氢", "储氢", "液氢",
                 "氢化物", "PEM", "电解槽", "加氢站", "氢动力", "亿华通", "美锦能源",
                 "厚普股份", "国富氢能", "中鼎股份"],
    "核电":     ["核电", "核能", "小堆", "核聚变", "核裂变", "铀", "核反应",
                 "中广核", "华能", "秦山", "三门核", "中国核", "大亚湾",
                 "核建", "同位素", "反应堆", "重水堆", "压水堆", "核燃料",
                 "核仪器", "辐照", "乏燃料"],
    "航空航天": ["航空", "航天", "卫星", "火箭", "飞机", "无人机", "导弹", "军工",
                 "歼击机", "直升机", "运载", "空天", "北斗", "遥感", "载人",
                 "深空", "探月", "高超声速", "发动机", "航发", "中航"],
    "算电协同": ["算力", "数据中心", "算电", "AI芯片", "GPU", "智算", "超算",
                 "大模型", "推理", "训练芯片", "AIGC", "算网", "智能计算",
                 "万卡", "千卡", "高性能计算", "HPC", "NPU"],
    "服务器液冷":["液冷", "冷板", "浸没式", "散热", "冷却", "热管理", "数据中心",
                  "相变", "两相", "CDU", "制冷", "温控", "冷量", "冷水机",
                  "氟化液", "导热", "热界面"],
    "太空光伏": ["太空", "空间站", "光伏", "微波传输", "无线输电", "在轨", "轨道",
                 "空间太阳能", "SSPS", "同步轨道", "低轨", "高轨", "天基",
                 "空间发电", "激光传能", "GEO"],
}

ALL_KEYWORDS = [kw for kws in FOCUS_THEMES.values() for kw in kws]


# ══════════════════════════════════════════════════════════ #
#  工具函数                                                  #
# ══════════════════════════════════════════════════════════ #

def safe_call(fn, *args, timeout=90, default=None, **kwargs):
    """带超时保护的安全调用，防止接口卡死"""
    result = [default]
    error  = [None]

    def _run():
        try:
            result[0] = fn(*args, **kwargs)
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout)

    if t.is_alive():
        return default, f"超时（>{timeout}s）"
    if error[0]:
        return default, str(error[0])[:120]
    return result[0], None


def match_themes(text: str) -> list[str]:
    """返回命中的主题列表"""
    if not isinstance(text, str):
        return []
    hits = []
    for theme, kws in FOCUS_THEMES.items():
        if any(kw in text for kw in kws):
            hits.append(theme)
    return hits


def tag_row(row: pd.Series, text_cols: list) -> str:
    """对一行数据的所有文本列做关键词匹配，返回命中主题"""
    text = " ".join(str(row.get(c, "")) for c in text_cols)
    hits = match_themes(text)
    return ", ".join(hits) if hits else ""


def section(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# ══════════════════════════════════════════════════════════ #
#  1. 机构调研监控（近7天，按接待机构数排序）                #
# ══════════════════════════════════════════════════════════ #

def get_institutional_research(days: int = 7) -> pd.DataFrame:
    section("1 / 4  机构调研监控（近7天）")

    start_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
    df, err = safe_call(ak.stock_jgdy_tj_em, date=start_date, timeout=120)

    if err:
        print(f"  [!] 接口失败: {err}")
        return pd.DataFrame()
    if df is None or df.empty:
        print("  [!] 无数据")
        return pd.DataFrame()

    # 标准化列名
    df = df.rename(columns={
        "代码":     "股票代码",
        "名称":     "股票名称",
        "接待机构数量": "调研机构数",
        "接待日期": "调研日期",
        "最新价":   "最新价",
        "涨跌幅":   "涨跌幅%",
    })

    # 过滤近7天
    if "调研日期" in df.columns:
        df["调研日期"] = pd.to_datetime(df["调研日期"], errors="coerce")
        cutoff = datetime.now() - timedelta(days=days)
        df = df[df["调研日期"] >= cutoff]

    # 转为数值
    df["调研机构数"] = pd.to_numeric(df.get("调研机构数", 0), errors="coerce").fillna(0).astype(int)

    # 关键词匹配（对所有文本列做拼接）
    text_cols = [c for c in ["股票名称", "接待方式", "接待人员", "接待地点"] if c in df.columns]
    df["主题标签"] = df.apply(lambda r: tag_row(r, text_cols), axis=1)

    df = df.sort_values("调研机构数", ascending=False).reset_index(drop=True)

    print(f"  共 {len(df)} 条调研记录  主题命中: {(df['主题标签'] != '').sum()} 条")
    keep = ["股票代码", "股票名称", "调研机构数", "调研日期", "涨跌幅%", "主题标签"]
    keep = [c for c in keep if c in df.columns]
    return df[keep].copy()


# ══════════════════════════════════════════════════════════ #
#  2. 北向资金今日净买入 TOP20                               #
# ══════════════════════════════════════════════════════════ #

def get_northbound_top(top_n: int = 0) -> pd.DataFrame:
    """
    北向个股/板块免费API已停更。
    改为：今日行业主力资金净流入排行（主力资金含北向机构，实时2026数据）。
    """
    section("2 / 4  今日行业主力资金流向（净流入排行）")

    df, err = safe_call(ak.stock_fund_flow_industry, symbol="即时", timeout=30)

    if err:
        print(f"  [!] 接口失败: {err}")
        return pd.DataFrame()
    if df is None or df.empty:
        print("  [!] 无数据")
        return pd.DataFrame()

    # 重命名
    df = df.rename(columns={
        "行业":       "行业板块",
        "行业-涨跌幅": "涨跌幅%",
        "净额":       "净流入(亿元)",
        "流入资金":   "流入(亿元)",
        "流出资金":   "流出(亿元)",
        "领涨股-涨跌幅":"领涨股涨跌幅%",
    })

    for col in ["净流入(亿元)", "流入(亿元)", "流出(亿元)"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["趋势"] = df["净流入(亿元)"].apply(
        lambda x: "流入 ▲" if pd.notna(x) and x > 0 else ("流出 ▼" if pd.notna(x) and x < 0 else "—")
    )
    df["主题标签"] = df["行业板块"].apply(lambda x: ", ".join(match_themes(str(x))))

    keep = ["行业板块", "涨跌幅%", "净流入(亿元)", "流入(亿元)", "流出(亿元)", "趋势", "领涨股", "领涨股涨跌幅%", "主题标签"]
    keep = [c for c in keep if c in df.columns]
    df = df[keep].sort_values("净流入(亿元)", ascending=False).reset_index(drop=True)

    inflow  = (df["净流入(亿元)"] > 0).sum()
    theme_hit = (df["主题标签"] != "").sum()
    print(f"  共 {len(df)} 个行业  净流入: {inflow} 个  主题命中: {theme_hit} 个")
    print(df.head(10).to_string(index=False))
    return pd.DataFrame()


# ══════════════════════════════════════════════════════════ #
#  3. 大宗交易分析（平价/溢价 = 机构锁仓信号）              #
# ══════════════════════════════════════════════════════════ #

def get_block_trades(days: int = 7) -> pd.DataFrame:
    section("3 / 4  大宗交易（昨日+今日 平价/溢价 & 机构席位）")

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    df, err = safe_call(
        ak.stock_dzjy_mrmx,
        symbol="A股", start_date=yesterday, end_date=TODAY,
        timeout=60
    )

    if err:
        print(f"  [!] 接口失败: {err}")
        return pd.DataFrame()
    if df is None or df.empty:
        print("  [!] 无数据")
        return pd.DataFrame()

    # 标准化数值列
    for col in ["折溢率", "成交额", "成交量"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # 机构席位判断
    mask_buy_inst  = df["买方营业部"].str.contains("机构专用", na=False) if "买方营业部" in df.columns else pd.Series(False, index=df.index)
    mask_sell_inst = df["卖方营业部"].str.contains("机构专用", na=False) if "卖方营业部" in df.columns else pd.Series(False, index=df.index)

    # 锁仓信号：综合折溢率 + 买卖方席位
    _SIGNAL_DESC = {
        "机构溢价锁仓 🔒": "机构席位以高于市价买入，主动加仓意愿强，后续大概率持股锁仓",
        "机构平价锁仓 🔒": "机构席位以市价买入，低调建仓，资金承接稳定",
        "机构折价吸筹 🏦": "机构以低于市价买入，对手方急于出货，机构趁机低价吸筹",
        "机构折价减持 ⚠️": "机构席位以折价卖出，可能为减持或调仓，注意风险",
        "溢价吸筹 📈":     "非机构买方溢价成交，买方主动接盘意愿强，短期看多",
        "平价过户":        "以市价平价成交，多见于大股东内部转让或正常换手",
        "折价甩卖 📉":     "以低于市价折价出售，卖方急于套现，短期注意抛压",
    }

    def _signal(row):
        rate     = row.get("折溢率", 0) or 0
        buy_inst = bool(row.get("_buy_inst", False))
        sel_inst = bool(row.get("_sel_inst", False))
        if buy_inst and rate > 0:  return "机构溢价锁仓 🔒"
        if buy_inst and rate == 0: return "机构平价锁仓 🔒"
        if buy_inst and rate < 0:  return "机构折价吸筹 🏦"
        if sel_inst and rate < 0:  return "机构折价减持 ⚠️"
        if rate > 0:               return "溢价吸筹 📈"
        if rate == 0:              return "平价过户"
        return "折价甩卖 📉"

    df["_buy_inst"] = mask_buy_inst
    df["_sel_inst"] = mask_sell_inst
    df["锁仓信号"] = df.apply(_signal, axis=1)
    df["信号解读"] = df["锁仓信号"].map(_SIGNAL_DESC)
    df.drop(columns=["_buy_inst", "_sel_inst"], inplace=True)

    # 标准化列名
    df = df.rename(columns={
        "证券代码": "股票代码",
        "证券简称": "股票名称",
        "折溢率":   "折溢率%",
        "成交额":   "成交额(元)",
    })

    # 折溢率格式化为百分比字符串，如 2.35 → "2.35%"
    if "折溢率%" in df.columns:
        df["折溢率%"] = df["折溢率%"].apply(
            lambda x: f"{x:.2f}%" if pd.notna(x) else ""
        )

    if "股票名称" in df.columns:
        df["主题标签"] = df["股票名称"].apply(lambda x: ", ".join(match_themes(str(x))))
    else:
        df["主题标签"] = ""

    keep = ["股票代码", "股票名称", "交易日期", "收盘价", "成交价", "折溢率%",
            "成交额(元)", "买方营业部", "卖方营业部", "锁仓信号", "信号解读", "主题标签"]
    keep = [c for c in keep if c in df.columns]
    df   = df[keep].sort_values("成交额(元)", ascending=False) if "成交额(元)" in df.columns else df

    print(f"  共 {len(df)} 条大宗交易  主题命中: {(df['主题标签'] != '').sum()} 条")
    return df.reset_index(drop=True)


# ══════════════════════════════════════════════════════════ #
#  0. 量价异动扫描（主板 换手3~25% 量比1.2~3 涨幅0.5~7%）  #
# ══════════════════════════════════════════════════════════ #

def get_volume_scanner() -> pd.DataFrame:
    """
    直接请求 push2.eastmoney.com（非 push2delay），单次获取全量 A 股快照。
    避免 akshare stock_zh_a_spot_em 分 58 页打 push2delay 被 CI 环境拦截的问题。
    字段：f12=代码 f14=名称 f2=最新价 f3=涨跌幅% f6=成交额(元) f8=换手率% f10=量比
    """
    section("0 / 4  量价异动扫描（主板）")

    url = "https://push2.eastmoney.com/api/qt/clist/get"
    params = {
        "pn": 1, "pz": 6000, "po": 1, "np": 1,
        "ut": "bd1d9ddb04089700cf9c27f6f7426281",
        "fltt": 2, "invt": 2, "fid": "f3",
        "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81+s:2048",
        "fields": "f2,f3,f6,f8,f10,f12,f14",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        items = resp.json().get("data", {}).get("diff", [])
        if not items:
            print("  [!] 无数据返回")
            return pd.DataFrame()
    except Exception as e:
        print(f"  [!] 数据获取失败: {e}")
        return pd.DataFrame()

    df = pd.DataFrame(items).rename(columns={
        "f12": "股票代码", "f14": "股票名称", "f2": "最新价",
        "f3": "涨跌幅%", "f6": "成交额(亿元)", "f8": "换手率%", "f10": "量比",
    })

    df["股票代码"] = df["股票代码"].astype(str).str.zfill(6)
    for col in ["最新价", "涨跌幅%", "成交额(亿元)", "换手率%", "量比"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 板块过滤：剔除创业板/科创板/北交所/ST/退
    df = df[~df["股票代码"].str.match(r"^(30|68|8|4|92)")]
    df = df[~df["股票名称"].str.contains("ST|退", case=False, na=False)]

    # 成交额：元 → 亿元
    df["成交额(亿元)"] = df["成交额(亿元)"] / 1e8

    cond = (
        (df["成交额(亿元)"] > 0.3)  &
        (df["换手率%"]    >= 3)     &
        (df["换手率%"]    <= 25)    &
        (df["量比"]       >= 1.2)   &
        (df["量比"]       <= 3.0)   &
        (df["涨跌幅%"]    >= 0.5)   &
        (df["涨跌幅%"]    <= 7.0)
    )
    df = df[cond].copy()

    keep = ["股票代码", "股票名称", "最新价", "涨跌幅%", "成交额(亿元)", "换手率%", "量比"]
    df = df[keep].sort_values("换手率%", ascending=False).reset_index(drop=True)

    for col in ["涨跌幅%", "成交额(亿元)", "换手率%", "量比"]:
        df[col] = df[col].round(2)

    df.insert(0, "交易日期", TODAY_DASH)

    print(f"  共筛出 {len(df)} 只符合条件的活跃主板个股")
    return df


# ══════════════════════════════════════════════════════════ #
#  4. 龙虎榜（昨日+今日，机构净买入）                       #
# ══════════════════════════════════════════════════════════ #

def get_lhb_institution(period: str = "近一月") -> pd.DataFrame:
    section("4 / 4  龙虎榜（昨日+今日 机构净买入）")

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    df, err = safe_call(
        ak.stock_lhb_detail_em,
        start_date=yesterday, end_date=TODAY,
        timeout=60
    )

    if err:
        print(f"  [!] 接口失败: {err}")
        return pd.DataFrame()
    if df is None or df.empty:
        print("  [!] 无数据")
        return pd.DataFrame()

    # 列名标准化
    df = df.rename(columns={
        "代码":        "股票代码",
        "名称":        "股票名称",
        "龙虎榜净买额": "龙虎榜净买额(元)",
        "上榜日":      "上榜日期",
        "涨跌幅":      "涨跌幅%",
    })

    # 只保留净买入（>0）并按净买额排序
    net_col = "龙虎榜净买额(元)"
    if net_col in df.columns:
        df[net_col] = pd.to_numeric(df[net_col], errors="coerce")
        df = df[df[net_col] > 0].sort_values(net_col, ascending=False)

    # 去重（同一股票可能多条上榜原因）
    df = df.drop_duplicates(subset=["股票代码", "上榜日期"], keep="first")

    if "股票名称" in df.columns:
        df["主题标签"] = df["股票名称"].apply(lambda x: ", ".join(match_themes(str(x))))

    keep = ["股票代码", "股票名称", "上榜日期", "涨跌幅%", "龙虎榜净买额(元)", "解读", "上榜原因", "主题标签"]
    keep = [c for c in keep if c in df.columns]

    print(f"  共 {len(df)} 条龙虎榜净买入  主题命中: {(df['主题标签'] != '').sum()} 条")
    return df[keep].head(50).reset_index(drop=True)


# ══════════════════════════════════════════════════════════ #
#  合并 & 输出                                              #
# ══════════════════════════════════════════════════════════ #

def merge_and_output(research: pd.DataFrame,
                     northbound: pd.DataFrame,
                     block: pd.DataFrame,
                     lhb: pd.DataFrame,
                     scanner: pd.DataFrame = None):

    print(f"\n{'='*60}")
    print("  综合信号合并 & 输出")
    print(f"{'='*60}")

    # ---- 建立股票评分表 ----
    score_map = {}   # {代码: {name, score, tags, sources}}

    def add_scores(df: pd.DataFrame, source: str, weight: float):
        if df is None or df.empty or "股票代码" not in df.columns:
            return
        for _, row in df.iterrows():
            code = str(row.get("股票代码", "")).zfill(6)
            name = str(row.get("股票名称", ""))
            tags = str(row.get("主题标签", ""))
            if code not in score_map:
                score_map[code] = {"股票代码": code, "股票名称": name,
                                   "综合热度": 0.0, "主题标签": set(), "信号来源": set()}
            score_map[code]["综合热度"] += weight
            if tags:
                score_map[code]["主题标签"].update(tags.split(", "))
            score_map[code]["信号来源"].add(source)

    # 权重：调研频率最高 > 北向资金 > 龙虎榜 > 大宗
    if not research.empty and "调研机构数" in research.columns:
        max_inst = research["调研机构数"].max() or 1
        for _, row in research.iterrows():
            code = str(row.get("股票代码", "")).zfill(6)
            name = str(row.get("股票名称", ""))
            tags = str(row.get("主题标签", ""))
            score = float(row.get("调研机构数", 0)) / max_inst * 4.0   # 最高4分
            if code not in score_map:
                score_map[code] = {"股票代码": code, "股票名称": name,
                                   "综合热度": 0.0, "主题标签": set(), "信号来源": set()}
            score_map[code]["综合热度"] += score
            if tags:
                score_map[code]["主题标签"].update(tags.split(", "))
            score_map[code]["信号来源"].add("机构调研")

    add_scores(northbound, "北向资金", 2.0)
    add_scores(lhb,        "龙虎榜",   1.5)
    add_scores(block,      "大宗交易", 1.0)

    if not score_map:
        print("  [!] 无有效数据，无法生成综合表")
        return

    # 转 DataFrame
    merged = pd.DataFrame([
        {
            "股票代码": v["股票代码"],
            "股票名称": v["股票名称"],
            "综合热度": round(v["综合热度"], 2),
            "主题标签": ", ".join(sorted(v["主题标签"])) if v["主题标签"] else "",
            "信号来源": " | ".join(sorted(v["信号来源"])),
            "主题命中": "★ 重点关注" if v["主题标签"] else "",
        }
        for v in score_map.values()
    ]).sort_values(["主题命中", "综合热度"], ascending=[False, False]).reset_index(drop=True)
    merged.index += 1

    # ---- 输出控制台 Markdown 表格 ----
    print(f"\n  共 {len(merged)} 只股票上榜，其中主题命中 {(merged['主题命中'] != '').sum()} 只\n")
    print("  ┌─ TOP 30 综合热度排行（主题命中优先）")

    md_cols = ["股票代码", "股票名称", "综合热度", "主题标签", "信号来源"]
    top30   = merged.head(30)[md_cols]
    col_w   = {c: max(len(c), top30[c].astype(str).str.len().max()) for c in md_cols}

    header = "  | " + " | ".join(c.ljust(col_w[c]) for c in md_cols) + " |"
    sep    = "  | " + " | ".join("-" * col_w[c] for c in md_cols) + " |"
    print(header)
    print(sep)
    for _, row in top30.iterrows():
        line = "  | " + " | ".join(str(row[c]).ljust(col_w[c]) for c in md_cols) + " |"
        print(line)

    # ---- 保存 Excel（多 Sheet）----
    now_str    = datetime.now().strftime("%Y%m%d_%H%M")
    today_str  = datetime.now().strftime("%Y%m%d")
    excel_path = os.path.join(OUTPUT_DIR, f"{today_str}_股票调研.xlsx")
    md_path    = os.path.join(OUTPUT_DIR, f"smart_money_{now_str}.md")

    try:
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            merged.to_excel(writer, sheet_name="综合热度排行", index=True)
            if not research.empty:
                research.to_excel(writer, sheet_name="机构调研", index=False)
            # 行业资金流向（同花顺即时数据）
            try:
                nb_raw = ak.stock_fund_flow_industry(symbol="即时")
                nb_raw = nb_raw.rename(columns={
                    "行业": "行业板块", "行业-涨跌幅": "涨跌幅%",
                    "净额": "净流入(亿元)", "流入资金": "流入(亿元)", "流出资金": "流出(亿元)",
                    "领涨股-涨跌幅": "领涨股涨跌幅%",
                })
                for col in ["净流入(亿元)", "流入(亿元)", "流出(亿元)"]:
                    if col in nb_raw.columns:
                        nb_raw[col] = pd.to_numeric(nb_raw[col], errors="coerce")
                nb_raw["趋势"] = nb_raw["净流入(亿元)"].apply(
                    lambda x: "流入 ▲" if pd.notna(x) and x > 0 else ("流出 ▼" if pd.notna(x) and x < 0 else "—")
                )
                nb_raw["主题标签"] = nb_raw["行业板块"].apply(lambda x: ", ".join(match_themes(str(x))))
                cols = ["行业板块", "涨跌幅%", "净流入(亿元)", "流入(亿元)", "流出(亿元)", "趋势", "领涨股", "领涨股涨跌幅%", "主题标签"]
                nb_raw[[c for c in cols if c in nb_raw.columns]].sort_values(
                    "净流入(亿元)", ascending=False
                ).to_excel(writer, sheet_name="行业资金流向", index=False)
            except Exception:
                pass
            if not block.empty:
                block.to_excel(writer, sheet_name="大宗交易", index=False)
            if scanner is not None and not scanner.empty:
                scanner.to_excel(writer, sheet_name="量价异动", index=False)
            if not lhb.empty:
                lhb_out = lhb.copy()
                # 批量查询所属行业（emweb.securities.eastmoney.com，非 push2，并行 ~2s）
                try:
                    import json as _json
                    from concurrent.futures import ThreadPoolExecutor as _TPE

                    _hdrs = {"User-Agent": "Mozilla/5.0",
                             "Referer": "https://emweb.securities.eastmoney.com/"}

                    def _get_sshy(code):
                        code = str(code).zfill(6)
                        pfx = "SH" if code.startswith(("6", "5")) else "SZ"
                        try:
                            url = (f"https://emweb.securities.eastmoney.com/"
                                   f"PC_HSF10/CompanySurvey/CompanySurveyAjax?code={pfx}{code}")
                            r = requests.get(url, headers=_hdrs, timeout=8)
                            data = _json.loads(r.content.decode("utf-8", errors="replace"))
                            return code, data.get("jbzl", {}).get("sshy", "—") or "—"
                        except Exception:
                            return code, "—"

                    _codes = lhb_out["股票代码"].astype(str).str.zfill(6).tolist()
                    with _TPE(max_workers=8) as _ex:
                        _ind_lookup = dict(_ex.map(_get_sshy, _codes))

                    lhb_out.insert(2, "所属板块",
                                   lhb_out["股票代码"].apply(
                                       lambda c: _ind_lookup.get(str(c).zfill(6), "—")))
                except Exception:
                    if "所属板块" not in lhb_out.columns:
                        lhb_out.insert(2, "所属板块", "—")
                lhb_out.to_excel(writer, sheet_name="龙虎榜机构", index=False)
        print(f"\n  [Excel] 已保存 → {excel_path}")
    except ImportError:
        print("  [!] 需要安装 openpyxl: pip install openpyxl")
    except Exception as e:
        print(f"  [!] Excel 保存失败: {e}")

    # ---- 保存 Markdown ----
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(f"# A股机构动向 & 聪明钱信号报告\n\n")
        f.write(f"**生成时间：** {datetime.now().strftime('%Y-%m-%d %H:%M')}  \n")
        f.write(f"**关注主题：** {', '.join(FOCUS_THEMES.keys())}  \n\n")
        f.write("## 综合热度 TOP 30\n\n")
        f.write("| 排名 | " + " | ".join(md_cols) + " |\n")
        f.write("|------|" + "|".join(["---"] * len(md_cols)) + "|\n")
        for i, row in top30.iterrows():
            f.write(f"| {i} | " + " | ".join(str(row[c]) for c in md_cols) + " |\n")
        f.write("\n## 主题命中详情\n\n")
        theme_hits = merged[merged["主题命中"] != ""][["股票代码", "股票名称", "综合热度", "主题标签", "信号来源"]]
        if not theme_hits.empty:
            f.write(theme_hits.to_markdown(index=True))
        else:
            f.write("_本次扫描未命中关注主题_\n")
    print(f"  [MD]    已保存 → {md_path}")

    # ---- 主题命中专项提示 ----
    theme_hits = merged[merged["主题命中"] != ""]
    if not theme_hits.empty:
        print(f"\n  ★★ 命中关注主题的股票 ({len(theme_hits)} 只) ★★")
        for _, row in theme_hits.iterrows():
            print(f"     {row['股票代码']} {row['股票名称']:8s}  主题:{row['主题标签']:20s}  来源:{row['信号来源']}")
    else:
        print("\n  本次扫描未发现命中关注主题的股票")

    return merged, excel_path


# ══════════════════════════════════════════════════════════ #
#  主程序                                                   #
# ══════════════════════════════════════════════════════════ #

def main():
    print(f"\n{'#'*60}")
    print(f"  机构动向 & 聪明钱信号捕捉系统")
    print(f"  扫描时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  关注主题: {', '.join(FOCUS_THEMES.keys())}")
    print(f"{'#'*60}")

    scanner    = get_volume_scanner()
    research   = get_institutional_research(days=5)
    northbound = get_northbound_top(top_n=0)   # 0 = 全量
    block      = get_block_trades(days=3)
    lhb        = get_lhb_institution(period="近一月")

    merged, excel_path = merge_and_output(research, northbound, block, lhb, scanner)

    print(f"\n{'#'*60}")
    print(f"  扫描完成  输出目录: {OUTPUT_DIR}")
    print(f"{'#'*60}\n")

    return merged, excel_path


if __name__ == "__main__":
    main()
