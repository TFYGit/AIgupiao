import os
import requests
import pandas as pd
from datetime import datetime

# 绕过系统代理（代理会导致 eastmoney 请求失败）
requests.utils.get_environ_proxies = lambda *a, **kw: {}
_orig_menv = requests.Session.merge_environment_settings
def _no_proxy(self, url, proxies, stream, verify, cert):
    result = _orig_menv(self, url, proxies, stream, verify, cert)
    result["proxies"] = {}
    return result
requests.Session.merge_environment_settings = _no_proxy

# ── 全量显示配置 ──────────────────────────────────────────────
pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.float_format", lambda x: f"{x:.2f}")

TODAY = datetime.today().strftime("%Y%m%d")
OUTPUT_DIR = r"D:\clode"


# ── 1. 直接请求东方财富（单次获取全量，绕过代理问题）──────────
def fetch_spot_data() -> pd.DataFrame:
    """
    使用东方财富选股接口（data.eastmoney.com），服务端直接过滤条件，
    返回今日符合量价要求的全部个股，无需分页拉取全量再本地筛选。
    """
    print("[INFO] 获取行情数据（data.eastmoney.com 选股接口）...")
    all_rows = []
    for page in range(1, 20):
        url = "https://data.eastmoney.com/dataapi/xuangu/list"
        params = {
            "sty":    "SECURITY_CODE,SECURITY_NAME_ABBR,CHANGE_RATE,DEAL_AMOUNT,TURNOVERRATE,VOLUME_RATIO",
            "filter": "(CHANGE_RATE>0.5)(CHANGE_RATE<7)(TURNOVERRATE>3)(TURNOVERRATE<25)(VOLUME_RATIO>1.2)(VOLUME_RATIO<3)(DEAL_AMOUNT>30000000)",
            "p":      page,
            "ps":     500,
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer":    "https://data.eastmoney.com/xuangu/",
        }
        resp = requests.get(url, params=params, headers=headers, timeout=30, proxies={})
        result = resp.json().get("result", {})
        rows = result.get("data") or []
        all_rows.extend(rows)
        if not result.get("nextpage"):
            break

    df = pd.DataFrame(all_rows).rename(columns={
        "SECURITY_CODE":      "code",
        "SECURITY_NAME_ABBR": "name",
        "CHANGE_RATE":        "pct_chg",
        "DEAL_AMOUNT":        "amount",
        "TURNOVERRATE":       "turnover",
        "VOLUME_RATIO":       "vol_ratio",
    })
    # 成交额：元 → 亿元（在 filter_liquidity 里处理，这里保持原单位）
    print(f"[INFO] 数据获取成功，共 {len(df)} 条（已含服务端筛选）")
    return df


# ── 3. 严格板块过滤 ──────────────────────────────────────────
def filter_board(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df["code"] = df["code"].astype(str).str.strip()

    # 剔除创业板(30)、科创板(68)、北交所(8x / 4x)
    mask_board = ~df["code"].str.match(r"^(30|68|8|4)")
    df = df[mask_board]

    # 剔除 ST / 退 风险股
    mask_risk = ~df["name"].str.contains("ST|退", case=False, na=False)
    df = df[mask_risk]

    print(f"[INFO] 板块过滤：{before} → {len(df)} 行（剔除创业板/科创板/北交所/ST/退）")
    return df


# ── 4. 流动性与量价初筛 ──────────────────────────────────────
def filter_liquidity(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)

    # 强制转数值，无法转换的置 NaN
    for col in ["amount", "turnover", "vol_ratio"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["amount", "turnover", "vol_ratio"])

    # EM 接口成交额单位为元，统一转为亿元
    df["amount"] = df["amount"] / 1e8
    # 换手率和涨跌幅 EM 已是百分比形式（11.88 = 11.88%），无需换算
    # 正式筛选
    cond = (
        (df["amount"] > 0.3) &               # 成交额 > 3000 万元 = 0.3 亿元
        (df["turnover"] >= 3) &              # 换手率 >= 3%
        (df["turnover"] <= 25) &             # 换手率 <= 25%
        (df["vol_ratio"] >= 1.2) &           # 量比 >= 1.2（温和放量）
        (df["vol_ratio"] <= 3.0) &           # 量比 <= 3.0（排除情绪过热）
        (df["pct_chg"] >= 0.5) &             # 涨幅 >= 0.5%（剔除放量下跌）
        (df["pct_chg"] <= 7.0)               # 涨幅 <= 7%（剔除涨停或接近涨停）
    )
    df = df[cond].copy()

    print(f"[INFO] 量价过滤：{before} → {len(df)} 行（成交额>3000万 / 换手3~25% / 量比1.2~3.0 / 涨幅0.5~7%）")
    return df


# ── 5. 整理输出列 ────────────────────────────────────────────
def finalize(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values("turnover", ascending=False).reset_index(drop=True)

    # 重命名为中文展示列
    df = df.rename(columns={
        "code":      "代码",
        "name":      "名称",
        "price":     "最新价",
        "pct_chg":   "涨跌幅(%)",
        "amount":    "成交额(亿元)",
        "turnover":  "换手率(%)",
        "vol_ratio": "量比",
    })

    # 只保留目标列
    keep = [c for c in ["代码", "名称", "最新价", "涨跌幅(%)", "成交额(亿元)", "换手率(%)", "量比"] if c in df.columns]
    df = df[keep]

    # 保留两位小数
    for col in ["成交额(亿元)", "换手率(%)", "量比", "最新价", "涨跌幅(%)"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(2)

    return df


# ── 主流程 ───────────────────────────────────────────────────
def main():
    print("=" * 60)
    print(f"  A股主板全量异动扫描  |  {TODAY}")
    print("=" * 60)

    df = fetch_spot_data()
    df = filter_board(df)
    df = filter_liquidity(df)
    df = finalize(df)

    # ── 全量打印 ─────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(df.to_string(index=True))
    print("=" * 60)

    # ── 保存 CSV ─────────────────────────────────────────────
    csv_file = os.path.join(OUTPUT_DIR, f"A股主板全量异动扫描_{TODAY}.csv")
    df.to_csv(csv_file, index=False, encoding="utf-8-sig")
    print(f"[INFO] CSV 已保存至：{csv_file}")

    # ── 保存 Excel ────────────────────────────────────────────
    xlsx_file = os.path.join(OUTPUT_DIR, f"A股主板全量异动扫描_{TODAY}.xlsx")
    with pd.ExcelWriter(xlsx_file, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="异动扫描")
        ws = writer.sheets["异动扫描"]
        # 自适应列宽
        for col in ws.columns:
            max_len = max(len(str(cell.value)) if cell.value else 0 for cell in col)
            ws.column_dimensions[col[0].column_letter].width = max_len + 4
    print(f"[INFO] Excel 已保存至：{xlsx_file}")

    # ── 运行反馈 ─────────────────────────────────────────────
    print(f"\n[完成] 本次扫描共发现 {len(df)} 只符合条件的活跃主板个股")


if __name__ == "__main__":
    main()
