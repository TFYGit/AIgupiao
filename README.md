# A股聪明钱信号捕捉系统

每日自动追踪机构调研、行业资金流向、大宗交易、龙虎榜，筛选与关注主题高度相关的股票，生成 Excel 报告并通过邮件发送。

## 功能模块

| 模块 | 数据源 | 说明 |
|---|---|---|
| 机构调研 | 东方财富 | 近 5 天被机构调研的股票，按机构数量排序 |
| 行业资金流向 | 同花顺 | 今日各行业主力资金净流入排行 |
| 大宗交易 | 东方财富 | 昨日+今日 平价/溢价成交 & 机构席位 |
| 龙虎榜 | 东方财富 | 昨日+今日 机构净买入，含所属行业 |

**关注主题**：氢能源、核电、航空航天、算电协同、服务器液冷、太空光伏

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置邮件

```bash
cp config.ini.example config.ini
# 编辑 config.ini，填入 SMTP 服务器、账号、授权码、收件人
```

**QQ邮箱授权码获取**：QQ邮箱 → 设置 → 账户 → POP3/IMAP/SMTP → 生成授权码

### 3. 立即运行

```bash
# 采集数据 + 发送邮件
python run.py

# 只采集数据，不发送邮件
python run.py --no-email
```

Excel 报告保存在 `output/` 目录，命名格式：`20260318_股票调研.xlsx`

---

## 定时任务配置

### 方案 A：Windows 任务计划程序（推荐本地运行）

1. 打开「任务计划程序」→「创建基本任务」
2. 触发器：每天 **19:00**
3. 操作：启动程序
   - 程序：`C:\Python313\python.exe`（根据实际路径修改）
   - 参数：`D:\lianghua\run.py`
   - 起始于：`D:\lianghua`
4. 完成

### 方案 B：GitHub Actions（云端自动运行）

1. 将代码推送到 GitHub 仓库
2. 在仓库 **Settings → Secrets → Actions** 中添加以下 Secret：

| Secret 名称 | 说明 |
|---|---|
| `SMTP_SERVER` | 如 `smtp.qq.com` |
| `SMTP_PORT` | 如 `465` |
| `SMTP_SSL` | `true` 或 `false` |
| `EMAIL_SENDER` | 发件邮箱 |
| `EMAIL_PASSWORD` | 授权码 |
| `EMAIL_RECIPIENT` | 收件邮箱 |

3. `.github/workflows/daily_report.yml` 已配置周一至周五 **19:00 北京时间**自动运行

---

## 项目结构

```
├── smart_money.py          # 核心数据采集与分析
├── send_report.py          # 邮件发送模块
├── run.py                  # 入口脚本（采集 + 发邮件）
├── config.ini.example      # 邮件配置模板
├── config.ini              # 实际配置（已在 .gitignore，不上传）
├── requirements.txt        # Python 依赖
└── .github/workflows/      # GitHub Actions 定时任务
```

## 注意事项

- `config.ini` 包含邮箱密码，已加入 `.gitignore`，**不会上传到 GitHub**
- 部分数据接口（东方财富 push2 域名）在有代理的环境下需要绕过，`smart_money.py` 已内置处理
- 交易日之外运行可能返回空数据，属正常现象
