# A股聪明钱信号捕捉系统

每日自动追踪机构调研、行业资金流向、大宗交易、龙虎榜，筛选关注主题相关股票，生成 Excel 报告并通过邮件发送。

## 功能模块

| 模块 | 数据源 | 说明 |
|---|---|---|
| 机构调研 | 东方财富 | 近 5 天被机构调研的股票，按调研机构数排序 |
| 行业资金流向 | 同花顺 | 今日各行业主力资金净流入排行 |
| 大宗交易 | 东方财富 | 昨日+今日 平价/溢价成交 & 机构席位 |
| 龙虎榜 | 东方财富 | 昨日+今日 机构净买入，含所属行业板块 |

**关注主题**：氢能源、核电、航空航天、算电协同、服务器液冷、太空光伏

---

## 部署到 GitHub Actions（推荐）

### 第一步：推送代码到 GitHub

```bash
git remote add origin https://github.com/你的用户名/仓库名.git
git push -u origin master
```

### 第二步：添加 GitHub Secrets

进入仓库页面 → **Settings → Secrets and variables → Actions → New repository secret**

逐一添加以下 6 个 Secret：

| Secret 名称 | 填写内容 | 示例 |
|---|---|---|
| `SMTP_SERVER` | SMTP 服务器地址 | `smtp.qq.com` |
| `SMTP_PORT` | SMTP 端口号 | `465` |
| `SMTP_SSL` | 是否使用 SSL | `true` |
| `EMAIL_SENDER` | 发件邮箱 | `12345678@qq.com` |
| `EMAIL_PASSWORD` | 邮箱授权码（非登录密码） | `abcdefghijklmnop` |
| `EMAIL_RECIPIENT` | 收件邮箱 | `yourmail@example.com` |

**常用邮箱配置：**

| 邮箱 | SMTP_SERVER | SMTP_PORT | SMTP_SSL |
|---|---|---|---|
| QQ邮箱 | `smtp.qq.com` | `465` | `true` |
| 163邮箱 | `smtp.163.com` | `465` | `true` |
| Gmail | `smtp.gmail.com` | `587` | `false` |

> **QQ邮箱授权码获取**：QQ邮箱网页版 → 设置 → 账户 → POP3/IMAP/SMTP → 开启服务 → 生成授权码

### 第三步：确认定时任务

`.github/workflows/daily_report.yml` 已配置：
- **周一至周五 19:00（北京时间）** 自动运行
- 也可在仓库 **Actions** 页面手动点击 `Run workflow` 触发

---

## 本地运行（可选）

```bash
# 安装依赖
pip install -r requirements.txt

# 复制并填写邮件配置
cp config.ini.example config.ini
# 编辑 config.ini，填入邮箱信息

# 运行
python run.py              # 采集 + 发邮件
python run.py --no-email   # 只采集，不发邮件
```

Excel 报告保存在 `output/` 目录，命名格式：`20260318_股票调研.xlsx`

---

## 项目结构

```
├── smart_money.py              # 核心：数据采集与分析
├── send_report.py              # 邮件发送（读取环境变量或 config.ini）
├── run.py                      # 入口脚本
├── config.ini.example          # 本地邮件配置模板
├── config.ini                  # 本地实际配置（已在 .gitignore，不会上传）
├── requirements.txt
└── .github/workflows/
    └── daily_report.yml        # GitHub Actions 定时任务
```
