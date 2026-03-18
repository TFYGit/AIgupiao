# A股聪明钱信号捕捉系统

每日自动追踪机构调研、行业资金流向、大宗交易、龙虎榜，生成 Excel 报告并通过 Gmail 邮件发送。

## 部署步骤

### 1. 推送代码到 GitHub

在 GitHub 新建一个仓库，然后：

```bash
cd D:\lianghua
git remote add origin https://github.com/你的用户名/仓库名.git
git push -u origin master
```

### 2. 获取 Gmail 应用专用密码

> Gmail 不能用登录密码发邮件，需要单独生成一个「应用专用密码」

1. 打开 [myaccount.google.com/security](https://myaccount.google.com/security)
2. 开启「两步验证」（未开启则无法生成应用密码）
3. 搜索「应用密码」→ 选择「邮件」→ 生成 → 复制 16 位密码

### 3. 添加 3 个 GitHub Secrets

进入仓库 → **Settings → Secrets and variables → Actions → New repository secret**

| Secret 名称 | 填写内容 |
|---|---|
| `EMAIL_SENDER` | 你的 Gmail 地址，如 `abc@gmail.com` |
| `EMAIL_PASSWORD` | 上一步生成的 16 位应用专用密码 |
| `EMAIL_RECIPIENT` | 接收报告的邮箱（任意邮箱均可） |

### 4. 触发运行

- **自动**：每个工作日 **19:00（北京时间）** 自动运行
- **手动**：仓库 → Actions → 「每日股票调研报告」→ Run workflow

---

## 本地运行

```bash
pip install -r requirements.txt

# 设置环境变量后运行
set EMAIL_SENDER=abc@gmail.com
set EMAIL_PASSWORD=xxxx xxxx xxxx xxxx
set EMAIL_RECIPIENT=target@example.com
python run.py

# 不发邮件，只生成 Excel
python run.py --no-email
```
