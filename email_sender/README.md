# email_sender

批量邮件自动发送脚本（支持多发件人轮换、收件人列表、文案模板 + AI 生成正文、按批次发送与间隔等待）。

## 功能

- 多发件人账号轮换发送（可直接在配置里写账号密码）
- 支持收件人列表文件（CSV/TSV/TXT）
- 支持多份文案模板（`template_id` 指定或轮询）
- 支持调用 OpenAI 兼容接口生成正文
- 每批发送数量可配置（默认 10）
- 批次间隔可配置（默认 10 秒）
- 防风控：每封随机延迟、批次随机抖动、SMTP 瞬时错误重试、连续失败熔断暂停
- 生成发送结果报告（CSV）

## 目录

- `main.py`：主脚本
- `config.example.yaml`：配置模板
- `recipients.example.csv`：收件人示例
- `requirements.txt`：依赖

## 安装

```bash
cd autoTools/email_sender
python -m pip install -r requirements.txt
```

## 配置

1. 复制配置模板：

```bash
cp autoTools/email_sender/config.example.yaml autoTools/email_sender/config.yaml
```

2. 修改 `config.yaml`：

- `senders`：填写企业邮箱账号、密码、SMTP 地址与端口
- `recipients.file`：指向你的收件人列表文件
- `templates.items`：填写你的定制模板
- `templates.items[].attachment_paths`：可选附件路径列表（如联系方式图片）
- `templates.items[].inline_image_paths`：可选图片附件路径列表（兼容字段名）
- `runtime.batch_size`：每批发送数量（默认 10）
- `runtime.batch_interval_seconds`：批次间隔秒数（默认 10）
- `runtime.per_email_delay_seconds`：单封邮件间隔（建议 >= 1）
- `runtime.smtp_retry_count`：瞬时错误自动重试次数（建议 1-3）
- `runtime.failure_pause_threshold` + `runtime.failure_pause_seconds`：连续失败冷却策略
- `ai`：填写 AI 接口信息（base_url、api_key、model）

如果你已在本机 Codex 配置好模型与密钥，可以直接启用：

- `ai.use_local_codex: true`
- `ai.local_codex_home: "~/.codex"`
- `ai.provider: "codex_cli"`（推荐，直接调用本机 `codex exec`）
- `ai.reasoning_effort: "low"`（建议，避免全局 `xhigh` 导致生成过慢）

启用后有两种模式：

- `ai.provider: "codex_cli"`：直接调用本机 `codex exec` 生成正文，最适合已经能正常使用 Codex CLI 的环境。
- `ai.provider: "openai_compatible"`：脚本读取 `~/.codex/config.toml` 与 `~/.codex/auth.json`，补全 `base_url/model/api_style/api_key` 后直接请求兼容接口。

建议：

- `codex_cli` 模式下将 `ai.timeout_seconds` 设为 `>= 60`（推荐 `120`）。
- 若你本机 `~/.codex/config.toml` 使用 `model_reasoning_effort = "xhigh"`，可在本项目配置 `ai.reasoning_effort: "low"`，单独降低邮件生成耗时。

3. 准备收件人 CSV，例如：

```csv
email,name,company,title,industry,template_id
alice@example.com,Alice,Acme Inc,Operations Manager,SaaS,intro
bob@example.com,Bob,Beta Tech,CTO,Manufacturing,follow_up
```

说明：

- `email` 必填
- `template_id` 可选；为空时使用 `templates.default_template_id`

## 运行

1. 先用 dry-run 验证配置：

```bash
python autoTools/email_sender/main.py --config autoTools/email_sender/config.yaml --dry-run
```

2. 正式发送：

```bash
python autoTools/email_sender/main.py --config autoTools/email_sender/config.yaml
```

3. 从第 N 条开始，只发送 M 条：

```bash
python autoTools/email_sender/main.py \
  --config autoTools/email_sender/config.yaml \
  --start-index 0 \
  --limit 50
```

4. 测试时跳过批次等待：

```bash
python autoTools/email_sender/main.py --config autoTools/email_sender/config.yaml --no-wait
```

## 防风控建议参数

推荐起步（先小批量验证）：

- `batch_size: 10`
- `batch_interval_seconds: 15`
- `batch_interval_jitter_seconds: 3`
- `per_email_delay_seconds: 2`
- `per_email_delay_jitter_seconds: 2`
- `smtp_retry_count: 2`
- `failure_pause_threshold: 3`
- `failure_pause_seconds: 120`

说明：

- 脚本会在每封之间随机停顿，避免固定节奏发送。
- 批次休眠也带随机抖动，减少可识别模式。
- 4xx/超时等瞬时 SMTP 错误会自动重试并退避。
- 若连续失败达到阈值会触发冷却暂停。

## 模板策略

- `templates.selection: by_recipient_field`
  - 从收件人行的 `template_id` 字段读取模板。
- `templates.selection: round_robin`
  - 按模板列表顺序轮换。

## 占位符

模板里可用收件人列名和内置字段：

- 收件人字段：`{name}`、`{company}`、`{title}` 等（来自 CSV 头）
- 内置字段：`{sender_name}`、`{sender_email}`、`{today}`、`{send_index}`、`{total_count}`

模板还支持可选附件字段：

- `attachment_paths`：列表，支持相对/绝对路径；发送时会作为邮件附件一并发出。
- `inline_image_paths`：列表，支持相对/绝对路径；按普通附件发送（不插入正文）。

## 输出

每次运行都会写入一个 CSV 报告，默认目录：`runtime.report_dir`（例如 `email_sender/reports/`）。

报告字段：

- `timestamp`：`YYYY-MM-DD HH:mm:ss`
- `status`：`SENT` / `FAILED` / `DRY_RUN`
- `recipient_email`
- `sender_email`
- `template_id`
- `subject`
- `body`：邮件正文（包含 AI 或 fallback 生成内容）
- `ai_used`
- `error`

## 注意事项

- 请确保收件人来源合法且允许接收此类邮件。
- 发送频率过高可能触发企业邮箱风控，建议先小批量验证。
- 明文密码可用，但更推荐用环境变量替代（脚本支持 `${ENV_VAR}` 写法）。
