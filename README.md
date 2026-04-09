# Claw Data Filter

LLM-powered agent conversation data filtering tool. Import JSONL files, run round feedback judgments, filter and export high-quality data.

当前主链路:

1. 导入 OpenAI 或 UniRouter 对话数据。
2. 将样本写入 DuckDB，并生成稳定的 sample_uid 与本地整数 id。
3. 对导入消息中只有 user、没有 assistant 的样本打标 empty_response=true。
4. session merge 按真实 user turns 检测并折叠会话快照重复，只保留应继续流转的样本。
5. round feedback 以 claim 模式批量领取 pending 或 failed 且 session_merge_keep 为 true 的样本。
6. 按双层语义分别生成 assistant response judgments 和 user episode judgments，并以 sample_uid 为键原子写回结果。
7. 样本进入 completed 或 failed 状态。
8. 通过 CLI 或 Web 筛选页按统一导出服务导出 raw_json JSONL，或导出带双层 round feedback 侧挂信息的 OpenAI 兼容 JSONL。

## Quick Start

```bash
# 1. 导入数据
claw-filter import data.jsonl

# 2. 运行 round feedback 评分（需要 LLM 服务器）
claw-filter pressure-test  # 先测试稳定性
claw-filter session-merge --workers 4 --batch-size 512 --min-prefix-turns 2
claw-filter round-feedback --workers 32 --batch-size 50

# 3. 查看统计
claw-filter stats

# 4. 筛选导出
claw-filter filter --response-helpful-rate ">=0.7" --export filtered.jsonl
```

## Bash 脚本

项目根目录提供了两份可直接修改配置后执行的脚本：

- scripts/run_import_to_stats.sh
  覆盖 import -> pressure-test -> round-feedback -> stats 全流程。
- scripts/run_export.sh
  覆盖 filter/export/report 流程。

使用方式：

```bash
# 先修改脚本顶部 Configuration 区域
bash scripts/run_import_to_stats.sh
bash scripts/run_export.sh
```

导入脚本可配置项包括：
- INPUT_FILE
- DB_PATH
- LLM_ENDPOINT
- LLM_API_KEY
- LLM_MODEL_ID
- MAX_CONCURRENCY
- BATCH_SIZE
- LLM_TIMEOUT
- RUN_PRESSURE_TEST
- RUN_SESSION_MERGE
- SESSION_MERGE_WORKERS
- SESSION_MERGE_BATCH_SIZE
- SESSION_MERGE_MIN_PREFIX_TURNS

导出脚本可配置项包括：
- DB_PATH
- EXPORT_PATH
- REPORT_PATH
- RESPONSE_HELPFUL_RATE
- USER_SATISFIED_RATE
- USER_NEGATIVE_FEEDBACK_RATE
- EXPORT_FORMAT
- SESSION_MERGE_KEEP
- SESSION_MERGE_STATUS
- EMPTY_RESPONSE
- HAS_ERROR
- LIMIT
- GENERATE_REPORT

脚本行为说明：
- 当 `EXPORT_FORMAT=raw_jsonl` 时，默认输出文件名是 `data/exported.jsonl`。
- 当 `EXPORT_FORMAT=openai_round_feedback` 且 `EXPORT_PATH` 仍保持默认值时，脚本会自动改写为 `data/exported_round_feedback.jsonl`，避免把两种格式写到同一个默认路径。

## 数据格式

支持 OpenAI 格式和 UniRouter 格式（自动转换）：

```json
{"messages": [
  {"role": "user", "content": "用户问题"},
  {"role": "assistant", "content": "回复", "tool_calls": [...]},
  {"role": "tool", "content": "工具结果", "tool_call_id": "..."}
]}
```

UniRouter 格式自动从 request.bodyJson.messages 提取:

```json
{
  "request": {
    "bodyJson": {
      "messages": [
        {"role": "user", "content": "用户问题"},
        {"role": "assistant", "content": "回复"}
      ]
    }
  }
}
```

说明:
- 导入阶段会统一提取消息并生成 user_query、assistant_response、num_turns、expected_judgment_count、empty_response 等派生字段。
- 导入阶段还会基于原始 payload 生成 SHA-256 的 sample_uid，用作稳定、低碰撞的导入身份；整数 id 继续作为本地关系键。
- 当导入数据中存在 user 消息但没有 assistant 消息时，会标记 empty_response=true，便于后续筛除这类样本。
- 当前代码中的 expected_judgment_count 等于 expected_response_judgment_count + expected_episode_judgment_count。
- response judgments 以 assistant 响应单元计数；episode judgments 以完整 user episode 计数。
- importer 当前直接读取的是普通 `.jsonl` 文件，不会自动解压 `.gz`。
- 如果原始包内是 `items.jsonl.gz`，需要先解压为普通 `items.jsonl` 后再导入。
- `scripts/run_import_to_stats.sh` 也要求 `INPUT_FILE` 指向普通 `.jsonl`，传入 `.gz` 会直接报错退出。

## 样本状态

samples 表当前使用显式处理状态:

| 状态 | 含义 |
|------|------|
| pending | 已导入，尚未进入 round feedback |
| processing | 已被当前批处理领取 |
| completed | round feedback 已完成且结果已原子写回 |
| failed | 处理失败，保留错误信息，后续可重新领取 |

说明:
- round feedback 使用 claim 模式领取 pending 和 failed 样本。
- 如果 session merge 已执行，claim 时只会领取 session_merge_keep=true 的样本；unmarked/null 和 session_merge_keep=false 都不会进入 round feedback。
- 结果写入采用按 sample_uid 的原子替换，避免 sample 聚合结果与双 judgment 明细不一致。

## Session Merge

session merge 用于修复导入后 DuckDB 中的会话快照重复问题，执行时只依赖真实 user content，而不依赖可能失真的 metadata 标识。

当前策略：
- 只抽取真实 user turns，跳过 tool_result-only user block。
- 先按第一轮真实 user 文本分桶。
- 桶内先折叠完全相同的 user-turn 序列。
- 对满足最小公共前缀阈值的严格前缀样本，只保留更长的叶子样本。

session merge 会在 samples 表写入以下字段：
- session_merge_status
- session_merge_keep
- session_merge_group_id
- session_merge_group_size
- session_merge_representative_uid
- session_merge_reason
- session_merge_updated_at

CLI 示例：

```bash
# 仅预览 merge 结果，不写回数据库
claw-filter session-merge --dry-run --workers 4 --batch-size 512 --min-prefix-turns 2

# 正式写回 merge 标记
claw-filter session-merge --workers 4 --batch-size 512 --min-prefix-turns 2
```

## 评分维度

当前 round feedback 维护两个指标：

| 维度 | 值 | 说明 |
|------|-----|------|
| **response_helpful** | yes/no/uncertain | assistant 当前响应单元对用户是否有帮助 |
| **user_satisfied** | yes/no/uncertain/neutral | 用户对完整 assistant 交互 episode 是否满意 |

目标设计采用两层级判定，而不是共用同一分轮边界：

- response_helpful: 以 assistant 响应单元为对象。当前 assistant 的 text、tool 选择、参数构造、调用命令都属于被评判内容；它只能使用紧邻的下一跳反馈块作为证据，下一跳要么是 tool result block，要么是 user 消息。
- user_satisfied: 以上一轮 user 开始、到下一轮 user 之前结束的完整 assistant/tool 交互 episode 为对象；其证据窗口是该 episode 之后最多 3 条 user 文本消息，不包含后续 assistant。

为什么要拆成两层：

- response_helpful 关注的是 assistant 当下这一步是否做对了，证据应尽量局部、紧邻，避免把后续 assistant 的补救结果反向归功到前一跳。
- user_satisfied 关注的是用户是否接受了整段交互结果，天然应该覆盖一个 user episode 内的多步 assistant/tool 往返。
- 两个指标的评判对象和反馈信号窗口不同，继续共用一套 judged turn 会把粒度混在一起，导致归因失真。
- 当前实现已经去掉这层共用 judged turn，分别落到 response-step 与 user-episode 两种明细记录上。

**user_satisfied 判定：**
- 用户追问/澄清 → no
- 用户确认/继续 → yes
- 用户转新话题 → neutral
- 无明确信号 → uncertain

边界说明:
- response_helpful 的边界是 assistant -> 紧邻反馈块。若 assistant 后面紧跟 tool 消息，则该 tool block 是反馈；若 assistant 后面直接进入 user，则该 user 是反馈。
- user_satisfied 的边界是 user episode：从某条 user 消息开始，到下一条 user 消息出现前的所有 assistant/tool 交互都属于同一 episode。
- 导出、Web detail、样本级 rate 聚合与测试都已经按这套双层边界运行。

## 筛选字段

| 字段 | 来源 | 说明 |
|------|------|------|
| response_helpful_rate | samples | helpful=yes 比例，分母为 yes+no |
| response_unhelpful_rate | samples | helpful=no 比例，分母为 yes+no |
| user_satisfied_rate | samples | satisfied=yes 比例，分母为 yes+no+neutral |
| user_negative_feedback_rate | samples | satisfied=no 比例，分母为 yes+no+neutral |
| empty_response | samples | 导入消息中是否只有 user、没有 assistant |
| num_turns | samples | 轮次数 |
| has_error | samples.tool_stats | round feedback 是否含错误 |

rate 计算说明:
- response_helpful_rate 的分母不计入 uncertain。
- user_satisfied_rate 和 user_negative_feedback_rate 的分母不计入 uncertain，仅统计 yes、no、neutral。

## 存储结构

主要表:

- samples
  记录 sample_uid、原始 JSON、empty_response 在内的派生字段、四个显式 rate 列、session_merge 标记列、tool_stats、processing_status 等样本级信息。
- assistant_response_judgments
  记录每个 assistant 响应单元的 response_helpful、feedback_kind、反馈块范围和 llm_error。
- user_episode_judgments
  记录每个 user episode 的 user_satisfied、signal_from_users、消息范围和 llm_error。

说明:
- 双 judgment 明细表都以 sample_uid 作为跨表关联键。
- samples.id 仍然存在，但不再承担 Web drill-down、round feedback 写回或 session merge 代表关系的业务主键职责。

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `LLM_ENDPOINT` | http://localhost:8000/v1 | LLM API 地址 |
| `LLM_API_KEY` | - | API 密钥 |
| `DB_PATH` | ./data.duckdb | 数据库路径 |
| `BATCH_SIZE` | 10 | round feedback 每批领取样本数 |
| `MAX_CONCURRENCY` | 10 | 最大并发数 |
| `LLM_TIMEOUT` | 60.0 | 超时（秒） |

## CLI 命令

```bash
claw-filter import <file>              # 导入 JSONL
claw-filter pressure-test              # LLM 稳定性测试
claw-filter round-feedback            # 运行 round feedback 评分
claw-filter session-merge            # 运行 session merge 打标
claw-filter stats                     # 查看统计
claw-filter filter [options] --export <file>  # 筛选导出
claw-filter info                     # 数据库信息
```

### Round Feedback 选项

```bash
# 使用 32 并发 worker，单批领取 50 条样本
claw-filter round-feedback --workers 32 --batch-size 50
```

行为说明:
- workers 控制 LLM 调用并发上限。
- batch-size 控制每轮从 pending 或 failed 状态领取的样本数量。
- 处理过程中失败的样本会标记为 failed，而不是静默丢失。

### Filter 选项

```bash
# 按 response_helpful_rate 筛选
claw-filter filter --response-helpful-rate ">=0.7" --export out.jsonl

# 按 user_satisfied_rate 筛选
claw-filter filter --user-satisfied-rate ">=0.7" --export out.jsonl

# 按 user_negative_feedback_rate 筛选
claw-filter filter --user-negative-feedback-rate ">=0.3" --export out.jsonl

# 仅导出 session merge 保留样本
claw-filter filter --session-merge-keep true --export out.jsonl

# 仅导出 empty response 样本
claw-filter filter --empty-response true --export out.jsonl

# 仅导出 session merge 标记为 merged 的样本
claw-filter filter --session-merge-status merged --export out.jsonl

# 组合筛选
claw-filter filter --response-helpful-rate ">=0.7" --user-satisfied-rate ">=0.5" --has-error false --export out.jsonl

# 带统计报告
claw-filter filter --response-helpful-rate ">=0.7" --export out.jsonl --report stats.json

# 导出 OpenAI 兼容 + round feedback 侧挂 JSONL
claw-filter filter --response-helpful-rate ">=0.7" --export-format openai_round_feedback --export out.jsonl
```

实现说明:
- CLI filter 和 Web 筛选页共用同一个 UnifiedExporter，不再维护两套导出链路。
- 导出统一采用结构化筛选条件构建，不再在不同入口各自拼 SQL。
- JSONL 导出采用临时文件写入后原子替换，避免生成半截文件。

### 导出格式

- `raw_jsonl`
  每行直接导出一个 sample 的原始 `raw_json`。
- `openai_round_feedback`
  每行导出一个包装后的 JSON 对象，包含：
  - `schema`: 固定为 `openai_round_feedback_v2`
  - `metadata`: sample 级派生字段和处理状态
  - `source_metadata`: 从原始载荷中提取的时间、model requested、user agent、request id、trace id、metadata 等来源信息
  - `conversation.messages`: 规范化后的 OpenAI 兼容消息数组
  - `conversation.tools`: 规范化后的工具定义数组；OpenAI 原生 `tools` 会原样保留，Anthropic request-level `tools` 会被转换为 OpenAI function tools
  - `round_feedback.response_helpful_steps`: 每个 assistant 响应单元的范围和 helpful judgment
  - `round_feedback.user_satisfied_episodes`: 每个 user episode 的范围和 satisfied judgment

`metadata` 当前采用 sample_uid-first 口径：
- `sample_uid`: 对外稳定样本键
- `local_sample_id`: 本地整数辅助键

`conversation.messages` 的规范化规则：
- 如果源数据本身是 OpenAI 风格，原有 `messages` 会直接保留。
- 如果源数据来自 UniRouter/Anthropic request body，顶层 `system` 会被前置转换为 OpenAI `system` message。
- Anthropic `tool_use` / `tool_result` block 会被转换为 OpenAI 风格的 `assistant.tool_calls` 和 `tool` message。

`round_feedback.response_helpful_steps` 中仅保留轻量侧挂信息：
- `response_index`
- `episode_index`
- `assistant_message_index`
- `feedback_kind`
- `feedback_message_start_index`
- `feedback_message_end_index`
- `feedback_payload`
- `response_helpful`
- `llm_error`

`round_feedback.user_satisfied_episodes` 中仅保留轻量侧挂信息：
- `episode_index`
- `message_start_index`
- `message_end_index`
- `signal_from_users`
- `user_satisfied`
- `llm_error`

range 语义说明：
- 所有 message index 都基于最终导出的 `conversation.messages` 重新计算。
- 如果规范化时在最前面新增了 `system` message，后续 assistant step 和 episode 的消息范围会相应后移。
- `conversation.tools` 不参与 range 计算，因为它不在 `messages` 数组中。

完整字段定义、示例记录、兼容性说明见 [docs/export-format.md](docs/export-format.md)。

## 老库回填

对已存在的 DuckDB，可用一次性脚本按导入阶段同样的规则回填 empty_response：

```bash
# 只看回填摘要，不写库
.venv/bin/python scripts/mark_empty_response.py --db-path data/unirouter_20260403_512.duckdb --dry-run

# 正式回填
.venv/bin/python scripts/mark_empty_response.py --db-path data/unirouter_20260403_512.duckdb
```

## Web 页面

项目已包含基于 Streamlit 的单入口可视化工作台，页面与后端使用同一套查询和双层 judgment 语义。

启动方式：

```bash
DB_PATH=data/unirouter_20260403_512.duckdb .venv/bin/streamlit run claw_data_filter/web/app.py --server.port 5000
```

运行后可以在侧边栏查看当前数据库文件，并直接输入新的 DuckDB 路径点击“加载数据库”切换，无需重启 Web。
当前 Web 固定使用浅色主题，不依赖 light/dark 模式切换。

当前页面包括:
- overview: 统计概览
- filter: 数据筛选、勾选与统一导出
- tables: 数据表预览
- detail: 样本详情、response steps 与 user episodes 展示

Web 页面说明:
- 只保留 `app.py` 这一个 Streamlit 入口；侧边栏导航由 query params 路由驱动，不再暴露默认多页标签。
- detail 页复用与 round feedback 相同的 dual-level context builder。
- 导出功能已并入 filter 页，不再维护独立 export 页。
- CLI 与 Web filter 页共用 UnifiedExporter，避免导出逻辑分叉。
- overview/filter/detail/tables 页都可以查看 session merge 标记信息。
- overview/filter/detail/tables 页都已接入 empty_response 信息或过滤能力。
- detail 页 URL 使用 sample_uid 进行 drill-down，local sample_id 仅作辅助展示。

## 目录结构

```
claw_data_filter/
├── cli.py              # CLI 命令
├── config.py           # 配置
├── models/             # 数据模型
├── importers/          # JSONL 导入
├── processors/         # RoundFeedback 处理器
├── storage/            # DuckDB 操作
├── filters/            # 筛选查询
├── exporters/          # 导出
├── llm/                # LLM 客户端
└── web/                # Streamlit 单入口工作台与共享视图组件
```

## 开发

```bash
pip install -e ".[dev]"
pytest tests/ -v           # 运行测试
SKIP_INTEGRATION=1 pytest tests/ -v  # 跳过集成测试
```

推荐回归命令:

```bash
.venv/bin/pytest tests/test_duckdb_store.py tests/test_round_feedback.py tests/test_query_filter.py tests/test_exporters.py tests/test_models.py tests/test_jsonl_importer.py tests/test_integration.py -q
```

## 已知限制

- processing 状态样本的超时回收机制尚未实现；如果进程在 claim 后崩溃，样本可能停留在 processing。
- 当前并发语义主要针对单进程 asyncio 批处理场景。
- Web 页面已与后端语义对齐，但仍缺少页面级自动化测试。

## 字段收敛说明

为避免死字段和语义冲突，samples 表已移除以下字段:

- task_type: 没有真实写入来源，属于未维护字段。
- 旧的 samples.has_error: 与 tool_stats.has_error 语义冲突，且后者才是 round feedback 的真实错误状态。

当前建议:
- 使用 sample_uid 作为稳定导入身份和去重依据。
- 使用整数 id 作为本地辅助键，不再作为页面详情路由或跨表业务关联键。
