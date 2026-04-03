# Claw Data Filter

LLM-powered agent conversation data filtering tool. Import JSONL files, run round feedback judgments, filter and export high-quality data.

当前主链路:

1. 导入 OpenAI 或 UniRouter 对话数据。
2. 将样本写入 DuckDB，并生成稳定的 sample_uid 与本地整数 id。
3. round feedback 以 claim 模式批量领取 pending 或 failed 样本。
4. 按统一 turn 语义做逐轮判断，并以原子方式写回结果。
5. 样本进入 completed 或 failed 状态。
6. 通过 CLI 或 Web 按结构化条件筛选和导出。

## Quick Start

```bash
# 1. 导入数据
claw-filter import data.jsonl

# 2. 运行 round feedback 评分（需要 LLM 服务器）
claw-filter pressure-test  # 先测试稳定性
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

导出脚本可配置项包括：
- DB_PATH
- EXPORT_PATH
- REPORT_PATH
- RESPONSE_HELPFUL_RATE
- USER_SATISFIED_RATE
- HAS_ERROR
- LIMIT
- GENERATE_REPORT

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
- 导入阶段会统一提取消息并生成 user_query、assistant_response、num_turns、expected_judgment_count 等派生字段。
- 导入阶段还会基于原始 payload 生成 SHA-256 的 sample_uid，用作稳定、低碰撞的导入身份；整数 id 继续作为本地关系键。
- judged turn 不是简单按 assistant 消息数计算，而是按同一 user 下的 assistant/tool/assistant 序列合并后的轮次计算。

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
- 结果写入采用原子替换，避免 sample 聚合结果和 turn_judgments 明细不一致。

## 评分维度

每轮 assistant 回复判断两个指标：

| 维度 | 值 | 说明 |
|------|-----|------|
| **response_helpful** | yes/no/uncertain | 回复对用户是否有帮助 |
| **user_satisfied** | yes/no/uncertain/neutral | 基于后续用户行为判断满意度 |

**user_satisfied 判定：**
- 用户追问/澄清 → no
- 用户确认/继续 → yes
- 用户转新话题 → neutral
- 无明确信号 → uncertain

turn 语义说明:
- 一个 judged turn 以 user 消息开始。
- 同一 user 之后连续的 assistant、tool、assistant 消息会被合并为同一轮。
- 这可以更准确地覆盖 agent 场景中的 tool call 和 final answer。

## 筛选字段

| 字段 | 来源 | 说明 |
|------|------|------|
| response_helpful_rate | samples.tool_stats | helpful=yes 比例 |
| user_satisfied_rate | samples.tool_stats | satisfied=yes 比例 |
| num_turns | samples | 轮次数 |
| has_error | samples.tool_stats | round feedback 是否含错误 |

## 存储结构

主要表:

- samples
  记录 sample_uid、原始 JSON、派生字段、tool_stats、processing_status 等样本级信息。
- turn_judgments
  记录每个 judged turn 的 response_helpful、user_satisfied、signal_from_users、llm_error。

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

# 组合筛选
claw-filter filter --response-helpful-rate ">=0.7" --user-satisfied-rate ">=0.5" --has-error false --export out.jsonl

# 带统计报告
claw-filter filter --response-helpful-rate ">=0.7" --export out.jsonl --report stats.json
```

实现说明:
- CLI filter 走参数化查询，不直接把筛选值拼接进 SQL。
- JSONL 导出采用临时文件写入后原子替换，避免生成半截文件。

## Web 页面

项目已包含基于 Streamlit 的可视化页面，页面与后端使用同一套查询和 turn 语义。

当前页面包括:
- overview: 统计概览
- filter: 数据筛选与导出选中
- export: 按条件导出
- tables: 数据表预览
- detail: 样本详情与逐轮 judgment 展示

Web 页面说明:
- detail 页复用与 round feedback 相同的 turn builder。
- filter/export 页复用统一查询语义，避免与 CLI 逻辑分叉。

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
└── web/                # Streamlit 可视化页面
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
- 使用整数 id 作为本地主键、外键和页面详情路由参数。
