# 导出接口文档

本文档描述当前仓库中筛选导出的稳定接口口径。实现来源是 `claw_data_filter.exporters.unified_exporter.UnifiedExporter`，而不是历史兼容逻辑。

## 适用范围

- CLI `claw-filter filter --export ...`
- Web 筛选页触发的导出
- `scripts/run_export.sh` 最终调用的统一导出链路

## 支持的导出格式

### 1. raw_jsonl

- MIME 语义：JSON Lines
- 每行内容：单个样本的原始 `raw_json`
- 用途：保留原始请求载荷，便于回灌、重放、二次解析

示例：

```json
{"request":{"bodyJson":{"model":"minimax-m2.5-fp8","messages":[...]}}}
```

### 2. openai_round_feedback

- MIME 语义：JSON Lines
- 每行内容：单个样本的结构化导出对象
- 当前顶层 schema：`openai_round_feedback_v2`
- 用途：将规范化对话、来源元信息、round feedback 结果一起导出，便于训练、评测、抽检与下游消费

## CLI 接口

基础示例：

```bash
claw-filter filter \
  --session-merge-keep true \
  --empty-response false \
  --has-error false \
  --export-format openai_round_feedback \
  --export out.jsonl \
  --report export_report.json
```

常用筛选参数：

- `--response-progress-rate`
- `--user-satisfied-rate`
- `--user-negative-feedback-rate`
- `--empty-response`
- `--session-merge-keep`
- `--session-merge-status`
- `--has-error`
- `--export-format`
- `--export`
- `--report`
- `--limit`

说明：

- `--export-format` 仅支持 `raw_jsonl` 与 `openai_round_feedback`
- `--report` 输出的是本次导出的统计报告，不影响导出 JSONL 主体结构
- CLI 与 Web 共用同一套导出器，因此两者产物结构一致

## openai_round_feedback_v2 顶层结构

每行记录是一个 JSON 对象，顶层字段如下：

```json
{
  "schema": "openai_round_feedback_v2",
  "metadata": {...},
  "source_metadata": {...},
  "conversation": {...},
  "round_feedback": {...}
}
```

字段说明：

### schema

- 类型：`string`
- 固定值：`openai_round_feedback_v2`
- 含义：导出记录版本号

### metadata

- 类型：`object`
- 含义：样本级派生字段、处理状态与聚合指标

字段定义：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `sample_uid` | `string` | 对外稳定样本键；跨表、跨模块统一引用该字段 |
| `local_sample_id` | `integer` | 本地数据库整数键，仅用于本地排查、排序、辅助展示 |
| `imported_at` | `string \| null` | 导入时间，ISO 8601 字符串 |
| `processing_status` | `string \| null` | 当前处理状态 |
| `empty_response` | `boolean` | 是否存在用户输入但无 assistant 回复 |
| `session_merge_status` | `string \| null` | session merge 状态，如 `keep`、`merged`，未标记时为 `null` |
| `session_merge_keep` | `boolean \| null` | 当前样本是否保留为 merge 代表样本 |
| `session_merge_reason` | `string \| null` | merge 决策原因 |
| `num_turns` | `integer` | 对话轮次数 |
| `expected_judgment_count` | `integer` | 预期 judgment 总数 |
| `expected_response_judgment_count` | `integer` | 预期 assistant response judgment 数 |
| `expected_episode_judgment_count` | `integer` | 预期 user episode judgment 数 |
| `num_tool_calls` | `integer` | 样本中的工具调用数量 |
| `response_progress_rate` | `number \| null` | assistant response 级 progress 比例 |
| `response_regress_rate` | `number \| null` | assistant response 级 regress 比例 |
| `user_satisfied_rate` | `number \| null` | user episode 级 satisfied 比例 |
| `user_negative_feedback_rate` | `number \| null` | user episode 级负反馈比例 |
| `has_error` | `boolean` | 样本是否被记录为有错误 |

### source_metadata

- 类型：`object`
- 含义：从原始请求载荷中抽取的来源信息，不改写业务语义

字段定义：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `timestamp` | `string \| null` | 原始时间戳，优先从 payload/request 中提取 |
| `model_requested` | `string \| null` | 请求中声明的模型名 |
| `user_agent` | `string \| null` | 请求头或原始载荷中的 user agent |
| `request_id` | `string \| null` | 请求标识 |
| `trace_id` | `string \| null` | trace 标识 |
| `source_format` | `string` | 当前识别出的源格式，取值为 `openai` 或 `anthropic` |
| `metadata` | `object \| array \| string \| number \| boolean \| null` | 原始样本附带的 metadata，原样透传 |

### conversation

- 类型：`object`
- 含义：规范化后的 OpenAI 兼容对话载荷

字段定义：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `messages` | `array<object>` | 规范化后的消息数组 |
| `tools` | `array<object>` | 可选字段；当原始请求中存在工具定义时输出 |

规范化规则：

- 若源数据本身是 OpenAI 风格，原有 `messages` 基本保持原样
- 若源数据来自 UniRouter/Anthropic request body，顶层 `system` 会被转成前置的 OpenAI `system` message
- Anthropic `tool_use` / `tool_result` block 会被转成 OpenAI 风格的 `assistant.tool_calls` 和 `tool` message
- 若请求级 `tools` 为 Anthropic 格式，会转换成 OpenAI function tools；若原本就是 OpenAI tools，则原样保留

### round_feedback

- 类型：`object`
- 含义：双层 judgment 结果侧挂

字段定义：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `response_progress_steps` | `array<object>` | assistant response 单元级 judgment |
| `user_satisfied_episodes` | `array<object>` | user episode 级 judgment |

## response_progress_steps

每个元素对应一个 assistant response 单元。

字段定义：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `response_index` | `integer` | 当前样本内的 response 单元索引 |
| `episode_index` | `integer` | 当前 response 所属的 user episode 索引 |
| `assistant_message_index` | `integer` | 对应 assistant message 在 `conversation.messages` 中的索引 |
| `feedback_kind` | `string` | 反馈块类型 |
| `feedback_message_start_index` | `integer \| null` | 反馈块起始消息索引 |
| `feedback_message_end_index` | `integer \| null` | 反馈块结束消息索引 |
| `feedback_payload` | `array` | 用于判断 progress 的反馈内容 |
| `response_progress` | `string \| null` | judgment 结果；通常为 `yes`、`no`、`uncertain`，未产出时为 `null` |
| `llm_error` | `boolean` | 本条 judgment 是否由 LLM 调用错误导致失败 |

## user_satisfied_episodes

每个元素对应一个 user episode。

字段定义：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `episode_index` | `integer` | 当前样本内的 episode 索引 |
| `message_start_index` | `integer` | episode 起始 user message 在 `conversation.messages` 中的索引 |
| `message_end_index` | `integer` | episode 截止位置索引 |
| `signal_from_users` | `array` | 用于判断 satisfied 的用户侧信号 |
| `user_satisfied` | `string \| null` | judgment 结果；通常为 `yes`、`no`、`uncertain`，未产出时为 `null` |
| `llm_error` | `boolean` | 本条 judgment 是否由 LLM 调用错误导致失败 |

## 真实样例

下面示例来自当前仓库一次真实导出，字段已做截断，仅保留关键结构：

```json
{
  "schema": "openai_round_feedback_v2",
  "metadata": {
    "sample_uid": "879528acc7fdd3a59f64789c84529384d3c6a08655363d8522200cc7b8cc0739",
    "local_sample_id": 1,
    "imported_at": "2026-04-09T17:04:57.251147",
    "processing_status": "completed",
    "empty_response": false,
    "session_merge_status": "keep",
    "session_merge_keep": true,
    "session_merge_reason": "leaf_sequence",
    "num_turns": 4,
    "expected_judgment_count": 17,
    "expected_response_judgment_count": 13,
    "expected_episode_judgment_count": 4,
    "num_tool_calls": 11,
    "response_progress_rate": 0.23076923076923078,
    "response_regress_rate": 0.7692307692307693,
    "user_satisfied_rate": 0.0,
    "user_negative_feedback_rate": 1.0,
    "has_error": false
  },
  "source_metadata": {
    "timestamp": null,
    "model_requested": "minimax-m2.5-fp8",
    "user_agent": "Anthropic/JS 0.73.0",
    "request_id": null,
    "trace_id": null,
    "source_format": "anthropic",
    "metadata": null
  },
  "conversation": {
    "messages": [
      {"role": "system", "content": "..."},
      {"role": "user", "content": "..."},
      {"role": "assistant", "content": "..."}
    ],
    "tools": [
      {"type": "function", "function": {"name": "read", "description": "..."}}
    ]
  },
  "round_feedback": {
    "response_progress_steps": [
      {
        "response_index": 0,
        "episode_index": 0,
        "assistant_message_index": 2,
        "feedback_kind": "tool_result",
        "feedback_message_start_index": 3,
        "feedback_message_end_index": 5,
        "feedback_payload": ["..."],
        "response_progress": "no",
        "llm_error": false
      }
    ],
    "user_satisfied_episodes": [
      {
        "episode_index": 0,
        "message_start_index": 1,
        "message_end_index": 6,
        "signal_from_users": ["..."],
        "user_satisfied": "uncertain",
        "llm_error": false
      }
    ]
  }
}
```

## 兼容性说明

- 当前文档不再描述旧版 `turn_judgments` 兼容结构
- `sample_uid` 是对外稳定主键；`local_sample_id` 不是跨库稳定标识
- `schema=openai_round_feedback_v2` 表示当前结构已经是双层 judgment 版本

## 消费建议

- 若下游做主键关联，请使用 `metadata.sample_uid`
- 若下游只做本地人工抽样或页面跳转，才使用 `metadata.local_sample_id`
- 若下游需要判断是否可用于训练或质检，建议联合使用：
  - `metadata.processing_status`
  - `metadata.empty_response`
  - `metadata.session_merge_keep`
  - `metadata.has_error`
- 若下游需要分层评估，请分别消费：
  - `round_feedback.response_progress_steps`
  - `round_feedback.user_satisfied_episodes`