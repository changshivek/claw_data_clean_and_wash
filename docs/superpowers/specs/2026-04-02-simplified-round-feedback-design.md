# Simplified Round Feedback Design

> **Goal:** Remove Evaluation layer, simplify Round Feedback to two simple judgments, unify data format at import.

## Overview

Remove the `evaluate` command and Evaluation model entirely. Simplify RoundFeedback to only two per-turn judgments:
- `response_helpful`: 判断整组（user + tool_result + assistant）是否有帮助
- `user_satisfied`: 只看 assistant text content，通过后续用户行为判断满意度

## Data Format Unification

### Import: Convert Anthropic → OpenAI Format

| Anthropic (source) | OpenAI (normalized) |
|-------------------|---------------------|
| `user {content: [tool_result, text]}` | `tool {role=tool, tool_call_id, content}` + `user {content}` |
| `assistant {content: [text, tool_use]}` | `assistant {content, tool_calls}` |

**实现位置:** `Sample.from_dict()` 方法内增加格式检测和转换逻辑

**检测逻辑:**
```python
def detect_format(messages: list) -> str:
    """检测消息格式：返回 'openai' 或 'anthropic'"""
    for msg in messages:
        if msg.get("role") == "tool":
            return "openai"
        content = msg.get("content", [])
        if isinstance(content, list):
            for c in content:
                if isinstance(c, dict) and c.get("type") == "tool_result":
                    return "anthropic"
    return "openai"  # 默认为 OpenAI 格式
```

**转换逻辑 (Anthropic → OpenAI):**
```python
def anthropic_to_openai(messages: list) -> list:
    """将 Anthropic 格式转换为 OpenAI 格式"""
    result = []
    for msg in messages:
        role = msg.get("role")
        content = msg.get("content", [])

        if role == "user" and isinstance(content, list):
            tool_results = [c for c in content if c.get("type") == "tool_result"]
            text_parts = [c.get("text") for c in content if c.get("type") == "text" and c.get("text")]

            # 先输出 tool 消息
            for tr in tool_results:
                result.append({
                    "role": "tool",
                    "tool_call_id": tr.get("tool_use_id"),
                    "content": tr.get("content", "")
                })
            # 再输出 user 消息（只保留 text 部分）
            if text_parts:
                result.append({"role": "user", "content": "".join(text_parts)})
        else:
            result.append(msg)
    return result
```

## Data Models

### Samples Table (扩展)
```sql
ALTER TABLE samples ADD COLUMN task_type TEXT;
ALTER TABLE samples ADD COLUMN tool_stats JSON;  -- 已存在
```

`tool_stats` 结构:
```json
{
  "response_helpful_rate": 0.85,
  "user_satisfied_rate": 0.72,
  "total_turns": 5,
  "has_error": false
}
```

### Turn Judgments Table (简化)
```sql
CREATE TABLE turn_judgments (
    id INTEGER PRIMARY KEY,
    sample_id INTEGER,
    turn_index INTEGER,
    response_helpful TEXT,     -- yes/no/uncertain
    user_satisfied TEXT,       -- yes/no/uncertain
    signal_from_users JSON,    -- ["用户消息1", ...]
    llm_error BOOLEAN,
    created_at TIMESTAMP
);
```

**移除:** `need_tool`, `tool_correct`

## Per-Turn Judgment

### Turn 定义
```
轮次N: [user 或 tool_result] → [assistant] → 信号窗口(后续最多3个user消息)
```

### 判断规则

**1. response_helpful (整组判断)**
- 输入: user消息 + tool_result + assistant回复
- 问题: 这个回答（包含工具调用和结果）对用户有帮助吗？
- 值域: yes/no/uncertain

**2. user_satisfied (只看assistant text)**
- 输入: assistant的text content + 后续用户信号
- 问题: 用户对assistant的回复满意吗？
- 值域: yes/no/uncertain

### 信号归因规则 (user_satisfied)

| 信号类型 | 表现 | 归因结果 |
|---------|------|---------|
| 用户追问 | 要求补充、澄清、详细说明 | `user_satisfied = no` |
| 用户确认 | 表示理解、继续、满意 | `user_satisfied = yes` |
| 用户新话题 | 转向完全不相关的全新任务 | `user_satisfied = neutral` |
| 无用户反馈 | 多轮后无明确反应 | `user_satisfied = uncertain` |

## Layer 2 Aggregation

从 Layer 3 聚合到 `samples.tool_stats`:

```python
def aggregate(judgments: list[RoundJudgment]) -> dict:
    total = len(judgments)
    helpful_yes = sum(1 for j in judgments if j.response_helpful == "yes")
    # user_satisfied: yes=正面, neutral=新话题(不计入满意), uncertain=不确定
    satisfied_yes = sum(1 for j in judgments if j.user_satisfied == "yes")

    return {
        "response_helpful_rate": helpful_yes / total if total > 0 else 0,
        "user_satisfied_rate": satisfied_yes / total if total > 0 else 0,
        "total_turns": total,
        "has_error": any(j.llm_error for j in judgments)
    }
```

## Prompt Design (简化版)

```
=== 当前轮 ===
[user]: 用户问题
[tool_result]: 工具结果
[assistant]: 助手回复

=== 后续用户信号（最多3轮）===
[user]: 追问或确认...

请判断：
1. response_helpful: 这个回答对用户有帮助吗？（yes/no/uncertain）
2. user_satisfied: 用户对助手回复满意吗？（yes/no/uncertain）

答案格式：response_helpful=yes; user_satisfied=no
```

## CLI Changes

### 移除
- `claw-filter evaluate` 命令
- `claw-filter stats` 中的 evaluation 统计

### 修改
- `claw-filter filter`: 基于 samples.tool_stats 和 samples.task_type 筛选
- `claw-filter stats`: 显示 samples 统计（total, avg_response_helpful_rate, avg_user_satisfied_rate, error_count）

### 新增/保留
- `claw-filter import` (增加格式转换)
- `claw-filter round-feedback` (简化版)
- `claw-filter pressure-test` (保留)

## Filter Query

### 可筛选字段
```python
ALLOWED_FIELDS = [
    "task_type",
    "response_helpful_rate",  # 从 tool_stats 提取
    "user_satisfied_rate",     # 从 tool_stats 提取
    "has_error",
    "num_turns",
    "num_tool_calls",
]
```

### 示例
```bash
claw-filter filter --response-helpful-rate ">=0.8" --user-satisfied-rate ">=0.5" --export output.jsonl
```

## Files to Remove

- `claw_data_filter/models/evaluation.py`
- `claw_data_filter/processors/evaluator.py`
- `claw_data_filter/prompts/evaluation_prompt.py`

## Files to Modify

- `claw_data_filter/models/sample.py`: 支持 Anthropic 格式检测和转换
- `claw_data_filter/models/round_judgment.py`: 移除 need_tool, tool_correct
- `claw_data_filter/storage/duckdb_store.py`: 新增 task_type 字段，修改 tool_stats 结构
- `claw_data_filter/processors/round_feedback.py`: 简化 prompt 和判断逻辑
- `claw_data_filter/cli.py`: 移除 evaluate 命令，修改 filter/stats
- `claw_data_filter/filters/query.py`: 更新允许字段
- `claw_data_filter/exporters/jsonl_exporter.py`: 修改 JOIN 逻辑
- `claw_data_filter/exporters/report_exporter.py`: 修改统计查询

## Migration Strategy

1. 数据库 schema 迁移：新增 task_type 列
2. 旧数据（无 turn_judgments）需要重新运行 round-feedback
3. 删除 evaluations 表及所有相关代码
