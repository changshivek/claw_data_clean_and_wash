# RoundFeedbackProcessor 设计文档

## 概述

开发一个逐轮反馈判断系统 `RoundFeedbackProcessor`，用于对 agent 对话数据进行细粒度质量筛选。该系统以小模型作为判断主体，通过分析后续轮次的用户反馈信号来评估当前轮次 assistant 的决策质量，为 RL 训练提供细粒度 reward 信号。

## 数据来源

- **输入格式**: `items.jsonl`，每行是一个完整对话 session
- **数据结构**: 包含 `log`、`request.bodyJson.messages` 和 `response`
- **消息格式**: OpenAI 兼容格式，`role` 为 user/assistant/tool，`content` 为 text 或 tool_use/tool_result

## 目标

1. 对每条数据的每个 assistant 决策轮次进行 4 维度判断
2. 基于用户后续反馈信号归因当前轮质量
3. 产出每条数据的 session 级别工具统计
4. 存入 DuckDB 供后续过滤和 RL 使用

## 层级体系

### 层级1: 整条数据级别（规则判断）
- `has_error`: tool_result 中包含 error/exception 关键词
- `is_test`: 规则判断（如消息数过少）

### 层级2: Session级别工具统计（汇总得出）
从逐轮判断结果汇总：
```json
{
  "tool_used": 5,
  "tool_success": 4,
  "tool_unnecessary": 1,
  "tool_missing": 0,
  "partial": false
}
```

**JSON Schema 约束**:
- `tool_used`: integer >= 0
- `tool_success`: integer >= 0, <= tool_used
- `tool_unnecessary`: integer >= 0, <= tool_used
- `tool_missing`: integer >= 0
- `partial`: boolean, true 表示部分轮次判断失败

### 层级3: 逐轮反馈判断（核心）

#### 轮次定义
```
轮次N: [user_N 或 tool_result_N] → [assistant_N] → 信号窗口(后续最多3个user消息)
```

#### 4个判断维度

| 维度 | 问题 | 值域 |
|-----|------|------|
| `need_tool` | 这个问题是否需要工具调用？ | yes/no/uncertain |
| `tool_correct` | 如果用了工具，工具选择正确吗？ | yes/no/uncertain |
| `response_helpful` | 这个回答对用户有帮助吗？ | yes/no/uncertain |
| `user_satisfied` | 用户对这个回答满意吗？ | yes/no/uncertain |

#### 信号归因规则

当判断轮次 N 时，查看轮次 N 之后、轮次 N+M 之前的 user 消息（M ≤ 3）：

| 信号类型 | 具体表现 | 归因结果 |
|---------|---------|---------|
| 用户追问 | 用户要求补充、澄清、详细说明 | `user_satisfied = no` |
| 用户确认 | 用户表示理解、继续、满意 | `user_satisfied = yes` |
| 用户新话题 | 用户转向完全不相关的全新任务 | `user_satisfied = neutral` |
| 无用户反馈 | 多轮后用户无明确反应 | `user_satisfied = uncertain` |

**注意**: tool_result 不参与信号归因，只作为上下文

**neutral 判断示例**:
- 用户从"查询天气"转向"帮我写代码" → neutral
- 用户从"北京天气"转向"上海天气"（同一领域不同实体）→ yes（视为继续）

## 输入上下文构建规则

| 轮次 | 保留内容 | 去除内容 |
|-----|---------|---------|
| 当前轮（待判断） | 完整的 user + tool_result + assistant | - |
| 历史轮次 | user + assistant | 所有 tool 调用和 tool_result |
| 全局 | - | system prompt |

**格式区分示例**:
```
=== 历史对话 ===
[user]: 你好，我想了解北京的天气
[assistant]: 北京今天晴，25度。
[user]: 那上海呢？
[assistant]: 上海今天多云，22度。

=== 当前轮 ===
[user]: 深圳呢？
[tool_result]: 深圳今天小雨，20度
[assistant]: 深圳今天小雨，气温20度。

=== 待判断 ===
[assistant回复]: 深圳今天小雨，气温20度。
[后续用户]: 好的，谢谢

请判断用户是否满意（yes/no/uncertain）：
```

**超出 context 时的处理**:
- Token 预算：单次判断输入不超过 4096 tokens
- 截断策略：优先保留最新轮次，从最早期开始截断
- 截断标记：被截断的轮次不参与判断，不影响结果

## 数据模型

### samples 表扩展
```sql
ALTER TABLE samples ADD COLUMN tool_stats JSON;
```

### turn_judgments 表
```sql
CREATE TABLE turn_judgments (
    id INTEGER PRIMARY KEY,
    sample_id INTEGER REFERENCES samples(id),
    turn_index INTEGER,
    need_tool TEXT,           -- yes/no/uncertain
    tool_correct TEXT,        -- yes/no/uncertain
    response_helpful TEXT,    -- yes/no/uncertain
    user_satisfied TEXT,      -- yes/no/uncertain
    signal_from_users JSON,   -- ["用户消息1", ...]
    llm_error BOOLEAN,        -- 判断是否失败，供后续补救
    created_at TIMESTAMP
);
CREATE INDEX idx_turn_judgments_sample ON turn_judgments(sample_id);
```

## Prompt 设计

### Group 1: 工具相关

```
=== 历史对话（仅user/assistant）===
[user]: ...
[assistant]: ...
...

=== 当前轮 ===
[user]: ...
[tool_result]: ...
[assistant]: ...

请判断：
1. need_tool: 当前问题是否需要工具调用？（yes/no/uncertain）
2. tool_correct: 如果用了工具，工具选择正确吗？（yes/no/uncertain）

答案格式：need_tool=yes; tool_correct=no
```

**规则**:
- `need_tool=no` 但实际用了工具 → `tool_correct=no`
- `need_tool=yes` 但没用工具 → `tool_correct=no`
- `need_tool=uncertain` 时 → `tool_correct=uncertain`

### Group 2: 效果相关

```
=== 当前轮 ===
[user]: ...
[tool_result]: ...
[assistant]: ...

=== 后续用户信号（最多3轮）===
[user]: ...
[user]: ...

请判断：
1. response_helpful: 这个回答对用户有帮助吗？（yes/no/uncertain）
2. user_satisfied: 用户对这个回答满意吗？（yes/no/uncertain）

答案格式：response_helpful=yes; user_satisfied=no
```

**判断规则**:
- 用户追问（要求补充/澄清） → `user_satisfied = no`
- 用户确认/继续/满意 → `user_satisfied = yes`
- 用户转向新话题 → `user_satisfied = neutral`
- 无明确反馈 → `user_satisfied = uncertain`

## 并发架构

使用 asyncio + Semaphore 控制总并发数：

```
RoundFeedbackProcessor
    ├── 处理样本队列
    │   ├── TurnContextBuilder: 构建每轮输入
    │   └── asyncio.Semaphore(max_concurrency): 控制并发
    │       ├── RoundJudgmentProcessor (并发执行)
    │       │   ├── Group1: need_tool + tool_correct
    │       │   └── Group2: response_helpful + user_satisfied
    │       └── ToolStatsAggregator: 即时计算 tool_stats
    └── 即时写入 DuckDB
```

**并发模型**:

```
每个 turn 作为一个处理单元：
- 每个 turn 创建 1 个异步任务
- 任务内部：Group1 和 Group2 两个 prompt 并行发送（asyncio.gather）
- Semaphore(max_concurrency) 控制全局并发任务数

伪代码：
async def process_turn(turn: TurnContext, semaphore: Semaphore) -> TurnJudgment:
    async with semaphore:
        group1_result, group2_result = await asyncio.gather(
            judge_group1(turn),  # need_tool + tool_correct
            judge_group2(turn)   # response_helpful + user_satisfied
        )
        return merge_results(group1_result, group2_result)
```

## 错误处理策略

| 场景 | 处理策略 |
|-----|---------|
| **LLM 连接错误/超时** | 指数退避重试3次（2s, 4s, 8s），timeout=60s → 标记 `llm_error=True` |
| **输出格式解析失败** | max_tokens=50 → 重试1次 → 仍失败标记 `llm_error=True`，该字段值设为 null |
| **某轮失败(LLM错误)** | 该轮标记失败（llm_error=True），其他轮继续 |
| **sample 全部轮次失败** | 标记该 sample 整体失败（tool_stats.partial=True） |

**容错原则**: 严格模式，保证数据质量

**重试策略详情**:
```python
retry_strategy = [
    {"attempt": 1, "max_tokens": 50, "timeout": 60},
    {"attempt": 2, "max_tokens": 50, "timeout": 60},
]
# 仍失败 → llm_error=True，字段值设为 null
```

## 压力测试

启动前执行压力测试，确保小模型在预设并发数下稳定：

```python
async def pressure_test(max_concurrency: int, duration: int = 30):
    """
    测试小模型在预设并发下是否稳定
    """
```

**测试场景**:
- 预设最大并发数的满载测试
- 30秒持续压力测试
- 成功率 > 95%

**测试指标**:
| 指标 | 要求 |
|-----|------|
| 成功率 | > 95% |
| P95 latency | < 10s |
| P99 latency | < 30s |
| 无 rate limit 触发 | - |

**失败处理**: 任一指标不达标则报错退出，不执行正式任务

## 边界情况处理

| 边界情况 | 处理方式 |
|---------|---------|
| sample 无 assistant 消息 | 跳过该 sample，标记为无需判断 |
| session 首轮是 assistant（如 system 引导） | 仍作为 turn_index=0 处理 |
| 连续多条 tool_result 无 assistant 回复 | 合并为同一轮的上下文 |
| 用户消息在 tool_result 之前 | 按消息顺序处理，tool_result 视为紧跟前一个 assistant |
| 信号窗口内无 user 消息 | user_satisfied 设为 uncertain |
| 解析失败 | 标记 llm_error=True，字段值设为 null |

| 组件 | 文件 | 职责 |
|-----|------|-----|
| `TurnContextBuilder` | `processors/round_feedback.py` | 按规则构建每轮判断输入 |
| `RoundJudgmentProcessor` | `processors/round_feedback.py` | asyncio并发执行4维度判断 |
| `ToolStatsAggregator` | `processors/round_feedback.py` | 即时计算 tool_stats |
| `PressureTest` | `processors/round_feedback.py` | 启动前压力测试 |
| `RoundJudgment` model | `models/round_judgment.py` | Pydantic 模型 |
| Store 扩展 | `storage/duckdb_store.py` | 新增 turn_judgments 表操作 |

## 实现计划

1. 创建 `RoundJudgment` model
2. 扩展 `DuckDBStore` 增加 turn_judgments 表操作
3. 实现 `TurnContextBuilder` - 按规则构建输入
4. 实现 `RoundJudgmentProcessor` - asyncio并发判断
5. 实现 `PressureTest` - 启动压力测试
6. 集成到 CLI（新增 `round-feedback` 命令）
7. 编写测试用例

## 监控指标

| 指标 | 用途 |
|-----|------|
| 判断成功率 | 评估数据质量 |
| 每轮判断平均耗时 | 评估效率 |
| 各维度 uncertain 比例 | 过高说明 prompt 需调整 |
| token 消耗统计 | 成本控制 |
| LLM error 比例 | 评估系统稳定性 |

## 依赖

- 现有 `claw_data_filter` 框架
- `LLMClient` 用于小模型调用
- `DuckDBStore` 用于存储
- Python 3.12+, asyncio
