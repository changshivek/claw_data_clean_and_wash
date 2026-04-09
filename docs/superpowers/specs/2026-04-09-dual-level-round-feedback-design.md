# 双层级 Round Feedback 设计

> 日期: 2026-04-09
>
> 目标: 将 response_helpful 与 user_satisfied 从同一套 judged turn 语义中解耦，分别回到各自真正对应的评判对象与反馈信号。

## 背景

当前实现把一个 user 之后直到下一条 user 之前的 assistant/tool/assistant 链整体视为一轮 judged turn，然后同时产出：

- response_helpful
- user_satisfied

这个做法的问题是：

- response_helpful 实际关注的是 assistant 当前这一步是否正确、有帮助。
- user_satisfied 关注的是用户对整段交互结果是否满意。
- 两者的评判对象和可用反馈信号并不相同，共用边界会造成归因污染。

典型失真场景：

1. assistant 第一步工具选错，但第二步 assistant 用额外解释补救成功。
2. 旧逻辑会把整段链条合并后评为 helpful=yes。
3. 结果是前一跳错误工具调用也被后续补救“洗白”。

## 设计结论

采用双层级判定更合理：

- 第一层: response_helpful，按 assistant 响应单元评判。
- 第二层: user_satisfied，按 user episode 评判。

当前已确认的设计决策：

- judgment 明细必须拆成两张表。
- sample 表主键改为导入时生成的 sample_uid，不再以整数 id 作为主联络键。
- Web 展示拆成两个视图：user_satisfied 视图延续当前 episode 风格，response_helpful 视图单独设计。
- 导出格式采用新的 schema，但继续沿用 metadata + conversation + round_feedback 的基本外形，降低下游切换成本。

这是更符合 agent 对话结构的方案，也是原始设计意图的更精确表达。

## 层级一: response_helpful

### 评判对象

一个 assistant 响应单元只包含当前 assistant 这一步输出的内容：

- assistant text
- assistant 中显式出现的 think 或 reasoning 文本（如果原始数据保留且允许用于评判）
- tool use 决策
- 工具名选择
- 参数构造
- 命令拼接或调用构造

不包含：

- 后续 assistant 的补充解释
- 后续 assistant 的修复性回答

### 边界规则

assistant 响应单元的右边界由紧邻的下一跳反馈块决定。

下一跳反馈块只能是两种之一：

1. tool result block
2. user 消息

建议规则：

- 当 assistant 后面紧跟一个或多个连续 tool 消息时，这段连续 tool 消息合并视为一个 feedback block。
- 当 assistant 后面直接出现 user 时，这条 user 消息就是 feedback block。
- 一旦进入下一个 assistant，则说明上一 assistant 的反馈边界已经结束，不能再拿后续 assistant 内容回填给上一单元。

### 判定原则

response_helpful 必须严格基于紧邻反馈块判断：

- assistant 的 tool use 是否带来了有效 tool result
- assistant 的 text 是否引导用户进入正确下一步
- assistant 的输出是否让接下来的 user 回复表现为接受、继续、或无需纠错

### 示例

示例 A:

```text
user: 帮我查北京天气
assistant: 我来调用天气工具查询。
tool: 北京今日晴，25 度
assistant: 北京今天晴，25 度。
user: 好的
```

按本设计应拆成两个 helpful judgment：

1. assistant: 我来调用天气工具查询。
   signal: 紧邻 tool result
   judgment: 工具选择和调用是否 helpful

2. assistant: 北京今天晴，25 度。
   signal: 紧邻 user=好的
   judgment: 最终文本回答是否 helpful

## 层级二: user_satisfied

### 评判对象

一个 user episode 的边界为：

- 从某条真实 user 消息开始
- 到下一条真实 user 消息出现前结束

这段范围内的所有 assistant/tool 交互共同构成一个 satisfaction episode。

换言之，user_satisfied 针对的是整段交互结果，而不是其中某一个 assistant step。

### 信号窗口

user_satisfied 的信号来自 episode 结束后最多 3 条 user 文本消息：

- 只看 user 文本
- 不看后续 assistant
- 不把 tool 结果作为 satisfaction signal

### 判定原则

- 用户追问、纠错、要求补充: no
- 用户确认、接受结果、继续推进原任务: yes
- 用户切到无关新话题: neutral
- 没有足够明确的后续 user 信号: uncertain

### 示例

仍以上述天气例子：

```text
user: 帮我查北京天气
assistant: 我来调用天气工具查询。
tool: 北京今日晴，25 度
assistant: 北京今天晴，25 度。
user: 好的
```

这里只产生一个 user_satisfied judgment：

- episode: 从 user“帮我查北京天气” 到 user“好的” 之前的全部 assistant/tool 交互
- signal window: user“好的”
- judgment: yes

## 为什么这套方案更合理

### 1. 归因更干净

response_helpful 只对当前 assistant step 负责，避免后续补救把前一步错误掩盖掉。

### 2. 满意度语义更稳定

用户满意度天然是对整段服务体验的反馈，不适合强行切到每一个 assistant step 上。

### 3. 更符合 agent 交互现实

agent 对话中经常出现：

- assistant 规划
- assistant 发 tool call
- tool 返回
- assistant 总结

这些步骤对 helpful 和 satisfied 的归因粒度本来就不同。

## 后续语义收束方向（未实施）

以下内容是 2026-04-09 在双层设计稳定后追加确认的下一步方向，当前仅作为后续优化目标，不代表代码已经实现。

### 背景

当前 step 级指标名仍为 `response_helpful`，但在实践讨论中暴露出两个问题：

- 名称过宽，容易被理解成“用户觉得有帮助”或“最终答案整体有帮助”。
- 现有 prompt 更偏向基于紧邻反馈判断局部有效性，尚未稳定覆盖“工具选择是否正确”和“当前 step 是否推进问题状态”这类更工程化的判据。

因此，后续更合理的方向不是继续扩大 `response_helpful` 的含义，而是将它收束为一个更明确的 step 级推进指标。

### 推荐方向

推荐将 step 级主指标从 `response_helpful` 逐步收束/重命名为 `response_progress`。

其目标语义为：

- 当前 assistant response unit 是否让当前问题状态发生了正向推进。

它关注的是过程推进，而不是最终满意度；`user_satisfied` 仍保留为 episode 级整体评价。

### response_progress 的推荐判据

在不改变当前 response unit 边界的前提下，`response_progress` 推荐基于以下四类判据综合判断：

1. 行动方向是否正确
  - 是否选择了合理的处理路径或工具
  - 是否没有明显跑偏到无关动作

2. 执行结果是否有效
  - 是否拿到了与当前目标相关的信息、状态或中间结果
  - 如果没有工具调用，当前文本是否提供了真实可执行的下一步

3. 任务状态是否前进
  - 是否减少了不确定性
  - 是否完成了必要中间步骤
  - 是否把问题推进到更接近解决的状态

4. 是否出现负向推进
  - 是否把任务带偏
  - 是否引入误导或增加返工成本

### prompt 边界约束

后续若实施 `response_progress`，在当前“不改 unit 边界”的前提下，prompt 输入边界建议如下：

可以使用：

- 当前 user request
- 当前 assistant response unit
- 当前 unit 内的 tool calls
- 紧邻 feedback block
- 当前 unit 之前的压缩执行背景（仅限当前 episode 内）

不要使用：

- next assistant text
- next assistant reasoning
- 更远的后续补救内容

### 前序执行背景的推荐构造

为了支持连续工具调用场景，允许在 prompt 中带入当前 unit 之前的有限执行背景，但推荐使用规则化压缩，而不是额外增加一次 LLM 摘要调用。

推荐仅保留最近 3 个前序 response unit 的摘要，并且明确按 response unit 计数，而不是按单条 message 计数。

每个前序 response unit 推荐只保留以下字段：

- `assistant_text_excerpt`
- `assistant_reason_excerpt`
- `tool_use_summary`
- `tool_result_status_hint`
- `tool_result_excerpt_100`

字段建议含义：

- `assistant_text_excerpt`: 当前前序 step 的 assistant 文本截断版，用于表达当时显式对外动作或说明。
- `assistant_reason_excerpt`: 当前前序 step 中显式保留的 reasoning/think 截断版；如果原始数据没有显式保留，则为空。
- `tool_use_summary`: 工具调用摘要，至少包含 tool name 与少量关键参数；若参数体量很大，只保留参数名、类型/规模信息与短预览，不要求完整参数原文。
- `tool_result_status_hint`: 工具结果状态的弱提示字段，推荐仅做高精度识别，取值优先限制为 `error` / `success` / `unknown`。
- `tool_result_excerpt_100`: tool result 原文前 100 字截断，仅作辅助证据，不作为唯一摘要来源。

其中：

- `assistant_text_excerpt` 和 `assistant_reason_excerpt` 建议分别截断到 100 到 200 字。
- `tool_result_excerpt_100` 建议固定前 100 字，不再额外扩展。
- `tool_use_summary` 建议只保留 1 到 3 个最能表达意图的参数。
- 单个参数若是长文本、大数组、大对象、文件内容或 patch/script 原文，应统一截断，并保留规模信息，例如字符数、元素数、key 数。
- 对 `write_file`、`create_file`、`apply_patch` 一类工具，应优先保留 path、目标对象、操作类型、内容长度等高价值元信息，而不是正文全文。
- `tool_use_summary` 本身也应有总长度上限；超过上限时，优先删除低信息密度参数，而不是删掉 tool name、path、url、query 等核心参数。
- 若某个前序 step 没有 tool use / tool result，允许只保留 text/reason 字段。
- 不建议带入前序 tool result 原文全文。
- `tool_result_status_hint` 不表示“业务正确”或“结果对任务有用”，只表示程序从原始结果中抽出的弱执行状态线索。
- 无法可靠判断时，应默认回落为 `unknown`，而不是做激进推断。

关于 `tool_result_status_hint` 的使用约束：

- 推荐用正则或规则仅提取显式 error 信息，优先识别报错。
- 若结果中存在稳定、结构化的成功标志，可标记为 `success`。
- `success` 只表示“未见明显失败且有成功迹象”，不等于工具选对了、参数正确、结果有用或当前 step 已推进问题。
- 后续模型必须结合 `tool_result_excerpt_100` 与当前上下文自行判断，不能仅凭该 hint 下结论。

推荐的 prompt 背景模板示意：

```text
=== 当前单元之前的执行背景（仅供理解当前阶段） ===
Step -3:
- assistant_text_excerpt: ...
- assistant_reason_excerpt: ...
- tool_use_summary: search_web(query=..., site=...)
- tool_result_status_hint: success
- tool_result_excerpt_100: 返回 3 条候选文档...

Step -2:
- assistant_text_excerpt: ...
- assistant_reason_excerpt: ...
- tool_use_summary: fetch_webpage(url=...)
- tool_result_status_hint: error
- tool_result_excerpt_100: HTTP 404 Not Found...

Step -1:
- assistant_text_excerpt: ...
- assistant_reason_excerpt: ...
- tool_use_summary: write_file(path=/tmp/report.md, content=<text:8421 chars, prefix="# Report ...">)
- tool_result_status_hint: success
- tool_result_excerpt_100: File written successfully...
```

该背景块仅用于帮助模型理解“当前 step 处于哪一阶段”，不用于把前序步骤的功劳或失败直接转嫁到当前 step。

### 实施建议

若后续推进该方向，建议按以下顺序执行：

1. 先保持现有 response unit 边界不变。
2. 先重写 step 级 prompt 的目标语义与判据。
3. 再决定是否把数据模型、聚合字段、CLI/Web/导出字段名从 `response_helpful` 正式迁移为 `response_progress`。
4. 仅在规则化前序背景明显不够用时，再评估是否增加更重的上下文压缩机制。

## 对数据模型的影响

这是本方案最关键的工程影响。

因为两类 judgment 的粒度不再一致，推荐不要继续沿用一张统一的 turn_judgments 表强行承载两个指标。

推荐方案：拆成两类明细。

### 方案 A: 两张表，已确定

1. assistant_response_judgments
2. user_episode_judgments

建议字段：

```sql
CREATE TABLE samples (
  sample_uid TEXT PRIMARY KEY,
  raw_json JSON,
  user_query TEXT,
  assistant_response TEXT,
  empty_response BOOLEAN,
  num_turns INTEGER,
  expected_judgment_count INTEGER,
  num_tool_calls INTEGER,
  response_helpful_rate DOUBLE,
  response_unhelpful_rate DOUBLE,
  user_satisfied_rate DOUBLE,
  user_negative_feedback_rate DOUBLE,
  imported_at TIMESTAMP,
  tool_stats JSON,
  ...
);

CREATE TABLE assistant_response_judgments (
  judgment_uid TEXT PRIMARY KEY,
  sample_uid TEXT NOT NULL,
  response_index INTEGER NOT NULL,
  assistant_message_index INTEGER NOT NULL,
  feedback_kind TEXT NOT NULL,           -- tool_result | user
  feedback_message_start_index INTEGER,
  feedback_message_end_index INTEGER,
  feedback_payload JSON,
  response_helpful TEXT,                 -- yes/no/uncertain
  llm_error BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP,
  UNIQUE(sample_uid, response_index)
);

CREATE TABLE user_episode_judgments (
  judgment_uid TEXT PRIMARY KEY,
  sample_uid TEXT NOT NULL,
  episode_index INTEGER NOT NULL,
  start_user_message_index INTEGER NOT NULL,
  end_before_user_message_index INTEGER,
  signal_from_users JSON,
  user_satisfied TEXT,                   -- yes/no/uncertain/neutral
  llm_error BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP,
  UNIQUE(sample_uid, episode_index)
);
```

主键与联络点设计：

- sample_uid 直接升级为 samples 主键。
- 业务联络一律使用 sample_uid，不再让自增整数 id 作为三张表之间的主关联键。
- 两张 judgment 表建议同时具备：
  - 稳定主键 judgment_uid
  - 语义唯一键 `(sample_uid, response_index)` 或 `(sample_uid, episode_index)`
- judgment_uid 可以由 sample_uid 与轮次信息稳定拼接生成，例如：
  - `resp:{sample_uid}:{response_index}`
  - `episode:{sample_uid}:{episode_index}`

优点：

- 语义最清晰
- 统计与回溯最直接
- 不需要在同一行里塞两套不同边界的字段
- 顺手解决 samples 自增主键冲突与 judgment 关联不稳定的问题

### 方案 B: 一张表复用，不推荐

继续使用 turn_judgments，但增加 kind=response_helpful 或 user_satisfied，再让不同 kind 拥有不同 index 和 payload。

缺点：

- 查询和聚合复杂
- 语义不直观
- 更容易留下历史兼容债务

## 聚合建议

samples.tool_stats 建议改为分别基于两类 judgment 聚合：

```json
{
  "response_helpful": {
    "yes": 5,
    "no": 2,
    "uncertain": 1,
    "rate": 0.71
  },
  "user_satisfied": {
    "yes": 3,
    "no": 1,
    "neutral": 1,
    "uncertain": 0,
    "rate": 0.60
  },
  "assistant_response_count": 8,
  "user_episode_count": 5,
  "has_error": false
}
```

这样可以避免把两个不同分母硬塞成同一个 total_turns。

## 对现有代码的直接含义

需要被重新定义或拆分的部分至少包括：

- round_feedback.py 中的 TurnContextBuilder
- prompt 构造逻辑
- count_expected_judgments
- turn_judgments 存储模型
- samples.tool_stats 聚合逻辑
- README 与测试中的 judged turn 语义

## 全库影响范围

这次改造不是 round feedback 单文件重写，而是一次贯穿“派生字段 -> 存储 -> 聚合 -> 查询 -> 展示 -> 导出”的语义重构。

### 1. 核心模型与派生字段

- claw_data_filter/models/sample.py
  - 当前 count_expected_judgments 仍按单层 user-anchor 语义计数。
  - num_turns、expected_judgment_count 与旧 judged turn 模型强耦合。
  - 若改成双层语义，需要决定是拆出 expected_helpful_count 与 expected_satisfied_count，还是保留旧字段做兼容。
- claw_data_filter/models/round_judgment.py
  - 当前一条 RoundJudgment 同时容纳 response_helpful 和 user_satisfied。
  - 双层方案下，这个模型要么拆成两类 judgment，要么引入 judgment kind，但后者不推荐。

### 2. Round Feedback 处理主链路

- claw_data_filter/processors/round_feedback.py
  - TurnContextBuilder.extract_turns 当前把 user 后的 assistant/tool/assistant 合并成同一轮。
  - build_judgment_prompt、_extract_signal_users、_parse_response、process_sample 都建立在单层 turn 上。
  - ToolStatsAggregator 当前默认 helpful 与 satisfied 来自同一批 turn_judgments，total_turns 也是单分母。

### 3. DuckDB schema 与存储操作

- claw_data_filter/storage/duckdb_store.py
  - samples 表中的 num_turns、expected_judgment_count、response_helpful_rate、user_satisfied_rate、tool_stats 都与当前单层 judgment 绑定。
  - turn_judgments 表假定一条记录同时拥有 response_helpful 和 user_satisfied。
  - _refresh_tool_stats_from_turn_judgments 使用同一批 turn_judgments 回刷 helpful/satisfied 两套统计。
  - insert_turn_judgment、get_turn_judgments、replace_round_feedback_results、get_stats、filter_samples 都会受影响。
  - 如果拆表，还要处理序列、索引、迁移与历史兼容。

### 4. CLI 与脚本

- claw_data_filter/cli.py
  - stats 输出仍默认 helpful/satisfied 是同层聚合结果。
  - round-feedback 命令本身入口可能不变，但完成态统计与日志语义需要更新。
- scripts/run_import_to_stats.sh
  - 脚本参数不一定要变，但步骤说明和成功判定口径要更新。
- scripts/run_export.sh
  - 现有导出筛选仍基于 response_helpful_rate、user_satisfied_rate；字段名可以保留，但其来源会变化。

### 5. Web 展示与查询

- claw_data_filter/web/services/detail_builder.py
  - 当前按 judgments_by_turn 映射单个 turn_index，把 helpful 和 satisfied 填到同一 TurnDetailView。
  - 这是 Web 层最核心的旧语义耦合点。
- claw_data_filter/web/view_models/sample_detail_view.py
  - TurnDetailView 当前天然假定“一行 turn = helpful + satisfied + signal”。
  - 双层语义下，这个 view model 需要拆成 assistant-step 视图和 user-episode 视图，或设计复合结构。
- claw_data_filter/web/views/sample_detail.py
  - 当前详情页按单 turn 渲染 helpful / satisfied 彩色标签。
  - 双层后需要重做展示结构，否则只能继续把两种不同粒度硬绑在一行 UI 上。
- claw_data_filter/web/services/sample_query_service.py
  - get_table_preview 当前直接暴露 turn_judgments 表，且 samples 预览里的 num_turns、expected_judgment_count 都沿用旧语义。
- claw_data_filter/web/views/tables.py
  - 如果新增 judgment 表，表浏览页要同步暴露新表。
- claw_data_filter/web/views/filter.py
  - 筛选项字段名可延续，但页面文案和字段解释要更新，避免用户误解 rate 的分母。

### 6. 导出与导出格式

- claw_data_filter/exporters/unified_exporter.py
  - _build_openai_round_feedback_record 当前导出 round_feedback.turns，并假设 turn_index 能同时对应 helpful 和 satisfied。
  - _build_turn_ranges 仍按 user-anchor 分轮。
  - metadata 中的 num_turns、expected_judgment_count 也沿用旧语义。
  - 这是导出格式层最重要的耦合点，需要单独设计 v2 schema，而不是只改内部实现。
- claw_data_filter/exporters/report_exporter.py
  - 当前只是转抄 samples 聚合字段，但报告定义也要跟着改，尤其是 total_turns 相关口径。

## Web 设计方向

当前已确定采用双视图展示，而不是继续在同一条 turn 上混合两类 judgment。

### 视图 A: user_satisfied 视图

目标：尽量沿用当前详情页的阅读方式，因为它天然更接近 user episode。

建议结构：

- 每个 episode 为一个折叠块。
- 块内展示：起始 user、该 episode 内所有 assistant/tool 往返、后续 signal users。
- 顶部展示 user_satisfied 标签和 episode_index。

这部分可以复用当前 sample detail 页的很多排版习惯，只需要把粒度从 judged turn 改成 episode。

### 视图 B: response_helpful 视图

目标：围绕 assistant step 本身展示“当前输出 -> 紧邻反馈块 -> judgment”的局部归因。

建议结构：

- 每个 assistant response step 为一个卡片或折叠块。
- 卡片内明确展示：
  - 当前 assistant text / tool call
  - feedback kind: tool_result 或 user
  - feedback block 的消息范围
  - response_helpful 标签
- 若同一 user episode 内有多个 assistant step，应在视觉上标出它们属于同一 episode，但不要强行混成一条。

这会是一个新视图，不建议在现有 turn 视图上打补丁。

## 导出格式方向

导出格式应升级为新 schema，但尽量保留当前总体外形：

```json
{
  "schema": "openai_round_feedback_v2",
  "metadata": { ... },
  "source_metadata": { ... },
  "conversation": { ... },
  "round_feedback": {
    "response_helpful_steps": [...],
    "user_satisfied_episodes": [...]
  }
}
```

这样做的原则：

- 下游仍然可以沿用 metadata / conversation / round_feedback 这套顶层读取方式。
- 只把 round_feedback 内部从单一 turns 改成两类 judgments 数组。

### response_helpful 的标识方式

这是 v2 schema 里最关键的新点。

建议每条 response_helpful step 至少包含：

```json
{
  "response_index": 3,
  "assistant_message_index": 8,
  "episode_index": 1,
  "feedback_kind": "tool_result",
  "feedback_message_start_index": 9,
  "feedback_message_end_index": 10,
  "response_helpful": "yes",
  "llm_error": false
}
```

核心标识原则：

- 用 assistant_message_index 标明被评判的 assistant 起点。
- 用 response_index 提供样本内稳定顺序编号。
- 用 feedback_message_start_index / end_index 标明它对应的紧邻反馈块范围。

这样无论下游是按消息索引回放，还是按 response_index 做训练样本对齐，信息都足够明确。

### user_satisfied 的标识方式

建议每条 episode judgment 包含：

```json
{
  "episode_index": 1,
  "start_user_message_index": 4,
  "end_before_user_message_index": 11,
  "signal_from_users": ["好的"],
  "user_satisfied": "yes",
  "llm_error": false
}
```

## LLM 并发分配原则

双层级 judgment 落地后，LLM 调用会从“每个 judged turn 一次调用”变成“两类任务并行提交”。
这个变化如果处理不好，会出现两类问题：

- 资源被硬切分后，一侧队列空闲但另一侧排队，整体吞吐下降。
- response_helpful 任务数量通常明显多于 user_satisfied，若完全按提交顺序跑，episode judgment 可能长期饥饿。

因此并发策略需要明确约束。

### 原则 1: 使用全局共享并发池

不要为 response_helpful 和 user_satisfied 各自固定切一半 worker。

推荐做法：

- 整个 round feedback 进程只维护一个全局 `max_concurrency` 预算。
- 两类 judgment 任务都从同一个全局预算里领取执行槽位。
- 任一侧没有待处理任务时，空闲并发槽位应立即被另一侧复用。

这样可以保证 LLM 资源始终尽量跑满。

### 原则 2: 使用双队列 + 软配额，而不是硬切分

建议维护两条任务队列：

- `response_helpful_queue`
- `user_satisfied_queue`

调度时采用软配额：

- 默认不做 50/50 硬切分。
- 当两边都有积压时，给 user_satisfied 保留一个最小份额，避免被大量 helpful 任务淹没。
- 当某一侧队列变空时，另一侧可以借满全部剩余并发。

一个可行策略：

- `reserved_episode_slots = max(1, floor(max_concurrency * 0.2))`
- 剩余槽位全部开放给任意任务竞争

这不是固定比例，而是防饥饿的软底线。

### 原则 3: 调度目标是“高利用率 + 不饥饿”

设计目标不是让两类 judgment 数量上平均，而是：

- 全局并发槽位尽量一直被占满。
- user_satisfied 不会因为 response_helpful 数量更大而长期滞后。
- 每个 sample 的两类结果都能在可接受时间内汇合并原子写回。

### 原则 4: 写回粒度与调度粒度分离

建议把“任务执行”与“样本写回”分开：

- 调度层按单个 response step 或单个 user episode 发起 LLM 调用。
- 聚合层按 sample 收集结果。
- 当一个 sample 的两类 judgment 都完成后，再统一写回数据库。

这样可以最大化并发利用率，而不必为了等待某个 sample 的另一类 judgment 完成而占住执行槽位。

### 原则 5: 不建议按 sample 串行跑完两层再切到下一个 sample

反例：

- 先对 sample A 跑完全部 helpful
- 再跑完 sample A 的 satisfied
- 再处理 sample B

这种做法实现简单，但会显著降低全局调度灵活性，也不利于批量高并发。

更合理的做法是：

- 先把 batch 中所有 sample 拆成两类任务
- 统一进入调度器
- 调度器按全局预算执行
- 每个 sample 的结果在内存中按 sample_uid 汇总，待齐后原子写回

### 推荐调度模型

推荐一个简单且足够稳定的模型：

1. batch 内预先提取所有 assistant response contexts 和 user episode contexts。
2. 分别进入两条队列。
3. 使用一个全局 semaphore 控制总并发。
4. dispatcher 优先从“未达到软配额的一侧”取任务；若该侧为空，则立即从另一侧取任务。
5. 任一任务完成后立刻释放槽位并继续拉取下一个任务。
6. sample 级 aggregator 只负责收集结果并判断是否达到可写回条件。

### 与现有配置的关系

现有配置仍可保留：

- `MAX_CONCURRENCY`: 全局总预算
- `BATCH_SIZE`: 一次 claim 的 sample 数量

但语义要更新为：

- `MAX_CONCURRENCY` 不再代表“样本并发数”，而是“双层 judgment 任务的全局调用上限”。

如有必要，后续可以新增可选配置：

- `EPISODE_MIN_SHARE`
- `RESPONSE_MIN_SHARE`

但默认应优先采用自动软配额，不要让用户必须手工切并发比例。

### 7. 测试

- tests/test_round_feedback.py
  - 当前大量测试显式断言“assistant/tool/assistant messages are grouped into one judged turn”。
  - 这是本次重构里会变化最大的测试文件。
- tests/test_duckdb_store.py
  - schema、插入、查询、claim 后聚合回写测试都要改。
- tests/test_integration.py
  - 导入 -> round feedback -> 导出链路会受新格式和新聚合影响。
- tests/test_web_overview_service.py 以及其他 Web 相关测试
  - 尽管这里很多测试不直接断言 round feedback 细节，但展示统计和字段含义变了，仍需复核。

### 8. 文档与历史设计稿

- README.md
  - 已经开始区分当前实现与目标设计，但后续还要在实现落地后同步成最终语义。
- docs/superpowers/specs/2026-04-01-round-feedback-processor-design.md
- docs/superpowers/specs/2026-04-02-simplified-round-feedback-design.md
- docs/superpowers/plans/2026-04-01-round-feedback-processor-impl.md
- docs/superpowers/plans/2026-04-02-simplified-round-feedback-implementation.md
  - 这些历史文档都建立在单层 judged turn 模型上，至少要补“已过时”说明，避免与新设计并存时产生歧义。

## 当前识别出的隐藏耦合

- tool_stats 不只是存储缓存，它已经被 stats、filter、Web、report、export 共用，改它就等于改全链路契约。
- num_turns 和 expected_judgment_count 不只是导入侧字段，Web 表格预览、导出 metadata、样本详情页都直接展示它们。
- turn_judgments 不只是内部中间表，Web tables 页面和 OpenAI round feedback 导出格式都把它当成对外可观察结构。
- detail_builder 与 unified_exporter 都各自实现了一套“从消息重建 judged turn”的逻辑，这说明分轮语义目前并没有被单点封装，后续需要收口。
- 历史文档、README、测试当前是和旧实现一致的；一旦开始改实现，不同步清理这些文档，团队会很容易误判真实语义。

## 旧文档处理策略

此前的 round feedback 设计文档与实现文档建立在单层 judged turn 语义上，现已不再作为未来实现依据。

处理原则：

- 已完全过时且会造成误导的文档，直接删除。
- 保留的文档必须只保留仍然有效的背景，不得继续保留旧语义设计段落。

## 非目标

本设计文档不直接规定：

- 最终 prompt 文案细节
- 是否保留当前 turn_judgments 以兼容历史查询
- 历史数据一次性回填还是惰性重算

这些在实现计划中展开。

## 当前结论

结论是肯定的：从评判对象和信号归因上看，双层级体系比当前单层 judged turn 更合理。真正需要仔细设计的，不是语义是否成立，而是存储与聚合该如何拆分，避免新一轮语义和实现再次错位。