# Unisound 导出格式迁移考察与计划

更新时间：2026-04-20

当前状态：实现、测试、抽样验证和全量转换校验均已完成。本文保留设计推导、实施计划和执行记录，作为迁移过程文档。

## 1. 背景

当前仓库的稳定导出格式是 `raw_jsonl` 和 `openai_round_feedback`。目标是参考 `docs/data_format_unisound.txt` 中定义的数据标准，把现有导出数据转换成新的 Unisound 交付格式，并尽量保留已有信息，便于后续验证、分类和抽样。

本轮只做考察和计划，不修改导出实现。

## 2. 当前导出链路考察

### 2.1 现有导出入口

- CLI 和 Web 共用 `claw_data_filter.exporters.unified_exporter.UnifiedExporter`
- 当前只支持两种导出格式：`raw_jsonl`、`openai_round_feedback`
- `openai_round_feedback` 的顶层结构固定为：

```json
{
  "schema": "openai_round_feedback_v2",
  "metadata": {},
  "source_metadata": {},
  "conversation": {},
  "round_feedback": {}
}
```

### 2.2 conversation 的真实语义

- `conversation.messages` 已经是归一化后的 OpenAI 风格消息流
- 如果原始数据是 Anthropic 风格，请求级 `system` 会被转成前置 `system` message
- 原始工具定义会被归一化到 `conversation.tools`
- `assistant` 的 `tool_calls` 和 `tool` 结果消息都被保留下来

这意味着，目标格式中的 `system_prompt`、`tools`、`dialog`，原则上都可以从 `conversation` 进一步重组得到；而且 `system_prompt` 和 `tools` 应作为 Unisound 记录的顶级字段独立保留，不应混入 `dialog`。此外，`round_feedback` 已经具备可对齐的轮级判断结果，只是需要重建映射关系。

### 2.3 对真实导出文件的抽样结论

抽样文件：`data/exported_0415_all_except_user_unsatisfy_gt_2ep.jsonl`

已确认：

- 前 200 条记录顶层字段完全稳定，只有 `schema`、`metadata`、`source_metadata`、`conversation`、`round_feedback` 五个字段
- 前 500 条记录中：
  - 491 条首条消息就是 `system`
  - 491 条存在 `conversation.tools`
  - 466 条存在 `assistant.tool_calls`
- 前 200 条记录中，`source_metadata.metadata` 全部为 `null`

结论：

- `system_prompt`、`tools` 在当前样本里覆盖率很高，可以稳定映射
- `domain`、`task_describe`、`data_source` 这类“数据集级语义字段”当前不在导出结果里，不能靠现有导出直接恢复
- 当前导出是“单路对话 + judgment 侧挂”，不是偏好对比数据；默认不存在天然的 `Chosen` / `Rejected` 双候选答案

### 2.4 当前导出里已有但目标格式没有直接对应的位置

- `metadata`：样本处理状态、统计指标、merge 标记等
- `source_metadata`：请求来源、模型名、user-agent 等
- `round_feedback`：`response_progress_steps`、`user_satisfied_episodes`

根据 Unisound 文档“除必备字段外尽可能多保留原始信息”的要求，这些字段不应直接丢弃，建议以扩展字段保留；其中 `round_feedback` 还应进一步下沉到重建后的 Unisound 每轮结构中，形成轮级可追溯信息。

## 3. 与 Unisound 目标格式的字段差距

| 目标字段 | 当前可用来源 | 现状判断 | 建议方案 |
| --- | --- | --- | --- |
| `_id` / `id` | `raw_json._id`、`raw_json.id`、`metadata.sample_uid`、`metadata.local_sample_id` | 部分可得 | 优先使用原始 `_id` / `id`；若都没有，回退到 `sample_uid` 作为稳定 `id` |
| `domain` | 无稳定来源 | 缺失 | 必须由数据集级配置提供，不能从当前导出自动推断 |
| `task_describe` | 无稳定来源 | 缺失 | 必须由数据集级配置提供；英文 query 需要补 `-en` 后缀 |
| `data_source` | 无稳定来源 | 缺失 | 必须由数据集级配置提供 |
| `Chosen` | 无现成偏好字段 | 需补规则 | 第一阶段按单答案样本处理，固定为 `Assistant` |
| `Rejected` | 无现成偏好字段 | 需补规则 | 第一阶段与 `Chosen` 相同，固定为 `Assistant` |
| `system_prompt` | `conversation.messages[0]` 中的 `system` | 高覆盖可得 | 提取首个 `system` message 文本 |
| `tools` | `conversation.tools` | 高覆盖可得 | 将 OpenAI function tools 扁平化为顶级工具项，直接保留 `name`、`description`、`parameters` |
| `dialog` | `conversation.messages` | 可转换 | 需要按 Unisound 轮次定义重组，而不是直接按 user episode 切分 |

补充说明：

- 本次转换只考虑从 `openai_round_feedback_v2` 启动，不再覆盖 `raw_jsonl`
- `system_prompt` 和 `tools` 需要提升为 Unisound 顶级字段，与 `dialog` 分开存放
- 新的 Unisound 轮次需要保留对应的 `round_feedback` 结果，不能只保留样本级侧挂信息

建议的目标顶层结构如下：

```json
{
  "id": "...",
  "domain": "Agent",
  "task_describe": "internal_web_tool",
  "data_source": "multiturn_dialog_search_and_answer",
  "Chosen": "Assistant",
  "Rejected": "Assistant",
  "system_prompt": "...",
  "tools": [],
  "dialog": []
}
```

## 4. 关键转换难点

### 4.1 `domain` / `task_describe` / `data_source` 无法自动恢复

这三个字段属于数据集语义，而不是单条样本里的请求语义。当前导出记录里没有稳定字段可直接恢复它们，因此必须引入额外配置。

建议不要把这部分逻辑写成启发式猜测，而是显式传入，例如：

- `domain`
- `task_describe`
- `data_source`
- `default_answer_key`，默认 `Assistant`

### 4.2 当前数据不是偏好对比格式

Unisound 目标格式要求 `Chosen`、`Rejected` 指向最后一轮中的答案 key。但当前导出只有单个 assistant 执行链，没有“更好答案 / 较差答案”的天然成对候选。

因此第一阶段只能采用以下保守映射：

- `Chosen = "Assistant"`
- `Rejected = "Assistant"`
- 最后一轮同时写入 `Assistant`

这符合文档中“当只有一个答案时，Chosen 值 = Rejected 值”的要求。

### 4.3 `dialog` 需要按 Unisound 轮次定义重组

当前 `conversation.messages` 是线性消息流，而 `round_feedback` 的切分基础更接近 user episode。两者都不能直接作为 Unisound 的最终轮次。

根据补充要求，Unisound 轮次的构建原则应改为：

- 每轮以 `user` 或 `tool result` 作为起点
- 每轮以一个 assistant 响应单元为核心
- 同一个 user episode 内可能存在连续多个 assistant 执行链，因此会拆成多个 Unisound 轮次

可执行的重组规则建议如下：

1. 提取并移除前置 `system` message，写入顶级字段 `system_prompt`
2. 从剩余消息中识别“轮起点”：
   - `user` 文本消息
   - `tool` 结果消息
3. 从每个轮起点开始，向后收集，直到形成一个完整 assistant 响应单元：
   - assistant 文本
   - assistant `tool_calls`
4. 如果 assistant 后继续出现 `tool` 结果，并驱动下一次 assistant 响应，则将该 `tool` 结果视为下一轮的起点，而不是并入上一轮
5. 最后一轮额外补 `Chosen`、`Rejected`

这样可以正确处理下面这类结构：

- 同一个用户请求
- assistant 调工具 A
- tool result A
- assistant 调工具 B
- tool result B
- assistant 给出最终回答

在 user episode 视角下这是一轮；但在 Unisound 视角下，应拆成多个以 `tool result` 驱动的轮次。

这里有三个需要在实现前固定的约定：

- `Tool` 字段在轮次里保留结构化对象数组，不降级为字符串数组
- assistant 内容统一输出为 Unisound 风格对象，并在存在 `<think></think>` 时拆分为 `thought` / `answer` / `tool_calls`
- 单轮中如果起点是 `tool result`，不额外保留上游 user 文本作为上下文扩展字段，按线性消息流逐步组装即可

此外还需要明确一条边界：

- `dialog` 中不再重复承载顶级 `system_prompt` 和请求级 `tools`；`dialog` 只承载轮次级交互内容

### 4.4 重建后的 Unisound 轮次需要挂接 round feedback

当前 `round_feedback` 包含两类结果：

- `response_progress_steps`
- `user_satisfied_episodes`

但它们的索引空间不是 Unisound 轮次索引，而是：

- assistant response 单元索引
- user episode 索引

因此转换时必须建立一层映射：

1. 先重建 Unisound 轮次
2. 为每个 Unisound 轮次记录其对应的原始消息范围
3. 根据消息范围反查命中的 `response_progress_steps`
4. 根据所属 user episode 反查命中的 `user_satisfied_episodes`
5. 把命中的结果写入当前 Unisound 轮次的新字段

建议新增轮级扩展字段，例如：

```json
{
  "turn_id": 3,
  "User": "...",
  "Assistant": {"answer": "..."},
  "round_feedback": {
    "response_progress": {
      "response_index": 2,
      "episode_index": 0,
      "response_progress": "yes",
      "llm_error": false,
      "feedback_kind": "tool_result",
      "feedback_message_start_index": 5,
      "feedback_message_end_index": 6
    },
    "user_satisfied_episode": {
      "episode_index": 0,
      "user_satisfied": "uncertain",
      "llm_error": false,
      "message_start_index": 1,
      "message_end_index": 8
    }
  }
}
```

这里建议保留“轮级映射结果”和“样本级原始 `round_feedback` 扩展字段”两层信息：

- 轮级字段便于直接消费和抽检
- 样本级原始字段便于排查映射是否正确
- 对于同一个 `user_satisfied_episode` 命中多个 Unisound 轮次的情况，允许将同一结果复制挂接到这些相关轮次

### 4.5 `<think>` 内容的处理需要单独约定

抽样数据里 assistant 文本经常包含 `<think>...</think>`。而 Unisound 示例把 assistant 写成对象，并区分 `thought`、`answer`、`tool_calls`。

建议第一阶段采用以下规则：

- 如果 assistant 文本含 `<think>`，则尝试拆成：
  - `thought`
  - `answer`
- 若拆分失败，则原文保留到 `answer`
- `tool_calls` 直接沿用当前归一化后的 OpenAI 结构
- 即使某轮只有纯文本回复，也统一输出为 Unisound 风格对象，例如 `{ "answer": "..." }`
- assistant 对象字段集合先固定为 `thought`、`answer`、`tool_calls`

## 5. 建议先固定的输出骨架

为避免实现阶段反复返工，建议先明确以下结构边界：

- `system_prompt`：顶级字段，承载样本级系统提示；缺失时输出空字符串
- `tools`：顶级字段，承载样本级工具清单；若输入是 OpenAI function tools，则拆出 `function` 内部字段，直接保留 `name`、`description`、`parameters`
- `dialog`：只承载轮次级交互，不重复放置 `system_prompt` 和请求级 `tools`
- `dialog[n].Tool`：承载该轮的结构化工具结果对象数组，并沿用当前 `tool` message 的结构
- `dialog[n].round_feedback`：承载映射到该轮的反馈结果
- `ext.*`：承载样本级原始补充信息，便于排查和追溯

## 6. 推荐实现路径

建议分两阶段做。

### 阶段一：先做离线转换器

目标：把当前 `openai_round_feedback_v2` 导出文件转换成 Unisound 格式文件，不改现有主导出逻辑。

原因：

- 风险最低，不影响现有导出使用方
- 可以先验证字段映射是否满足标注/采样需求
- 更适合处理 `domain`、`task_describe`、`data_source` 这类需要外部配置的字段

建议输入：

- 输入文件：`openai_round_feedback_v2` JSONL
- 外部配置：一个数据集级配置文件，或脚本内配置区

建议输出：

- Unisound 标准 JSONL
- 可选输出一个校验报告，统计缺失字段、英文样本数量、单答案样本数量等

### 阶段二：验证稳定后再并入统一导出器

目标：在 `UnifiedExporter` 中新增第三种导出格式，例如 `unisound_jsonl`。

需要同步修改：

- `claw_data_filter/exporters/unified_exporter.py`
- CLI `--export-format`
- Web 导出格式下拉项
- 文档和测试

## 7. 第一阶段的具体实现计划

### 7.1 输入约束

第一阶段只支持从 `openai_round_feedback_v2` 转换，不直接从 `raw_jsonl` 转换。

原因：

- `openai_round_feedback_v2` 已经完成了 system、tools、messages 的归一化
- `round_feedback` 已经可用，便于直接挂接到重建后的 Unisound 轮次
- 转换逻辑更稳定，避免重复做格式识别

### 7.2 配置项设计

建议转换脚本必须支持两种配置方式，至少实现其中一种：

- 脚本顶部集中配置区
- 外部 JSON 或 YAML 配置文件

更推荐配置文件方式，原因是数据集级字段很可能按批次变化，不适合写死在代码里。

建议引入如下配置：

```json
{
  "domain": "Agent",
  "task_describe": "internal_web_tool",
  "data_source": "multiturn_dialog_search_and_answer",
  "default_answer_key": "Assistant",
  "id_strategy": "prefer_raw_id_then_sample_uid",
  "preserve_extensions": true,
  "task_describe_en_suffix": true,
  "turn_feedback_field": "round_feedback"
}
```

建议额外支持：

- 输出路径
- 是否保留样本级原始 `round_feedback`
- 是否保留样本级原始 `conversation`
- `<think>` 拆分策略
- 英文判定策略

### 7.3 转换器处理流程

建议新增一个独立转换模块，流程如下：

1. 读取一条 `openai_round_feedback_v2` 记录
2. 校验必需顶层字段是否存在
3. 解析 `conversation.messages`
4. 提取顶级 `system_prompt`
5. 转换 `conversation.tools` 到顶级 `tools`
6. 依据“以 user 或 tool result 为起点、以 assistant 为核心”的规则重建 Unisound `dialog`
7. 为每个重建轮次记录原始消息范围、assistant 响应索引、所属 user episode 索引
8. 将 `response_progress_steps` 和 `user_satisfied_episodes` 映射到对应 Unisound 轮次，并写入轮级 `round_feedback` 字段
9. 生成 `id`
10. 根据 query 语言决定 `task_describe` 是否追加 `-en`
11. 写入 `Chosen` / `Rejected`
12. 若开启扩展字段保留，则附带保留：
   - `ext.metadata`
   - `ext.source_metadata`
   - `ext.round_feedback`
   - `ext.conversation`

其中第 6 到第 8 步是本次转换的核心，也是与“按 user episode 直接导出”最大的区别。

### 7.4 测试计划

至少覆盖以下场景：

- OpenAI 风格对话转 Unisound
- Anthropic 风格对话转 Unisound
- 带 `system` 的样本
- 带 `tools` 和 `tool_calls` 的样本
- 同一个 user episode 内包含连续多个 assistant 执行链的样本
- 有 `tool` 结果消息驱动下一轮 assistant 的样本
- 重建后的 Unisound 轮次可正确映射 `response_progress_steps`
- 重建后的 Unisound 轮次可正确映射 `user_satisfied_episodes`
- 只有一个答案时 `Chosen == Rejected`
- 原始 `id` 缺失时回退到 `sample_uid`
- 英文 query 自动追加 `-en`
- 扩展字段保留开关
- 配置区模式与配置文件模式至少覆盖一种

另外建议增加两类结构断言：

- 顶级 `system_prompt` 和 `tools` 存在时，`dialog` 中不重复保留请求级同源信息
- 映射后的轮级 `round_feedback` 与样本级 `ext.round_feedback` 可交叉校验
- 轮次级 `Tool` 字段始终为结构化对象数组
- assistant 字段始终为 Unisound 风格对象，而不是裸字符串

## 8. 当前进展

已完成：

- 阅读 `docs/data_format_unisound.txt`
- 阅读当前导出文档 `docs/export-format.md`
- 阅读导出实现和归一化实现
- 抽样检查真实导出文件 `data/exported_0415_all_except_user_unsatisfy_gt_2ep.jsonl`
- 确认当前格式与目标格式的主要缺口

未开始：

- 离线转换器实现
- `dialog` 重组代码
- CLI / Web 的新增导出格式接入
- 自动化测试补充

## 9. 待确认问题

从当前文档内容看，整体方向已经基本正确，但下面这些点仍需要明确，否则实现时会出现口径漂移：

1. `round_feedback` 在轮次中的字段命名是否保持当前草案：`response_progress` 和 `user_satisfied_episode`。
2. `id` 字段最终优先级是否固定为：原始 `_id` > 原始 `id` > `sample_uid`。
3. 英文判定策略是否采用简单规则，还是依赖单独语言识别实现。

## 10. 当前建议

下一步优先做离线转换器原型，不直接修改现有导出器。原型实现应先解决两个核心问题：

- 按 Unisound 轮次定义重建 `dialog`
- 给每个重建轮次挂接正确的 `round_feedback`

在这两个问题跑通之后，再用真实导出文件验证 `Tool` 字段形态、`thought/answer` 拆分规则，以及配置区或配置文件的可维护性，最后再决定是否把它并入统一导出链路。

## 11. 实施计划

基于当前确认口径，实施阶段按以下顺序推进：

1. 定义输入 `openai_round_feedback_v2` 与输出 Unisound 的数据模型，用于转换前后校验
2. 在 `scripts` 目录实现转换配置、转换逻辑和文件级校验逻辑
3. 基于文档主需求补充测试代码和测试说明文档
4. 对 `data/exported_0415_all_except_user_unsatisfy_gt_2ep.jsonl` 抽样 10 条做全流程验证
5. 对完整导出文件执行全量转换，并对结果做模型校验

执行原则：

- 每一步开始前先更新文档中的计划或前置判断
- 每一步结束后记录进展、验证结果和遇到的问题
- 输入数据在进入转换前必须通过 `openai_round_feedback_v2` 数据模型校验
- 输出数据在写入交付文件前必须通过 Unisound 数据模型校验

## 12. 实施进展记录

### 2026-04-20 Step 0: 实现前检查

已完成：

- 确认依赖中已包含 `pydantic`，可直接用于输入输出数据模型校验
- 确认仓库当前测试框架为 `pytest`
- 确认脚本目录为 `scripts/`
- 确认本轮只使用 `data/exported_0415_all_except_user_unsatisfy_gt_2ep.jsonl` 作为测试和转换输入

发现的问题：

- 目标输入文件是 `openai_round_feedback_v2` 导出结果，不包含 `raw_json`，因此输出 `id` 不能再依赖 `raw_json._id/raw_json.id`；实现阶段需要基于当前可见字段重新定义回退策略
- 真实数据中存在“连续 assistant 且中间没有新的 user/tool”的情况；这会影响 Unisound 轮次构建和 response feedback 对齐，转换逻辑需要显式处理这种连续 assistant 场景

当前决定：

- 模型和转换脚本统一放在 `scripts/` 下
- 先实现严格校验，再实现转换，避免带病输入直接进入批量转换

### 2026-04-20 Step 1: 模型与转换脚本骨架

已完成：

- 在 `scripts/unisound_export_models.py` 中新增输入 `openai_round_feedback_v2` 和输出 Unisound 的 Pydantic 数据模型
- 在 `scripts/unisound_export.py` 中实现输入校验、配置加载、单条转换、文件级转换和输出校验逻辑
- 明确转换入口只接受已通过输入模型校验的数据
- 明确交付输出必须逐条通过 Unisound 数据模型校验

当前实现口径：

- 顶级 `system_prompt`、`tools` 与 `dialog` 分离
- `Tool` 在轮次中保留结构化对象数组，沿用当前 `tool` message 结构
- assistant 统一输出为对象，并支持 `<think></think>` 拆分
- `user_satisfied_episode` 允许复制挂接到同一 episode 对应的多个 Unisound 轮次

发现的问题：

- `openai_round_feedback_v2` 导出结果中没有 `raw_json`，因此当前实现的 `id` 回退策略暂时基于 `source_metadata.metadata -> sample_uid -> local_sample_id`
- 连续 assistant 场景下，当前转换实现会复用最近一次线性锚点来构建新轮次；该行为需要通过测试和抽样验证进一步确认是否满足预期

### 2026-04-20 Step 2: 单测与显式校验入口

已完成：

- 新增 `tests/test_unisound_export.py`，覆盖顶级字段分离、`<think>` 拆分、`Tool` 结构保留、`round_feedback` 映射、连续 assistant 处理、输入输出文件级校验
- 新增 `validate-output` 显式校验入口，支持对写出的 Unisound JSONL 再次逐条做模型校验
- 当前新增测试已在本地通过

验证结果：

- `pytest tests/test_unisound_export.py` 通过，当前结果为 `3 passed`

当前状态：

- 输入模型校验可单独执行
- 输出模型校验可在转换前后显式执行
- 具备进入真实数据抽样验证的条件

### 2026-04-20 Step 3: 10 条抽样全流程验证

已完成：

- 对 `data/exported_0415_all_except_user_unsatisfy_gt_2ep.jsonl` 前 10 条执行输入校验
- 使用 `scripts/unisound_export_config.exported_0415.json` 完成 10 条样本转换
- 对输出文件再次执行显式输出模型校验

产物：

- `data/exported_0415_all_except_user_unsatisfy_gt_2ep.sample10.unisound.jsonl`
- `data/exported_0415_all_except_user_unsatisfy_gt_2ep.sample10.unisound.report.json`

验证结果：

- 输入校验通过，10 条记录均符合 `openai_round_feedback_v2` 输入模型
- 输出校验通过，10 条记录均符合 Unisound 输出模型
- 抽样报告显示当前 10 条样本均被识别为英文任务，因此 `task_describe` 追加了 `-en`

观察：

- 某些样本的 `system_prompt` 很长，这是当前源数据中系统提示本身较长导致的，不是转换重复拼接造成的
- 顶级字段结构、轮次级 `round_feedback` 和 `Tool` 结构都已按预期落盘

### 2026-04-20 Step 4: 全量验证前兼容修复

全量输入校验第一次执行时发现一个真实数据兼容问题：

- 第 514 条记录出现了 `developer` 角色消息，当前输入模型只允许 `system/user/assistant/tool`

已处理：

- 输入模型已扩展支持 `developer` 角色
- 转换逻辑已将 `developer` 内容与 `system` 一样并入顶级 `system_prompt`
- 已补充对应单测，当前测试结果更新为 `4 passed`

结论：

- 抽样 10 条未覆盖到该问题
- 全量验证对于发现真实边界条件是必要的，不能仅依赖抽样样本

### 2026-04-20 Step 5: 全量转换与交付校验

已完成：

- 对 `data/exported_0415_all_except_user_unsatisfy_gt_2ep.jsonl` 全量 1957 条执行输入模型校验
- 完成全量 Unisound 转换
- 对全量输出文件再次执行显式输出模型校验

产物：

- `data/exported_0415_all_except_user_unsatisfy_gt_2ep.unisound.jsonl`
- `data/exported_0415_all_except_user_unsatisfy_gt_2ep.unisound.report.json`

结果：

- 输入校验通过：`validated_records = 1957`
- 输出校验通过：`validated_records = 1957`
- 全量转换报告：`count = 1957`，`english_count = 1742`

观察：

- 最终交付文件体积较大，约 1.52 GB，主要原因是当前按计划保留了 `ext.round_feedback` 和 `ext.conversation` 等扩展信息
- 如果后续需要更轻量的交付版本，可以在不改变主转换逻辑的前提下，通过配置裁剪 `ext.*` 字段

### 2026-04-21 Step 6: 顶级 tools 结构调整

根据 Unisound 字段要求，顶级 `tools` 不再保留 OpenAI 风格的外层包装：

- 旧结构：`{"type": "function", "function": {"name": ..., "description": ..., "parameters": ...}}`
- 新结构：`{"name": ..., "description": ..., "parameters": ...}`

已完成：

- 在转换逻辑中新增顶级 `tools` 扁平化处理
- 保留 `function` 内部的 `name`、`description`、`parameters`
- 保持非 function 工具项原样透传
- 已补充并通过测试覆盖

当前状态：

- sample10 产物已重建并确认顶级 `tools` 不再含 `type/function` 包装
- 全量产物需要按新结构再重建一次