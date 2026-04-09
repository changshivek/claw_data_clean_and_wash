# 双层级 Round Feedback 实现计划

> 对应设计: docs/superpowers/specs/2026-04-09-dual-level-round-feedback-design.md
>
> 目标: 将 response_helpful 与 user_satisfied 的判定边界、存储和聚合彻底拆开，避免继续共用单层 judged turn。

## 当前执行状态

更新时间: 2026-04-09

- 已完成: 基线提交已建立，旧单层 round feedback 设计文档已清理，双层级设计与并发原则已补齐。
- 已完成: claw_data_filter/processors/round_feedback.py 已重建为双层处理器，并实现全局并发池、双队列软配额和按 sample_uid 原子写回。
- 已完成: claw_data_filter/storage/duckdb_store.py 已接入 assistant_response_judgments / user_episode_judgments 双表、schema migration、sample_uid 查询接口，并已移除 turn_judgments 兼容写回/回填壳。
- 已完成: claw_data_filter/exporters/unified_exporter.py 已切换到 openai_round_feedback_v2，直接导出双层 judgment 明细。
- 已完成: Web 详情页已切换为 user_satisfied episodes / response_helpful steps 双视图，detail_builder 与 sample_detail_view 不再把单个 turn 作为主展示结构。
- 已完成: sample_query_service 已支持 assistant_response_judgments / user_episode_judgments 明细预览，旧 turn_judgments 表预览入口已移除，Web 测试已替换为直接验证双层语义的测试。
- 已完成: report_exporter 已补齐双层统计摘要与语义说明；CLI stats、overview、filter、sample table 文案已明确区分 assistant steps 与 user episodes。
- 已完成: session merge 决策/写回、Web drill-down、DuckDBStore 写接口、导出 metadata 与测试主断言已统一切到 sample_uid-first / session_merge_representative_uid。
- 已完成: `RoundJudgment`、`RoundJudgmentProcessor`、`extract_turns`、`build_judgment_prompt` 与对应 legacy 测试已删除，当前只保留 response-step / user-episode 双层语义。
- 已完成: README 与 implementation 文档已同步到 sample_uid-first 和 openai_round_feedback_v2 口径，不再把 turn_judgments / sample_id drill-down 当作当前实现。
- 当前回归结果: 全量 pytest 已通过，116 passed。
- 下一步: 提交本轮 sample_uid-first 一次性收口改动；后续若继续推进，仅需要按独立主题做增量优化。

## 本次执行策略（单次收口）

本轮不再按多个 phase 分批落地，而是按一次性收口执行：

- 直接把 session merge、Web drill-down、存储写接口、导出 metadata 和测试主断言统一切到新键/新表体系。
- 只保留 `samples.id` 作为本地展示与排序辅助列，不再让任何业务流、跨表关系或 URL 路由依赖它。
- 在确认新键链路完整可运行后，立刻删除 `RoundJudgment`、`RoundJudgmentProcessor`、legacy turn helper 和对应旧测试，避免继续维护两套语义。
- 实施完成后，以“代码中不存在过时主键路径、测试不再验证过时语义、文档口径一致”为验收标准。

### 本次收口清单

- [x] session merge 决策与写回切到 `sample_uid` / `session_merge_representative_uid`
- [x] samples 读取模型与详情视图切到 `representative_uid` 主展示
- [x] Web 路由、列表详情跳转和 query params 切到 `sample_uid`
- [x] DuckDBStore 写接口改成 `sample_uid` 单一入口
- [x] 导出 metadata 明确为 `sample_uid-first`，`sample_id` 仅保留辅助定位信息或下线
- [x] 删除 `RoundJudgment`、`RoundJudgmentProcessor`、`extract_turns`、`build_judgment_prompt`、legacy 聚合分支
- [x] 重写或删除对应旧测试，只保留双层 response/episode 语义测试
- [x] 跑全量 pytest，确认结构清理后无残留回归

## 本轮推进结果

- 已把 session merge 全链路切到 `sample_uid` / `session_merge_representative_uid`，写回不再依赖整数样本主键。
- 已把 Web router、query params、列表 drill-down 和 detail 拉取统一切到 `sample_uid`，`samples.id` 仅保留为本地展示字段。
- 已把 DuckDBStore 的 round feedback 写接口统一为 `sample_uid` 单入口，测试里不再验证 `int | str` 兼容调用。
- 已把 `tests/test_round_feedback.py`、`tests/test_session_merge.py`、`tests/test_web_router.py` 等过时测试重写为双层语义版本，并同步更新 store/exporter/detail builder 断言。
- 已把 README 中关于评分边界、存储结构、导出 schema、Web detail drill-down 和主键职责的说明同步到当前实现状态。
- 已完成全量回归，当前测试基线为 `116 passed`。
- 已把 Web detail 主视图从单层 turn 渲染切换为双层结构：先看 episode satisfaction，再看 response-step helpfulness。
- 已补充 response_context / episode_context 直接测试，避免继续只靠兼容 turn 测试间接覆盖核心语义。
- 已替换 Web detail/query 里对旧 turn 语义的过时断言，使测试直接反馈当前实现而不是历史兼容层。
- 已把 report_exporter 从平铺旧 rate 字段升级为带 judgment_totals 和 semantics 的双层统计报告。
- 已把 CLI stats、overview、filter、sample table 文案改为显式说明 response steps / user episodes 语义，减少“turn”口径误导。
- 已移除 DuckDB 存储层的 turn_judgments 兼容写回、回填和查询接口，并将存储/导出/round feedback 相关测试改为直接验证双 judgment 表。

## 主键/表切换历史影响范围与综合方案

以下内容保留为本轮收口前的分析记录。对应影响项现已全部完成，不再代表当前代码状态。

### 目标口径

- `sample_uid` 作为业务主键、跨表关联键、Web drill-down 键和对外稳定标识。
- `samples.id` 保留为本地自增辅助列，仅用于导入顺序、人工排查和 UI 展示，不再承担跨模块语义。
- `assistant_response_judgments` / `user_episode_judgments` 继续以 `sample_uid` 为唯一关联键，不再回退到任何 `sample_id` 兼容路径。
- session merge 从“以整数样本 id 标记代表样本”切换为“以 sample_uid 标记代表样本”，避免主键迁移只完成一半。

### 当前尚未适配的影响范围

1. session merge 仍以 `sample_id` 为决策主键。
	- `claw_data_filter/session_merge.py` 中 `SessionMergeCandidate`、`SessionMergeDecision`、排序、去重和写回全部围绕整数 `sample_id` 组织。
	- `samples.session_merge_representative_id` 仍是 `INTEGER`，代表样本关系没有切到 `sample_uid`。

2. Web 路由和详情页仍以 `sample_id` 作为 drill-down 键。
	- `claw_data_filter/web/state/models.py` 的 `RouteState` 仍持有 `sample_id`。
	- `claw_data_filter/web/state/router.py` 仍通过 query param `sample_id` 导航详情页。
	- `claw_data_filter/web/views/sample_detail.py` 仍以 `get_sample_by_id()` 取样本，并把 `sample_id` / `representative_id` 作为主要展示字段。
	- `claw_data_filter/web/components/sample_table.py`、`web/views/filter.py`、`web/views/tables.py` 的详情回调签名仍是 `sample_id: int`。

3. 存储层仍保留 `id` 优先或 `int | str` 混合接口。
	- `DuckDBStore.replace_round_feedback_results()`、`update_sample_tool_stats()`、`mark_sample_processing_failed()` 等接口仍接受 `int | str`，继续允许调用方依赖旧路径。
	- `get_sample_by_id()` 仍被 Web、测试和部分流程广泛使用；`get_sample_by_uid()` 虽已存在，但尚未成为默认入口。

4. 兼容模型和兼容聚合分支仍存在。
	- `claw_data_filter/models/round_judgment.py` 里的 `RoundJudgment` 仍保留 `sample_id` 字段。
	- `claw_data_filter/processors/round_feedback.py` 里的 `RoundJudgmentProcessor` 和 `ToolStatsAggregator.aggregate(..., episode_judgments=None)` 仍是单层兼容入口。

5. 导出与外部元数据仍双写 `sample_id` / `sample_uid`。
	- `claw_data_filter/exporters/unified_exporter.py` 的 metadata 同时输出 `sample_id` 与 `sample_uid`。
	- 这本身不是错误，但意味着对外 contract 还没有完成“sample_uid-first”收口。

6. 测试仍有一批以 `sample_id` 为中心的断言。
	- `tests/test_session_merge.py` 仍以 `sample_id` / `representative_id` 作为核心预期。
	- `tests/test_web_router.py`、`tests/test_web_detail_builder.py`、`tests/test_round_feedback.py` 等仍保留不少 `sample_id` drill-down 断言。

### 关联判断

- 本轮已完成的双 judgment 表切换，与 session merge 没有直接表级耦合；session merge 不读 judgment 表。
- 但两者共享 `samples` 表上的主键口径、`session_merge_*` 标记以及 Web 详情入口，因此后续主键迁移不能只做 round feedback，不动 session merge。
- 结论：session merge 不是这轮表切换的 blocker，但它是下一轮 `sample_uid-first` 收口时必须一并处理的核心影响面。

### 推荐切换方案

#### Phase A: 先补齐 schema 和双写能力

- 在 `samples` 表新增 `session_merge_representative_uid TEXT`。
- 启动时为历史数据做一次 backfill：通过 `session_merge_representative_id -> samples.id -> samples.sample_uid` 映射填充新列。
- 在过渡期保留 `session_merge_representative_id INTEGER`，但新逻辑优先读写 `session_merge_representative_uid`。

验收标准：

- 旧库启动后可自动补齐 `session_merge_representative_uid`。
- 新写入不再依赖 `session_merge_representative_id` 才能读回代表样本。

#### Phase B: 重构 session merge 到 sample_uid-first

- 将 `SessionMergeCandidate` / `SessionMergeDecision` 的主标识从 `sample_id` 切换到 `sample_uid`。
- 数据扫描可继续带出 `id` 作为排序 tie-breaker，但不再作为业务 identity。
- 写回 `samples` 时改为 `WHERE sample_uid = ?`，代表样本改写到 `session_merge_representative_uid`。
- 若仍需 UI 展示代表样本整数 id，则在读取层通过 `representative_uid -> id` 映射按需补出，而不是把 `id` 存成关系键。

验收标准：

- session merge 的 dry-run 与正式写回在语义上保持一致。
- 同一批数据在迁移前后，keep/merged/skipped 结果不变。
- `session_merge_representative_uid` 成为唯一可信的代表样本关联字段。

#### Phase C: Web 路由与详情页切到 sample_uid

- `RouteState` 新增或替换为 `sample_uid` 字段，query param 从 `sample_id` 切到 `sample_uid`。
- filter/tables/sample_table 的详情回调统一改为 `on_detail(sample_uid: str)`。
- sample detail 页面改为 `store.get_sample_by_uid()` 取数，并将 `sample_uid` 作为主显示键；`sample_id` 降级为辅助展示。
- 详情页里的 session merge 展示改为优先显示 `representative_uid`，如需人工排查再附带代表样本 id。

验收标准：

- 从列表页进入详情页时不再依赖整数 id。
- 手工修改 URL 时，`sample_uid` 可以稳定定位同一条样本。
- Web 页面上 `sample_id` 不再承担任何导航或关联职责。

#### Phase D: 收缩存储接口与兼容模型

- 将 `DuckDBStore` 的对外写接口逐步收敛到 `sample_uid`，去掉 `int | str` 双态入口。
- 评估 `insert_sample()` 是否继续返回 `id`，还是增加 `insert_sample_and_get_uid()` / 返回完整 sample record；推荐先保留返回 `id` 以减少导入链路波动，但新流程不再依赖它进行后续关联。
- 删除 `RoundJudgment`、`RoundJudgmentProcessor` 和 `ToolStatsAggregator` 的 legacy 分支，彻底去掉单层模型的残留语义。

验收标准：

- 新代码路径中不再需要 `get_sample_by_id()` 才能完成业务流程。
- round feedback 处理链、Web、导出都不再依赖单层兼容模型。

#### Phase E: 导出 contract 与测试收口

- 明确 `sample_id` 在导出 metadata 中的定位：
  - 若只是排障辅助，则保留但标注 deprecated。
  - 若希望完全对外稳定，则在下一版 schema 中仅保留 `sample_uid`。
- 更新 `tests/test_session_merge.py`、`tests/test_web_router.py`、`tests/test_web_detail_builder.py` 等，把核心断言切到 `sample_uid` / `representative_uid`。
- README 和 implementation 文档同步说明：`id` 是本地辅助列，不是业务主键。

验收标准：

- 测试主断言不再把 `sample_id` 当成跨模块唯一键。
- 文档、Web、导出、存储对主键口径的描述一致。

### 推荐执行顺序

1. 先做 Phase A，为 session merge 迁移准备无损过渡列。
2. 然后做 Phase B，把 session merge 的真实关联键切换到 `sample_uid`。
3. 再做 Phase C，清掉 Web drill-down 对 `sample_id` 的依赖。
4. 最后做 Phase D 和 Phase E，收缩兼容接口、导出 contract 与测试。

### 关键风险与约束

- `samples.id` 目前仍承担稳定排序和人工排障作用，不建议在这轮迁移中删除，只应降级为非业务键。
- session merge 一旦改写代表样本字段，需要保证历史库自动 backfill，否则旧数据会出现详情页无法跳转或代表样本断链。
- Web 路由切换若不兼顾旧链接，历史书签和人工分享链接会失效；可考虑在过渡期同时兼容 `sample_id` 和 `sample_uid` 读取，但写回统一输出 `sample_uid`。
- `RoundJudgment` 兼容模型的清退要等相关测试和任何潜在外部脚本都切到双层语义后再做，避免一次性破坏排障工具。

## 实施原则

- 先固定语义，再改代码，不反过来迁就现有表结构。
- 优先让明细层表达准确，再考虑聚合字段兼容。
- 每一步都配测试，避免回到“README、测试、实现一致但语义不对”的状态。

## Phase -1: 清理工作区与建立基线

目标：在开始大重构前，把当前工作区整理成可回退、可对比、可审阅的基线状态。

- [x] 先梳理当前未提交改动，确认哪些属于本轮 round feedback 设计准备，哪些属于之前已完成但尚未提交的修复。
- [x] 以“当前稳定实现”为基线提交一次，至少包含已验证通过的存储修复、README 更新、脚本修复与测试修复。
- [x] 在提交说明里明确：这一提交仍是单层 judged turn 语义，不包含双层级实现。
- [x] 确保后续双层级重构可以在 Git 历史里和当前稳定版本清晰对比。

当前已知工作区涉及的改动类别：

- DuckDB sequence/tool_stats/session_merge_keep 相关修复
- import script 的 .gz -> .jsonl 约束说明
- README 的语义与操作说明更新
- tests/test_duckdb_store.py 与 tests/test_integration.py 的回归测试调整
- 新增双层级设计文档与实现计划

验收标准：

- [x] 工作区没有含义不明的混杂改动
- [x] 当前单层实现的稳定状态已形成可回退提交
- [x] 后续双层改造可以按阶段单独提交

## Phase 0: 先拍板的设计决策

- [x] 确认 response_helpful 是否把 assistant 中显式保留的 think/reasoning 文本纳入评判输入。
- [x] 确认连续 tool 消息是否合并成一个 feedback block。
- [x] 确认 user_satisfied 是否保留 neutral。
- [x] 确认存储层采用两张表，还是单表多 kind。
- [x] 确认 samples 主键切换为 sample_uid，并作为三张表的统一联络点。
- [x] 确认 Web 采用 response_helpful / user_satisfied 双视图展示。
- [x] 确认导出格式升级为新 schema，但保持 metadata + conversation + round_feedback 的基本结构。

推荐结论：

- think/reasoning: 仅在原始数据里显式存在且允许使用时纳入。
- 连续 tool 消息: 合并为一个 feedback block。
- user_satisfied: 保留 neutral。
- 存储层: 使用两张表。
- sample 主键: 使用 sample_uid。
- Web: 拆成两个视图。
- 导出: 设计 openai_round_feedback_v2。

## Phase 1: 拆分上下文构建器

目标：把当前单一 TurnContextBuilder 拆成两类 builder。

- [x] 新增 AssistantResponseContextBuilder
- [x] 新增 UserEpisodeContextBuilder
- [x] 明确 assistant response index 与 user episode index 的生成规则
- [x] 为两类 builder 分别补充边界测试

建议改动文件：

- [ ] claw_data_filter/processors/round_feedback.py
- [ ] tests/test_round_feedback.py
- [ ] tests/test_models.py 或新增 builder 专项测试

验收标准：

- [x] 一个 assistant tool-call + tool result + final answer 的链条，能拆成 2 个 response_helpful 单元
- [x] 同一链条仍只对应 1 个 user_satisfied episode

## Phase 2: 拆分数据模型与存储

目标：让存储层和语义一一对应。

- [x] 将 samples 主键从整数 id 迁移为 sample_uid
- [ ] 移除三张表之间对整数 sample_id 的主关联依赖
- [x] 新增 assistant_response_judgments 表模型
- [x] 新增 user_episode_judgments 表模型
- [x] 为两张表设计稳定 judgment_uid 生成规则
- [x] DuckDB schema migration 或启动时建表补齐
- [x] 为两张表各自增加 sample_uid 索引和唯一约束
- [x] 删除或下线旧 turn_judgments 表，不保留长期兼容壳

建议改动文件：

- [ ] claw_data_filter/storage/duckdb_store.py
- [ ] claw_data_filter/models/round_judgment.py 或拆成两个 model 文件
- [ ] tests/test_duckdb_store.py

验收标准：

- [x] 可以分别按 sample_uid 插入和查询两类 judgment
- [x] judgment_uid 与 `(sample_uid, response_index|episode_index)` 保持稳定且幂等
- [x] 原子写回时不会出现 sample 聚合结果与明细表不一致
- [ ] 导入时不再因本地自增样本主键冲突而失败

## Phase 3: 拆分判定与 Prompt

目标：为两类 judgment 分别构造输入和 prompt。

- [x] response_helpful prompt 只看当前 assistant 响应单元 + 紧邻 feedback block
- [x] user_satisfied prompt 只看当前 user episode + 后续最多 3 条 user 文本
- [x] 移除当前“同一 prompt 同时产出两种语义不同字段”的耦合逻辑
- [x] 失败重试和解析逻辑分别覆盖两类 prompt
- [x] 设计双层 judgment 的 LLM 调度器，确保共用全局并发池而不是静态切分并发
- [x] 为 user_satisfied 设置防饥饿的软配额策略，并允许空闲槽位被 response_helpful 借用

建议改动文件：

- [ ] claw_data_filter/processors/round_feedback.py
- [ ] tests/test_round_feedback.py

验收标准：

- [ ] helpful 的 prompt 中不再带入后续 assistant 补救内容
- [ ] satisfied 的 prompt 中不再把 tool_result 当成反馈信号
- [ ] 两类 judgment 同时存在时，LLM 槽位仍能保持高利用率
- [ ] user_satisfied 不会因 helpful 任务量更大而长期饥饿

### Phase 3.5: 并发与调度实现

目标：在双层 judgment 下维持当前吞吐能力，并尽量跑满 LLM 资源。

- [x] 将 `max_concurrency` 明确定义为全局 judgment 任务预算，而不是按样本或按单一层级计数
- [x] 实现 `response_helpful_queue` 与 `user_satisfied_queue` 双队列调度
- [x] 使用单个全局 semaphore 控制两类任务总并发
- [x] 增加软配额或最小保底份额，避免 episode judgment 饥饿
- [x] 允许空闲配额跨队列借用，避免资源碎片化
- [x] 将任务完成与 sample 原子写回解耦，按 sample_uid 聚合结果后统一落库
- [ ] 为并发调度补充压力测试和顺序一致性测试

建议改动文件：

- [ ] claw_data_filter/processors/round_feedback.py
- [ ] claw_data_filter/cli.py
- [ ] tests/test_round_feedback.py
- [ ] tests/test_integration.py

验收标准：

- [ ] 在同一 batch 中，两类 judgment 可共享全部 LLM 并发预算
- [ ] 任一侧队列为空时，另一侧可占用全部剩余槽位
- [ ] user_satisfied 在双队列积压时仍能持续推进
- [ ] 样本写回仍保持原子性，不出现一半 helpful、一半 satisfied 的持久化中间态

## Phase 4: 重写聚合口径

目标：让 samples.tool_stats 与两类 judgment 的真实分母一致。

- [x] tool_stats 分开记录 helpful 与 satisfied 的计数和 rate
- [ ] 删除或替换当前单一 total_turns 字段
- [x] 增加 assistant_response_count 与 user_episode_count
- [x] 更新 stats/filter/export 查询

建议改动文件：

- [ ] claw_data_filter/processors/round_feedback.py
- [ ] claw_data_filter/storage/duckdb_store.py
- [ ] claw_data_filter/filters/query.py
- [ ] claw_data_filter/exporters/report_exporter.py
- [ ] claw_data_filter/exporters/jsonl_exporter.py
- [ ] tests/test_integration.py

验收标准：

- [x] response_helpful_rate 分母来自 assistant response judgments 的 yes/no
- [x] user_satisfied_rate 分母来自 user episode judgments 的 yes/no/neutral

## Phase 4.5: 调整 Web 与导出契约

目标：把外部可见的数据结构同步到双层语义，避免存储改完但 UI/导出仍暴露旧模型。

- [x] 重构 detail_builder，使其不再假定一个 turn 同时拥有 helpful 与 satisfied
- [x] 为 user_satisfied 设计 episode 视图，尽量沿用当前详情页的阅读结构
- [x] 为 response_helpful 设计新的 assistant-step 视图
- [x] 重构 sample_detail_view 与 sample_detail 页面展示结构
- [x] 更新 sample_query_service 与 tables 页面，暴露新的 judgment 明细表或兼容视图
- [x] 设计并实现 OpenAI round feedback 导出 v2 schema
- [x] 在 v2 schema 中为 response_helpful 增加 assistant_message_index 与 feedback block range 标识
- [x] 更新 report_exporter 中对统计字段的解释与导出内容

建议改动文件：

- [ ] claw_data_filter/web/services/detail_builder.py
- [ ] claw_data_filter/web/view_models/sample_detail_view.py
- [ ] claw_data_filter/web/views/sample_detail.py
- [ ] claw_data_filter/web/services/sample_query_service.py
- [ ] claw_data_filter/web/views/tables.py
- [ ] claw_data_filter/web/views/filter.py
- [ ] claw_data_filter/exporters/unified_exporter.py
- [ ] claw_data_filter/exporters/report_exporter.py
- [ ] tests/test_web_overview_service.py 和其他 Web 相关测试

验收标准：

- [x] Web 详情页能够以双视图清晰区分 assistant-step judgment 与 user-episode judgment
- [x] user_satisfied 视图可沿用当前 episode 式阅读体验
- [x] response_helpful 视图能明确展示 assistant step 与其紧邻反馈块的对应关系
- [x] 表格预览页可浏览新 judgment 明细
- [x] 导出格式不再假定单一 turn_index 同时对应 helpful 和 satisfied

## Phase 5: 调整导入派生字段与兼容逻辑

目标：修正与旧单层 judged turn 耦合的派生统计。

- [x] 重新定义 expected_judgment_count，或拆成 expected_helpful_count / expected_satisfied_count
- [ ] 检查 num_turns 是否仍保留当前 user-anchor 语义，还是改为更中性的对话统计字段
- [ ] 检查所有 sample 查询与 Web 页面中对 sample_id 的引用，迁移到 sample_uid 或 UI 友好的展示方案
- [ ] 更新 CLI info/stats 输出解释

建议改动文件：

- [ ] claw_data_filter/models/sample.py
- [ ] claw_data_filter/cli.py
- [ ] README.md

验收标准：

- [ ] 新导入样本不会再假设 helpful 与 satisfied 数量恒相等

## Phase 6: 测试与回填策略

目标：保证迁移后结果可验证、可重跑。

- [x] 补充最小示例测试，覆盖 assistant tool-call / tool result / final answer / user confirm
- [ ] 补充“前一步错误、后一步补救”的归因测试
- [ ] 补充 user 新话题 -> neutral 的 satisfaction 测试
- [ ] 设计历史数据回填流程
- [ ] 评估是否需要一次性清空旧 judgment 并全量重跑 round-feedback
- [ ] 为历史设计文档补过时说明或链接到新方案
- [ ] 清理已过时的单层 round feedback 设计文档与实现文档

建议改动文件：

- [ ] tests/test_round_feedback.py
- [ ] tests/test_integration.py
- [ ] scripts/run_import_to_stats.sh 或独立 backfill 脚本
- [ ] README.md 与 docs/superpowers 下旧 round feedback 文档

验收标准：

- [x] 关键语义测试通过
- [ ] 历史样本可在可控时间内重算

## 风险点

- 粒度拆分后，历史依赖 turn_judgments 的页面和脚本会全部受影响。
- 若继续复用旧表结构，短期改动看似小，但长期会积累更多查询和维护复杂度。
- sample_uid 作为主键会放大一次性迁移成本，但这是把业务主身份和本地自增键彻底分开的必要代价。
- 双层 judgment 若错误地静态切分并发，会直接降低吞吐；若完全不做公平调度，又会造成 user_satisfied 饥饿。
- 如果 README 先改而实现未跟上，必须明确标注“目标设计”与“当前实现”的差异，避免误导使用者。

## 推荐推进顺序

1. 先确认 Phase 0 的边界决策。
2. 先做 builder 和测试，再动存储。
3. 存储层拍板后，再改 prompt 和聚合。
4. 最后再处理 CLI、README、历史数据回填。

## 预期产出

完成后应具备以下结果：

- response_helpful 真正按 assistant step 归因
- user_satisfied 真正按 user episode 归因
- 存储、统计、导出、README、测试全部与语义一致
- 不再出现“实现一致但设计不对”的情况