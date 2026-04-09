# 双层级 Round Feedback 实现计划

> 对应设计: docs/superpowers/specs/2026-04-09-dual-level-round-feedback-design.md
>
> 目标: 将 response_helpful 与 user_satisfied 的判定边界、存储和聚合彻底拆开，避免继续共用单层 judged turn。

## 实施原则

- 先固定语义，再改代码，不反过来迁就现有表结构。
- 优先让明细层表达准确，再考虑聚合字段兼容。
- 每一步都配测试，避免回到“README、测试、实现一致但语义不对”的状态。

## Phase -1: 清理工作区与建立基线

目标：在开始大重构前，把当前工作区整理成可回退、可对比、可审阅的基线状态。

- [ ] 先梳理当前未提交改动，确认哪些属于本轮 round feedback 设计准备，哪些属于之前已完成但尚未提交的修复。
- [ ] 以“当前稳定实现”为基线提交一次，至少包含已验证通过的存储修复、README 更新、脚本修复与测试修复。
- [ ] 在提交说明里明确：这一提交仍是单层 judged turn 语义，不包含双层级实现。
- [ ] 确保后续双层级重构可以在 Git 历史里和当前稳定版本清晰对比。

当前已知工作区涉及的改动类别：

- DuckDB sequence/tool_stats/session_merge_keep 相关修复
- import script 的 .gz -> .jsonl 约束说明
- README 的语义与操作说明更新
- tests/test_duckdb_store.py 与 tests/test_integration.py 的回归测试调整
- 新增双层级设计文档与实现计划

验收标准：

- [ ] 工作区没有含义不明的混杂改动
- [ ] 当前单层实现的稳定状态已形成可回退提交
- [ ] 后续双层改造可以按阶段单独提交

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

- [ ] 新增 AssistantResponseContextBuilder
- [ ] 新增 UserEpisodeContextBuilder
- [ ] 明确 assistant response index 与 user episode index 的生成规则
- [ ] 为两类 builder 分别补充边界测试

建议改动文件：

- [ ] claw_data_filter/processors/round_feedback.py
- [ ] tests/test_round_feedback.py
- [ ] tests/test_models.py 或新增 builder 专项测试

验收标准：

- [ ] 一个 assistant tool-call + tool result + final answer 的链条，能拆成 2 个 response_helpful 单元
- [ ] 同一链条仍只对应 1 个 user_satisfied episode

## Phase 2: 拆分数据模型与存储

目标：让存储层和语义一一对应。

- [ ] 将 samples 主键从整数 id 迁移为 sample_uid
- [ ] 移除三张表之间对整数 sample_id 的主关联依赖
- [ ] 新增 assistant_response_judgments 表模型
- [ ] 新增 user_episode_judgments 表模型
- [ ] 为两张表设计稳定 judgment_uid 生成规则
- [ ] DuckDB schema migration 或启动时建表补齐
- [ ] 为两张表各自增加 sample_uid 索引和唯一约束
- [ ] 删除或下线旧 turn_judgments 表，不保留长期兼容壳

建议改动文件：

- [ ] claw_data_filter/storage/duckdb_store.py
- [ ] claw_data_filter/models/round_judgment.py 或拆成两个 model 文件
- [ ] tests/test_duckdb_store.py

验收标准：

- [ ] 可以分别按 sample_uid 插入和查询两类 judgment
- [ ] judgment_uid 与 `(sample_uid, response_index|episode_index)` 保持稳定且幂等
- [ ] 原子写回时不会出现 sample 聚合结果与明细表不一致
- [ ] 导入时不再因本地自增样本主键冲突而失败

## Phase 3: 拆分判定与 Prompt

目标：为两类 judgment 分别构造输入和 prompt。

- [ ] response_helpful prompt 只看当前 assistant 响应单元 + 紧邻 feedback block
- [ ] user_satisfied prompt 只看当前 user episode + 后续最多 3 条 user 文本
- [ ] 移除当前“同一 prompt 同时产出两种语义不同字段”的耦合逻辑
- [ ] 失败重试和解析逻辑分别覆盖两类 prompt

建议改动文件：

- [ ] claw_data_filter/processors/round_feedback.py
- [ ] tests/test_round_feedback.py

验收标准：

- [ ] helpful 的 prompt 中不再带入后续 assistant 补救内容
- [ ] satisfied 的 prompt 中不再把 tool_result 当成反馈信号

## Phase 4: 重写聚合口径

目标：让 samples.tool_stats 与两类 judgment 的真实分母一致。

- [ ] tool_stats 分开记录 helpful 与 satisfied 的计数和 rate
- [ ] 删除或替换当前单一 total_turns 字段
- [ ] 增加 assistant_response_count 与 user_episode_count
- [ ] 更新 stats/filter/export 查询

建议改动文件：

- [ ] claw_data_filter/processors/round_feedback.py
- [ ] claw_data_filter/storage/duckdb_store.py
- [ ] claw_data_filter/filters/query.py
- [ ] claw_data_filter/exporters/report_exporter.py
- [ ] claw_data_filter/exporters/jsonl_exporter.py
- [ ] tests/test_integration.py

验收标准：

- [ ] response_helpful_rate 分母来自 assistant response judgments 的 yes/no
- [ ] user_satisfied_rate 分母来自 user episode judgments 的 yes/no/neutral

## Phase 4.5: 调整 Web 与导出契约

目标：把外部可见的数据结构同步到双层语义，避免存储改完但 UI/导出仍暴露旧模型。

- [ ] 重构 detail_builder，使其不再假定一个 turn 同时拥有 helpful 与 satisfied
- [ ] 为 user_satisfied 设计 episode 视图，尽量沿用当前详情页的阅读结构
- [ ] 为 response_helpful 设计新的 assistant-step 视图
- [ ] 重构 sample_detail_view 与 sample_detail 页面展示结构
- [ ] 更新 sample_query_service 与 tables 页面，暴露新的 judgment 明细表或兼容视图
- [ ] 设计并实现 OpenAI round feedback 导出 v2 schema
- [ ] 在 v2 schema 中为 response_helpful 增加 assistant_message_index 与 feedback block range 标识
- [ ] 更新 report_exporter 中对统计字段的解释与导出内容

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

- [ ] Web 详情页能够以双视图清晰区分 assistant-step judgment 与 user-episode judgment
- [ ] user_satisfied 视图可沿用当前 episode 式阅读体验
- [ ] response_helpful 视图能明确展示 assistant step 与其紧邻反馈块的对应关系
- [ ] 表格预览页可浏览新 judgment 明细
- [ ] 导出格式不再假定单一 turn_index 同时对应 helpful 和 satisfied

## Phase 5: 调整导入派生字段与兼容逻辑

目标：修正与旧单层 judged turn 耦合的派生统计。

- [ ] 重新定义 expected_judgment_count，或拆成 expected_helpful_count / expected_satisfied_count
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

- [ ] 补充最小示例测试，覆盖 assistant tool-call / tool result / final answer / user confirm
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

- [ ] 关键语义测试通过
- [ ] 历史样本可在可控时间内重算

## 风险点

- 粒度拆分后，历史依赖 turn_judgments 的页面和脚本会全部受影响。
- 若继续复用旧表结构，短期改动看似小，但长期会积累更多查询和维护复杂度。
- sample_uid 作为主键会放大一次性迁移成本，但这是把业务主身份和本地自增键彻底分开的必要代价。
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