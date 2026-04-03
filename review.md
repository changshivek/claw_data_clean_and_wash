# Claw Data Clean And Wash 综合 Review 与修复结论

审查日期: 2026-04-03

审查范围:
- 数据导入
- round feedback 处理
- 筛选与导出
- DuckDB 存储层
- Web 可视化页面与后端接口一致性

当前结论:
- 核心高风险问题已完成第一轮修复，主链路从“可运行但数据语义不稳”提升为“语义基本一致、支持重跑、具备显式处理状态”。
- 导入、round feedback、筛选导出、Web 页面已经统一到同一套消息提取、turn 分组和样本状态语义。
- 核心相关自动化测试当前通过 75 项。
- 仍存在少量中低风险问题，主要集中在异常恢复、Web 端到端测试覆盖和大数据量性能策略上。

---

## 1. 审查目标与方法

本次 review 不是单纯做代码风格检查，而是围绕以下问题展开:

1. 数据导入是否与真实输入格式一致。
2. round feedback 的处理完成定义是否正确，失败重跑是否可靠。
3. 筛选与导出是否存在逻辑漏洞或安全风险。
4. DuckDB 持久化与并行处理边界是否清晰。
5. Web 页面展示与后端真实语义是否一致。

审查方式:
- 阅读 CLI、models、importers、processors、filters、exporters、storage、web 页面代码。
- 交叉检查 README、测试文件与实现之间的一致性。
- 对发现的问题直接实施修复，并补充自动化测试验证。

---

## 2. 初始问题汇总

### 2.1 高风险问题

#### 2.1.1 导入契约与真实数据格式不一致

初始实现中，Sample.from_dict 只读取顶层 messages，但 README 明确说明支持 UniRouter 格式，并从 request.bodyJson.messages 提取消息。结果是:

- UniRouter 数据可以入库，但派生字段可能为空。
- user_query、assistant_response、num_turns、has_error、统计字段基础值可能错误。
- 后续 round feedback、筛选与导出建立在错误派生字段之上。

#### 2.1.2 round feedback 的“已处理”定义错误

初始实现中，只要 sample 已经有任意一条 turn_judgment，就会被判定为已处理。这会导致:

- 一条样本只处理了部分 turn 时，后续重跑无法补全。
- sample 级 tool_stats 和 turn_judgments 明细可能不一致。
- 失败恢复语义不可靠。

#### 2.1.3 round feedback 写入缺少原子边界

初始逻辑是先更新 sample.tool_stats，再逐条插入 turn_judgments。如果中间失败，会出现:

- 聚合结果已更新，但明细未写完。
- 同一样本重复重跑时可能带入旧脏数据。

#### 2.1.4 筛选导出链路存在 SQL 安全风险

初始筛选构造和导出存在拼接 SQL 片段的问题。虽然有部分正则验证，但仍然偏脆弱，尤其体现在:

- CLI 和 Web 侧都存在自行拼接 WHERE 条件的代码。
- 导出时对原始字符串 filter_query 的约束不足。

### 2.2 中风险问题

#### 2.2.1 turn 分组语义错误

初始 turn 提取按“每个 assistant 消息都算一轮”来做，导致代理场景下常见的:

- user -> assistant(tool call) -> tool -> assistant(final answer)

会被拆成多个轮次，其中还会出现空 user_message 的伪轮次。这直接影响:

- expected_judgment_count
- user_satisfied 的后续用户信号窗口
- Web detail 页的展示

#### 2.2.2 Web 页面和后端逻辑分叉

初始 Web 页面存在直接写 SQL、直接按固定 JSON 路径解析消息、手写 turn 配对逻辑的问题。风险在于:

- 页面看到的数据语义和后端 round feedback 语义不一致。
- 存储层变更后，Web 侧容易失配。

#### 2.2.3 存储层缺少统一查询接口

Web 页面依赖的 get_processed_count、filter_samples、get_sample_by_id、get_table_list、get_table_schema 等方法未在存储层集中实现，导致:

- 页面自行访问 conn 执行 SQL。
- 逻辑分散在 UI 层，难以维护与测试。

---

## 3. 已完成修复

### 3.1 数据导入与消息提取统一

已完成:
- 增加统一消息提取函数 extract_messages_from_payload。
- 同时支持顶层 messages 和 UniRouter 的 request.bodyJson.messages。
- Sample.from_dict 改为统一走同一入口。

修复效果:
- 导入阶段派生字段与真实 payload 对齐。
- Web、round feedback、统计逻辑都可以复用同一消息入口。

涉及文件:
- claw_data_filter/models/sample.py
- claw_data_filter/importers/jsonl_importer.py

### 3.2 expected_judgment_count 与 turn 分组语义修正

已完成:
- 新增 count_expected_judgments 逻辑。
- 将同一 user 下的 assistant/tool/assistant 序列合并为一个 judged turn。
- TurnContextBuilder.extract_turns 已按新语义改造。

修复效果:
- judged turn 数与真实代理交互结构更一致。
- 不再出现空 user_message 的伪轮次作为正常结果。
- sample_detail 页面与 round feedback 的 turn 语义对齐。

涉及文件:
- claw_data_filter/models/sample.py
- claw_data_filter/processors/round_feedback.py
- claw_data_filter/web/pages/sample_detail.py

### 3.3 round feedback 改为原子替换写入

已完成:
- 增加 replace_round_feedback_results。
- 对同一样本的 turn_judgments 采用先删旧结果再统一写入的事务方式。
- 完成后统一写 tool_stats、expected_judgment_count、processing_status。

修复效果:
- 避免 sample 聚合结果和 turn 明细不一致。
- 重跑可以覆盖旧脏数据。
- 样本级结果具备幂等替换语义。

涉及文件:
- claw_data_filter/storage/duckdb_store.py
- claw_data_filter/processors/round_feedback.py

### 3.4 显式处理状态机上线

已完成:
- samples 表增加 processing_status、processing_updated_at。
- 默认状态为 pending。
- 批处理 claim 时标记为 processing。
- 成功完成标记为 completed。
- 失败标记为 failed，并在 tool_stats 中记录错误原因。

修复效果:
- “是否已处理”不再通过猜测 turn_judgments 条数得出。
- pending、processing、completed、failed 状态语义明确。
- 后续支持恢复策略和可视化统计更容易扩展。

涉及文件:
- claw_data_filter/storage/duckdb_store.py
- claw_data_filter/cli.py
- claw_data_filter/processors/round_feedback.py

### 3.5 batch 领取逻辑改为 claim 模式

已完成:
- CLI round-feedback 不再直接 get_unprocessed_samples，而是 claim_unprocessed_samples。
- 领取时立即转成 processing，避免同一进程内重复领取同一批数据。

修复效果:
- 单进程批处理语义更稳定。
- pending 和 failed 样本可以被统一纳入下一轮处理。

涉及文件:
- claw_data_filter/cli.py
- claw_data_filter/storage/duckdb_store.py

### 3.6 筛选与导出链路参数化

已完成:
- FilterQueryBuilder 支持 build_parameterized_where_clause 和 get_parameterized_query。
- CLI filter 命令改为传递参数化 where 与 params。
- JSONLExporter 支持 filter_query + filter_params 组合。
- 仅在兼容旧字符串 filter_query 且无 params 时，才走基础危险模式校验。

修复效果:
- 主链路不再直接拼接用户输入值到 SQL。
- CLI 与 Web 导出共享更安全的查询构造方式。

涉及文件:
- claw_data_filter/filters/query.py
- claw_data_filter/exporters/jsonl_exporter.py
- claw_data_filter/cli.py
- claw_data_filter/web/pages/export.py

### 3.7 导出文件改为原子落盘

已完成:
- JSONLExporter 使用临时文件写入。
- 成功后使用 os.replace 原子替换目标文件。

修复效果:
- 中途失败不会留下“看起来存在但内容不完整”的结果文件。

涉及文件:
- claw_data_filter/exporters/jsonl_exporter.py

### 3.8 存储层补齐统一查询接口

已完成:
- 新增 get_processed_count。
- 新增 get_sample_by_id。
- 新增 filter_samples。
- 新增 get_table_list。
- 新增 get_table_schema。
- 新增内部 sample record 标准化构造。

修复效果:
- Web 页面不再依赖散落的手工 SQL 逻辑来完成核心查询。
- 后端语义可以集中维护。

涉及文件:
- claw_data_filter/storage/duckdb_store.py

### 3.9 Web 页面已同步到统一后端语义

已完成:
- filter 页面“导出选中”改为复用 JSONLExporter。
- export 页面改为参数化筛选条件构造。
- sample_detail 页面改为复用统一 turn builder，而不是手写消息配对。

修复效果:
- 页面展示与批处理逻辑不再分叉。
- 数据筛选、详情和导出的一致性提升。

涉及文件:
- claw_data_filter/web/pages/filter.py
- claw_data_filter/web/pages/export.py
- claw_data_filter/web/pages/sample_detail.py
- claw_data_filter/web/pages/overview.py
- claw_data_filter/web/pages/tables.py

### 3.10 清理未维护字段并引入稳定 sample_uid

已完成:
- 移除 samples.task_type。
- 移除旧的 samples.has_error 列，统一以 tool_stats.has_error 表示 round feedback 错误状态。
- 新增 sample_uid，并使用原始 payload 的 SHA-256 作为稳定导入身份。
- insert_sample 对相同 sample_uid 做幂等去重，避免重复导入生成多个逻辑重复样本。

修复效果:
- 清除了未维护字段和语义冲突字段。
- 样本主键不再只依赖本地自增序列的唯一性，导入身份更稳。
- 仍然保留整数 id 作为本地关系键，避免大范围破坏现有 URL 与 turn_judgments 外键语义。

涉及文件:
- claw_data_filter/models/sample.py
- claw_data_filter/storage/duckdb_store.py
- claw_data_filter/filters/query.py
- claw_data_filter/web/components/sample_table.py
- claw_data_filter/web/pages/filter.py
- claw_data_filter/web/pages/export.py
- claw_data_filter/web/pages/sample_detail.py

---

## 4. 当前状态评估

### 4.1 数据正确性

结论: 明显改善，核心问题已修复。

当前保证:
- UniRouter 导入与顶层 messages 导入统一。
- judged turn 数与真实代理轮次更接近。
- round feedback 重跑会替换旧结果，而不是叠加旧脏数据。

### 4.2 逻辑健壮性

结论: 从“推断式状态”提升为“显式状态”。

当前保证:
- 样本处理状态可观察。
- 批处理失败会进入 failed，而不是静默留在未知状态。
- 明确区分 pending、processing、completed、failed。

### 4.3 并行设计

结论: 单进程 asyncio 模型下已明显收紧，但还不是完全通用的分布式领取方案。

当前保证:
- LLM 调用受 semaphore 控制。
- sample 结果写入受 write_lock 保护。
- 单进程批处理不会重复 claim 同一批 pending 样本。

### 4.4 代码质量

结论: 结构明显更清晰，但仍有继续重构空间。

当前改善:
- Web 页面对存储层依赖更明确。
- 过滤逻辑集中到了 FilterQueryBuilder 与 DuckDBStore。
- turn 语义不再在多个地方重复实现。
- schema 中未维护字段已被移除，样本身份改为 sample_uid + 本地 id 双层设计。

---

## 5. 自动化验证结果

已补充并通过的重点测试包括:

- UniRouter payload 导入派生字段测试。
- parameterized filter clause 与 query 测试。
- exporter 参数化导出测试。
- exporter 危险 filter_query 拦截测试。
- round feedback 对 UniRouter 样本的完整处理测试。
- judged turn 计数与 extract_turns 一致性测试。
- 部分结果重跑与结果替换写入测试。
- claim_unprocessed_samples 状态流转测试。
- failed 状态落库测试。
- filter_samples 返回结构化结果测试。

当前测试结果:
- 75 passed

验证命令:

```bash
cd /kanas/nlp/liuchang/claw/claw_data_clean_and_wash
.venv/bin/pytest tests/test_duckdb_store.py tests/test_round_feedback.py tests/test_query_filter.py tests/test_exporters.py tests/test_models.py tests/test_jsonl_importer.py tests/test_integration.py -q
```

---

## 6. 仍然保留的风险与未完成项

### 6.1 processing 状态卡死恢复机制尚未实现

当前批处理在 claim 后会将样本标为 processing。如果进程在 claim 完成后、样本处理完成前直接崩溃，这些样本会停留在 processing。

影响:
- 后续批处理默认不会重新领取这些样本。

建议:
- 增加 processing 超时回收机制。
- 例如基于 processing_updated_at 实现 lease 过期重领。

### 6.2 多进程/多实例并发领取仍未完全验证

当前方案对单进程 asyncio 场景有效，但如果未来同时运行多个 round-feedback 进程或多个实例，DuckDB 的并发写入和 claim 事务语义仍需进一步验证。

建议:
- 明确运行模型是否只允许单实例。
- 如果需要多实例，需重新设计领取机制或改用更适合并发 claim 的存储。

### 6.3 Web 页面缺少自动化测试

虽然 Web 侧已切到统一后端语义，但当前没有针对 Streamlit 页面行为的自动化测试。

建议:
- 至少增加页面级 smoke test。
- 核心关注 detail 页面、导出页和筛选页的基本加载与参数联动。

### 6.4 Web 页面仍有体验级改进空间

当前剩余问题主要偏低风险:
- detail 页面直接访问无 sample_id 时仍比较简陋。
- 页面未加缓存策略。
- 长文本展示和大数据量交互还有优化空间。

---

## 7. 文件级结论

### 7.1 核心修复文件

- claw_data_filter/models/sample.py
    作用: 统一消息提取，修正 expected_judgment_count 语义。

- claw_data_filter/processors/round_feedback.py
    作用: 修正 turn 分组，串行化写入边界，失败样本显式标记。

- claw_data_filter/storage/duckdb_store.py
    作用: 增加显式状态机、统一查询接口、claim 逻辑和原子替换写入。

- claw_data_filter/filters/query.py
    作用: 增加参数化 where/query 构造能力。

- claw_data_filter/exporters/jsonl_exporter.py
    作用: 参数化导出、原子落盘。

- claw_data_filter/cli.py
    作用: filter 导出走参数化路径，round-feedback 改为 claim 模式。

### 7.2 同步修复文件

- claw_data_filter/web/pages/filter.py
- claw_data_filter/web/pages/export.py
- claw_data_filter/web/pages/sample_detail.py
- claw_data_filter/web/pages/overview.py
- claw_data_filter/web/pages/tables.py

### 7.3 测试补充文件

- tests/test_models.py
- tests/test_jsonl_importer.py
- tests/test_duckdb_store.py
- tests/test_query_filter.py
- tests/test_exporters.py
- tests/test_round_feedback.py

---

## 8. 最终结论

本次 review 的核心结论是:

1. 最初存在的主要问题集中在数据语义不一致、处理完成定义错误、重跑不可靠、SQL 查询不够安全，以及 Web 页面和后端逻辑分叉。
2. 这些问题中的主风险项已经完成修复，并形成了一套更一致的主链路: 导入 -> claim -> round feedback -> completed/failed -> 筛选/导出 -> Web 展示。
3. 当前系统已经从“功能基本可跑”提升到“主链路可维护、可验证、可重跑”的状态。
4. 后续最值得优先继续做的不是再加新功能，而是补上 processing 超时回收和 Web 页面 smoke test，这两项会显著提升真实运行稳定性。

综合评级:
- 修复完成度: 高
- 数据正确性: 中高
- 并发与恢复能力: 中
- 可维护性: 中高
- 剩余风险等级: 中

---

审查与修复结论人: GitHub Copilot
模型: GPT-5.4
结论日期: 2026-04-03