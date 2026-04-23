# Pipeline 修复计划

本文只保留当前仍需执行的修复计划。

已经完成的修复、manydata benchmark、正式 Docker 受控启动结论、以及为何不再继续扩展 route A，统一归档到 [docs/review_res.md](docs/review_res.md)。

## 本次目标

本次只推进一条路线：route B。

目标是一次性把 samples 主链路从“依赖 raw_json 运行时读取”切到“依赖结构化运行时字段”，并以新库重建完成切换。

完成后应满足：

1. samples 主表不再把 `raw_json` 作为运行时字段。
2. import 阶段直接落后续需要的结构化字段。
3. source locator 仅用于备查，不承担运行时主读取职责。
4. round feedback、session merge、Web detail、pipeline service、exporter、CLI、Web 导出入口全部切到结构化输入契约。
5. `raw_jsonl` 导出能力彻底移除。
6. 不保留新旧双写、新旧双读、旧库兼容层。
7. 以重新导入后的新库作为最终切换结果。

## 已知前提

这些内容不再作为计划项追踪，而是本次实施的固定前提：

1. route A 已被证明只能降压，不能根治导入内存问题。
2. manydata 正式 Docker 路径已经实证当前服务会继续爬升到 `256 GiB` 以上。
3. 导出功能已明确不再需要 `raw_jsonl` 原始副本导出。
4. 本轮允许通过新库重建完成切换，不要求保留旧库兼容。

## 目标契约

### samples 主表

需要保留的运行时输入分为两类。

第一类是结构化运行时字段：

- `normalized_messages_json`
- `normalized_tools_json`
- `normalized_user_turns_json`
- `source_metadata_json`
- 现有导入阶段已经生成的派生标量字段

第二类是 source locator：

- `items_path`
- `source_path`
- `line_number`
- `byte_offset`
- `source_fingerprint`

约束：

1. locator 只能用于审计、排障、追溯。
2. 任何运行时主链路都不能以 locator 回放原始 JSONL 作为默认输入。
3. `sample_uid` 必须在 import 时稳定生成，后续不再依赖 `raw_json` 回填。

### 运行链路输入

各模块应统一切到以下输入契约：

1. round feedback：`sample_uid + normalized_messages_json + normalized_tools_json + source_metadata_json(optional)`
2. session merge：`sample_uid + normalized_user_turns_json + message_count + num_turns`
3. Web detail：`sample_uid + normalized_messages_json + judgments + source_metadata_json`
4. pipeline service：只在编排层传递结构化字段，不再回读 `sample_uid, raw_json`

## 工作拆分

### 第一批：schema 与 import

目标：让 samples 主表和 importer 不再依赖 `raw_json`。

需要完成：

1. 从 sample/import 抽取层确定最终落库字段集合。
2. 修改 `samples` schema：新增结构化字段与 locator 字段，移除 `raw_json` 运行时依赖。
3. 重写 importer 行构造逻辑，只写结构化字段与 locator。
4. 删除基于 `raw_json` 的 `sample_uid` 兜底逻辑。
5. 调整 store 的插入、查询、claim、sample detail 读取接口。

涉及文件：

- [claw_data_filter/models/sample.py](claw_data_filter/models/sample.py)
- [claw_data_filter/importers/jsonl_importer.py](claw_data_filter/importers/jsonl_importer.py)
- [claw_data_filter/storage/duckdb_store.py](claw_data_filter/storage/duckdb_store.py)

### 第二批：主运行链路切换

目标：让所有主运行模块改为消费结构化字段。

需要完成：

1. 改 `DuckDBStore.claim_unprocessed_samples()` 返回结构化字段。
2. 改 `RoundFeedbackProcessor.process_sample()` 输入签名。
3. 改 pipeline service 的 round feedback 喂数逻辑。
4. 改 session merge candidate 读取逻辑。
5. 改 Web detail builder 的消息来源。

涉及文件：

- [claw_data_filter/processors/round_feedback.py](claw_data_filter/processors/round_feedback.py)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py)
- [claw_data_filter/session_merge.py](claw_data_filter/session_merge.py)
- [claw_data_filter/web/services/detail_builder.py](claw_data_filter/web/services/detail_builder.py)
- [claw_data_filter/storage/duckdb_store.py](claw_data_filter/storage/duckdb_store.py)

### 第三批：导出与入口收口

目标：彻底移除 `raw_jsonl` 暴露面。

需要完成：

1. 从 exporter 中删除 `RAW_JSONL` 支持。
2. 从 CLI 中删除 `raw_jsonl` 选项与相关分支。
3. 从 Web 过滤/导出界面中删除 `raw_jsonl` 选项。

涉及文件：

- [claw_data_filter/exporters/unified_exporter.py](claw_data_filter/exporters/unified_exporter.py)
- [claw_data_filter/cli.py](claw_data_filter/cli.py)
- [claw_data_filter/web/views/filter.py](claw_data_filter/web/views/filter.py)

### 第四批：测试重构

目标：让测试覆盖新契约，而不是继续覆盖已废弃的 `raw_json` 运行时语义。

保留并重构：

1. `tests/test_jsonl_importer.py`
2. `tests/test_duckdb_store.py`
3. `tests/test_round_feedback.py`
4. `tests/test_session_merge.py`
5. `tests/test_pipeline_service.py`
6. `tests/test_web_detail_builder.py`
7. `tests/test_cli.py`

删除或替换：

1. 所有把“samples 表中存在 raw_json”当作正确行为前提的断言。
2. 所有验证 `RAW_JSONL` / `raw_jsonl` 导出能力的测试。
3. 所有默认以 `json.loads(raw_json)` 驱动运行链路的测试。

新增测试簇：

1. 结构化导入契约测试。
2. round feedback / session merge / Web detail / pipeline service 的结构化输入契约测试。
3. 导出能力收口测试。
4. 新库重建测试。

### 第五批：验证与切换

目标：证明模式切换后不仅功能成立，而且内存收益成立。

必须执行：

1. 完整回归测试。
2. 真实 manydata 文件上的 import benchmark。
3. 正式 Docker 受控启动复验。
4. 用新库重建完成最终切换，确认不再需要旧库兼容逻辑。

## 验收标准

必须同时满足以下条件：

1. samples 主表不再存储完整 `raw_json` 作为运行时字段。
2. round feedback、session merge、Web detail、pipeline service 的主输入都不再依赖 `raw_json`。
3. CLI、Web、exporter 中不再存在 `raw_jsonl` 导出入口。
4. locator 已退化为备查信息，而不是主运行输入。
5. 回归测试已经按新契约完成重构。
6. manydata 导入 benchmark 给出结构化模式相对旧模式的对照结果。
7. 正式 Docker 受控复验确认运行时峰值显著低于当前实测。
8. 切换以新库重建完成，不再保留旧库兼容层。

以下情况都不算完成：

1. 主表不存 `raw_json`，但运行时仍普遍依赖 locator 回放原始 JSONL。
2. `raw_jsonl` 导出入口仍然保留。
3. 测试仍以旧 `raw_json` 语义作为正确行为前提。
4. 功能改动合入了，但没有给出内存对照结果。

## 实施顺序

1. 先做 schema 与 import。
2. 再切主运行链路。
3. 然后收口导出与入口。
4. 再完成测试重构。
5. 最后执行 manydata benchmark、正式 Docker 受控复验与新库重建切换。

## 建议提交拆分

1. `refactor: replace raw_json storage with structured sample runtime fields`
2. `refactor: switch round feedback and session merge to structured inputs`
3. `refactor: remove raw_jsonl export surfaces and raw_json runtime reads`
4. `test: rebuild regression coverage around structured sample inputs`
5. `docs: finalize route-b migration plan`
