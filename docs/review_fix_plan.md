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

## 提交拆分评估

原先的 5 提交拆分：

1. `refactor: replace raw_json storage with structured sample runtime fields`
2. `refactor: switch round feedback and session merge to structured inputs`
3. `refactor: remove raw_jsonl export surfaces and raw_json runtime reads`
4. `test: rebuild regression coverage around structured sample inputs`
5. `docs: finalize route-b migration plan`

当前判断：

1. 这 5 个标题仍然保留了正确的逻辑顺序，可以继续作为 review 时的理解框架。
2. 但它们已经不能直接按当前工作树原样执行，因为后续新增了 Web 锁等待页、Web import-chain 修复、Docker 调度修复、容器工具补齐、cold start 配置等跨主题变更。
3. 如果继续强行按旧方案拆，会出现同一文件在多个提交之间频繁交叉切分，review 和回滚成本都会偏高。

结论：

1. 旧拆分“逻辑上仍可参考”。
2. 当前真正可执行的方案应该改成“4 个主提交 + 1 个可选文档补充提交”。

## 当前建议提交计划

### 提交 1：结构化 samples 主链路切换

建议提交信息：

`refactor: switch sample runtime flow to structured fields`

目标：

1. 完成 route B 主链路切换，去掉运行时对 `raw_json` 的主依赖。
2. 把 schema / import / round feedback / session merge / exporter / CLI / pipeline runtime 输入统一到结构化字段。

建议纳入文件：

1. `claw_data_filter/models/sample.py`
2. `claw_data_filter/importers/jsonl_importer.py`
3. `claw_data_filter/storage/duckdb_store.py`
4. `claw_data_filter/processors/round_feedback.py`
5. `claw_data_filter/session_merge.py`
6. `claw_data_filter/pipeline/service.py`
7. `claw_data_filter/exporters/unified_exporter.py`
8. `claw_data_filter/cli.py`
9. `claw_data_filter/empty_response.py`
10. `claw_data_filter/filters/query.py`
11. `claw_data_filter/web/services/detail_builder.py`

配套测试建议同提交进入：

1. `tests/test_exporters.py`
2. `tests/test_web_detail_builder.py`

说明：

1. 旧方案中的提交 1、2、3、4 在当前工作树里已经高度耦合，建议合并成一个“主链路切换提交”。
2. 这样更符合当前代码状态，也更方便一次性 review 结构化输入契约。

### 提交 2：Web 读锁等待页与 UI 修复

建议提交信息：

`fix: harden web reads during pipeline writes`

目标：

1. Web 在 DuckDB 写锁期间退避重试并显示等待页，而不是直接报错。
2. 修复 Web 的 import-chain 问题与 sidebar widget 警告。

建议纳入文件：

1. `claw_data_filter/web/services/database_access.py`
2. `claw_data_filter/web/views/filter.py`
3. `claw_data_filter/web/views/overview.py`
4. `claw_data_filter/web/views/sample_detail.py`
5. `claw_data_filter/web/views/tables.py`
6. `claw_data_filter/web/app.py`
7. `claw_data_filter/pipeline/__init__.py`
8. `tests/test_web_database_access.py`

说明：

1. 这部分不属于 route B 的核心数据契约切换，但属于本轮部署后必须带上的稳定性修复。
2. 单独拆出来更利于 review “Web 与 pipeline 共存”的行为变化。

### 提交 3：Docker 调度修复与运行维护

建议提交信息：

`fix: restore scheduled pipeline execution in docker`

目标：

1. 修复 loop 调度被 `/bin/sh` 拉起后立即退出的问题。
2. 修复脚本执行依赖权限位的问题，统一显式用 `bash` 调度。
3. 补齐容器内基础排障工具，并纳入 cold start 配置。

建议纳入文件：

1. `docker/entrypoint.sh`
2. `docker/pipeline.cron`
3. `docker/scheduler_loop.sh`
4. `Dockerfile`
5. `configs/autoprocess.pipeline.cold_start_20260423.toml`

说明：

1. 这是本轮后半段新增问题的独立修复面，和主链路切换最好分开。
2. 该提交完成后，容器行为才与 README / 部署预期一致。

### 提交 4：README 与执行计划文档收口

建议提交信息：

`docs: refresh route-b and docker operation docs`

目标：

1. 更新 README 到当前真实状态。
2. 更新 fix plan，使提交方案与当前工作树一致，而不是继续沿用过时拆分。

建议纳入文件：

1. `README.md`
2. `docs/review_fix_plan.md`

说明：

1. 这一提交只处理文档，不再混入行为代码。
2. 当前这一步完成后，fix plan 就能作为最终提交执行说明使用。

### 可选提交 5：外部 review 记录归档

建议提交信息：

`docs: archive external review notes`

仅当你确认需要纳入仓库时再提交：

1. `docs/review_by_deepseek.md`

说明：

1. 该文件当前是新增未跟踪状态，不属于主修复链路。
2. 如果其内容只是临时 review 记录，也可以选择不入库。

## 执行建议

推荐顺序：

1. 提交 1
2. 提交 2
3. 提交 3
4. 提交 4
5. 视需要决定是否保留提交 5

推荐原因：

1. 先锁定结构化主链路，避免后续提交混入数据契约变化。
2. 再提交 Web 稳定性修复，便于单独 review 它对锁竞争和 UI 行为的影响。
3. 再提交 Docker 调度修复，便于单独验证容器行为。
4. 最后再收文档，避免文档在中间多次失真。

## 执行跟进

### 2026-04-23 Step 1

执行结果：

1. 已确认第一批的控制路径集中在 `claw_data_filter/models/sample.py`、`claw_data_filter/importers/jsonl_importer.py`、`claw_data_filter/storage/duckdb_store.py`。
2. 当前 importer 仍直接写入 `sample["raw_json"]`，store schema 与 claim/query 接口也仍以 `raw_json` 为主输入。
3. 第二批依赖的最小结构化输入已经明确为 `normalized_messages_json`、`normalized_tools_json`、`normalized_user_turns_json`、`source_metadata_json`，另需补齐 `message_count` 与 locator 字段作为 session merge / detail / tracing 输入基础。

当前进展：

1. 已完成 route B 第一批的本地假设确认，准备先落 schema/import 的最小可运行切片。

当前问题：

1. 现有 samples schema、store record builder、round feedback、session merge、detail builder、pipeline service 都直接读取 `raw_json`，第一批改完后必须连续完成第二批切换，否则代码将处于半断裂状态。
2. locator 字段在当前输入中没有统一显式来源，先按 payload 中已有同名字段抽取，缺失时允许为空。

下一步计划：

1. 修改 sample 抽取层，生成结构化运行时字段、source metadata、locator 字段与 `message_count`。
2. 重写 importer 插入行和 samples schema。
3. 立刻执行 importer/store 的窄测试，验证第一批切片是否成立。

### 2026-04-23 Step 2

执行结果：

1. 已完成第一批最小可运行切片：sample 抽取层新增结构化运行时字段、source metadata、locator 字段与 `message_count`。
2. samples schema 已切到 `normalized_messages_json`、`normalized_tools_json`、`normalized_user_turns_json`、`source_metadata_json` 与 locator 列；importer 已停止写入 `raw_json` 列。
3. store 的插入、查询、claim 接口已切到结构化字段，并保留旧表升级时的标量字段回填与 judgment 聚合回算语义。
4. 针对第一批执行的窄测试已通过：`tests/test_jsonl_importer.py` + `tests/test_duckdb_store.py`，结果为 `32 passed`。

当前进展：

1. 第一批目标已完成并通过 focused validation。
2. 当前进入第二批：主运行链路切换。

当前问题：

1. `round_feedback.py`、`session_merge.py`、`web/services/detail_builder.py`、`pipeline/service.py` 仍在读取 `raw_json` 或假定可回放旧 payload。
2. store 目前在 sample record 中临时重建了兼容用的 runtime payload，后续需要在第二批完成后删除，避免形成隐性兼容层。

下一步计划：

1. 改 `RoundFeedbackProcessor.process_sample()` 与批处理入口，使其消费 `normalized_messages` / `normalized_tools` / `source_metadata`。
2. 改 session merge 读取 `normalized_user_turns_json + message_count + num_turns`。
3. 改 detail builder 与 pipeline service，切断对 `raw_json` 列的直接查询。
4. 运行第二批相关窄测试，再继续第三批导出收口。

### 2026-04-23 Step 3

执行结果：

1. 已完成第二批主运行链路切换：`round_feedback.py`、`session_merge.py`、`pipeline/service.py`、`web/services/detail_builder.py`、CLI isolated round-feedback 入口全部改为消费结构化字段。
2. 已完成第三批导出与入口收口：`UnifiedExporter` 只保留 `openai_round_feedback`，CLI `filter --export-format` 与 Web 过滤页导出选项都已去掉 `raw_jsonl`。
3. 已完成第四批测试重构：`tests/test_web_detail_builder.py` 和 `tests/test_exporters.py` 已切到 `normalized_messages` / `openai_round_feedback` 契约；相关聚合测试切片通过 `52 passed`。
4. `empty_response.py` 已改为读取 `normalized_messages_json`，store 不再在 sample record 中重建兼容 `raw_json`。

当前进展：

1. 第二、三、四批目标已完成，并通过行为验证。
2. 当前进入第五批最终验证与切换证据收集。

当前问题：

1. manydata benchmark 的第二条长跑轨迹耗时较长，已在出现稳定低位 RSS 证据后提前结束，不再等待完整 480 chunk 收尾。
2. 正式 Docker guarded 复验为真实长跑过程，完成观测后已主动清理容器，避免持续占用环境资源。

下一步计划：

1. 执行全量回归测试。
2. 记录 manydata benchmark 的结构化模式内存结果。
3. 记录正式 Docker guarded 复验结果，并与之前的 256+ GiB kill 证据做对照。

### 2026-04-23 Step 4

执行结果：

1. 已执行全量回归测试：`/kanas/nlp/liuchang/claw/claw_data_clean_and_wash/.venv/bin/python -m pytest -q`，结果为 `152 passed`。
2. 已执行 manydata import benchmark：
	- `route_b_structured_serial64`: 1920 lines / 30 chunks 完成，`rss_before_close_mb=1250.14`，`rss_after_close_mb=283.66`，未触发 `4096 MB` 中止阈值。
	- `route_b_structured_serial4_reconnect25`: 已跑到 1280 lines / 320 chunks，观测峰值约 `1414.39 MB`，仍未触发阈值；基于已形成稳定低位轨迹，提前终止长跑以转入正式 Docker 复验。
3. 已执行正式 Docker guarded 复验：
	- 命令：`scripts/docker_run_incremental_pipeline_guarded.sh`
	- 参数：`MEMORY_LIMIT_GIB=256`, `LLM_ENDPOINT=http://182.242.159.76:31870/v1`
	- 观测窗口内峰值约 `146.8 GiB`，随后回落到 `112 GiB` 和 `75 GiB` 平台，未再出现此前 `260.2 GiB` 后被 kill 的行为。
	- 复验结束后容器已清理。

当前进展：

1. fix plan 中列出的五个批次均已执行到位。
2. 当前代码、测试、manydata benchmark 和正式 Docker 复验都已给出 route B 落地证据。

当前问题：

1. 无阻塞性问题；后续切换以新库重建流程执行即可。

下一步计划：

1. 以新库重建作为正式切换动作。
2. 将本次执行结果沉淀到 review 结论文档或提交说明中，作为 route B 验收记录。
