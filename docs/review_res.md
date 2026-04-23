# Claw Data Clean And Wash 综合 Review 与修复结论

审查日期: 2026-04-22

审查范围:
- 数据导入
- round feedback 处理
- 筛选与导出
- DuckDB 存储层
- Web 可视化页面与后端接口一致性
- pipeline / Docker / manydata 正式部署链路
- 导入与 feedback 阶段的内存行为

当前结论:
- 核心高风险问题已完成第一轮修复，主链路从“可运行但数据语义不稳”提升为“语义基本一致、支持重跑、具备显式处理状态”。
- 导入、round feedback、筛选导出、Web 页面已经统一到同一套消息提取、turn 分组和样本状态语义。
- 核心相关自动化测试当前通过 75 项。
- 从内存管理视角补充复审后，确认导入阶段旧并行路径曾具备触发宿主机多 GB 级内存峰值的现实可能；当前正式容器已通过强制串行导入绕开这一高风险路径。
- round feedback 阶段未发现典型的无界泄漏，但存在与 batch 大小、单样本上下文数量成比例的峰值占用，需要保持当前保守配置。
- 仍存在少量中低风险问题，主要集中在异常恢复、Web 端到端测试覆盖和大数据量性能策略上。

---

## 1. 审查目标与方法

本次 review 不是单纯做代码风格检查，而是围绕以下问题展开:

1. 数据导入是否与真实输入格式一致。
2. round feedback 的处理完成定义是否正确，失败重跑是否可靠。
3. 筛选与导出是否存在逻辑漏洞或安全风险。
4. DuckDB 持久化与并行处理边界是否清晰。
5. Web 页面展示与后端真实语义是否一致。
6. Docker / pipeline 正式部署在冷启动、长时运行和 manydata 场景下是否稳定。
7. 导入与 feedback 阶段是否存在不可接受的内存压力。

审查方式:
- 阅读 CLI、models、importers、processors、filters、exporters、storage、web 页面代码。
- 交叉检查 README、测试文件与实现之间的一致性。
- 对发现的问题直接实施修复，并补充自动化测试验证。
- 结合真实 manydata 文件做导入与内存实测。

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
- 仍然保留整数 id 作为本地关系键，避免大范围破坏现有 URL 与 judgments 关联语义。

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

## 6. 内存专项复审补充结论（2026-04-22）

本轮补充 review 重点回答的问题是：manydata 真实容器在导入阶段出现的“宿主机内存爆炸并卡死”，是否可能由本服务自身造成。

结论先行:

1. 不能排除，而且在旧的并行导入配置下，这种可能性成立。
2. 高风险点主要在导入阶段的 `_import_parallel_batched`，不是 DuckDB 事务本身，也不是典型的 Python 对象泄漏。
3. round feedback 阶段当前配置下存在可观但受控的峰值内存占用，未发现与导入阶段同量级的失控风险。
4. 当前正式部署已通过 `CLAW_IMPORT_FORCE_SERIAL=1` 强制切回串行导入，已经移除了最主要的内存爆炸路径。

### 6.1 导入阶段内存审查

审查对象:
- claw_data_filter/importers/jsonl_importer.py
- claw_data_filter/storage/duckdb_store.py

关键实现事实:

1. 串行导入路径 `_import_serial_batched` 按 `chunk_size` 逐块读取 JSONL 行。
2. 每个 chunk 在当前进程内完成 `json.loads`、`Sample.from_dict` 和 DuckDB 批量写入后即进入下一块，没有跨 chunk 的全量缓存。
3. 并行导入路径 `_import_parallel_batched` 会把 chunk 提交给 `ProcessPoolExecutor`，并维护 `max_pending = max(2, workers * 2)` 个待处理 future。
4. 正式 manydata 配置原始值是 `workers = 8`、`chunk_size = 64`。

这意味着旧并行路径的峰值内存不是由单个 chunk 决定，而是由以下几部分叠加决定：

- pending chunk 的原始字符串
- 子进程中的解析结果
- 父进程等待写入 DuckDB 的 rows
- DuckDB 和 Python 运行时额外开销

### 6.2 基于真实 manydata 数据的量级估算

对 manydata 解压后的 `*_items.jsonl` 做真实采样，单条 JSONL 行长度统计如下:

- p50: 454940 字符
- p95: 1627342 字符
- max: 1788839 字符

在旧配置 `workers = 8`、`chunk_size = 64` 下:

1. 单个 chunk 仅原始行字符串的体量约为:
   - p50: `64 * 454940 ≈ 29 MB`
   - p95: `64 * 1627342 ≈ 104 MB`
   - max: `64 * 1788839 ≈ 114 MB`
2. 并行路径的 `max_pending = 16`，仅父进程待处理 chunk 原始字符串的体量约为:
   - p50: `16 * 29 MB ≈ 464 MB`
   - p95: `16 * 104 MB ≈ 1.66 GB`
   - max: `16 * 114 MB ≈ 1.82 GB`

而这还没有计入:

- 子进程各自持有的 chunk 数据
- `json.loads` 后的 Python dict / list 对象膨胀
- `Sample.from_dict` 和 `json.dumps(sample.raw_json)` 形成的二次对象复制
- DuckDB 插入前 `rows` 列表在父进程中的暂存

因此，从内存管理角度，这条旧并行导入路径完全有能力把宿主机推到多 GB 级别 RSS。也就是说，之前正式容器在导入阶段引发服务器内存爆炸，不能简单归咎为“外部其他进程”，本服务旧导入路径本身就具备足够的放大能力。

### 6.3 当前串行导入路径的风险重新评估

当前正式容器已经通过环境变量 `CLAW_IMPORT_FORCE_SERIAL=1` 将导入强制降为单 worker。

这会把导入峰值从“多个 chunk 与多个子进程并存”降为“单 chunk 在单进程内流动”。在相同 `chunk_size = 64` 下，串行路径的主要驻留对象只剩:

- 当前 chunk 的原始行字符串
- 当前 chunk 解析出的 Python 对象
- 当前 chunk 对应的 DuckDB 写入参数列表

按真实 manydata 数据量级估算，单 chunk 原始字符串体量大约是 29 MB 到 114 MB。考虑到 Python 解析对象和插入参数会进一步膨胀，串行路径的瞬时峰值仍可能到达数百 MB，但已经不再具备旧并行路径那种轻易冲到 1 到 3 GB 以上的结构性放大器。

因此，当前正式容器如果仍出现整机级内存爆炸，导入器已不再是首要怀疑对象；但在旧并行配置下，导入器确实是高概率原因。

进一步针对 route A 第二阶段做了真实 manydata 复验：

- 导入器热路径已从“每行构造完整 `Sample.from_dict` Pydantic 对象”改成“直接从 payload 提取入库字段”。
- 复验基准仍使用同一 full `items.jsonl`，并保持 `chunk_size = 4`、`reconnect_every_chunks = 25`。
- 两次 post-opt run 的共同结论是：
  - 处理 1920 行后，`rss_before_close_mb` 约为 `2938.30` 到 `3033.02`
  - 关闭连接后，`rss_after_close_mb` 可回落到 `468.75` 到 `575.10`
  - 中途 checkpoint 波动仍然较大，其中一次 run 在 1280 行观测到 `3775.84 MB`

这说明：

- 去掉 Pydantic 热路径是有效的，说明 Python 中间对象构造确实是内存放大的组成部分之一。
- 但这不是主导项，因为在 route A 下保留 `raw_json` 写入主表后，close 前 RSS 仍会停留在接近 `3 GiB` 的量级。
- 因此，第二阶段优化只能被定义为“继续降压”，不能被定义为“已经根治 manydata 导入内存问题”。

另外，本轮还做了正式 Docker 路径的受控启动验证：

- 启动方式不是手工裸跑，而是通过 `scripts/docker_run_incremental_pipeline_guarded.sh` 包装正式启动脚本，并用 `docker stats` 按固定间隔采样 RSS。
- 本轮使用 `claw-incremental-pipeline:prod-20260422`、`DOCKER_USER=2005:2005`、`RUN_ON_START=true`、`MEMORY_LIMIT_GIB=256`，LLM 端点为 `http://182.242.159.76:31870/v1`。
- 监控日志显示，容器内存一路从几十 MiB 级爬升，经 `150 GiB`、`187 GiB`、`224 GiB`、`248 GiB` 后，在 `2026-04-22 19:17:38` 到达 `260.2 GiB`。
- 受控脚本在同一时刻触发 `memory threshold exceeded` 并主动执行 `docker kill`。
- 事后检查容器状态为 `Exited (137)`，且 `oom_killed=false`，说明不是 Docker 或宿主机先触发 OOM，而是我们设计的阈值保护生效。

这条结果的重要性在于：

- 它把“manydata 正式路径可能把机器拖入极高内存区间”的判断，从离线 benchmark 提升成了真实 Docker 运行链路上的实证。
- 它同时证明了 guard 脚本本身是有效的，能在超过上限后及时切断进程，避免继续上冲。
- 但它也进一步说明，当前系统仍然具备爬升到 `256 GiB` 以上的能力，因此 guard 只能作为防线，不能作为问题已解决的证据。

进一步定位后的补充结论是：

- 串行模式虽然移除了“多进程 + 多 pending chunk”的结构性放大器，但没有消除“超大 `raw_json` 写入 DuckDB 本身”的高内存问题。
- 对当前首个 manydata 大文件 `items.jsonl` 的实测显示，该文件共有 6563 行，平均每行约 437 KB，总体量约 2736 MB。
- 在隔离实验中，只处理该文件前 64 行时：
  - 读取并解压阶段几乎不涨内存。
  - 进入 `_parse_jsonl_chunk` 后，当前 RSS 上升到约 164 MB。
  - 同一批 64 行写入 DuckDB 后，当前 RSS 上升到约 270 MB。
- 连续处理 30 个 chunk，也就是 1920 行后，当前 RSS 已增长到约 1.84 GB。

这说明：

- 当前真正的主放大点已经从“并行导入框架”缩小到“超大 JSON 行在 DuckDB 中的持续驻留与写入路径”。
- 仅靠强制串行可以把风险从“极易瞬间冲高”降下来，但并不能阻止 RSS 随已导入超大 `raw_json` 持续抬升。

### 6.4 DuckDB 存储层是否存在额外内存滞留

对 `DuckDBStore.insert_sample_batch_detailed` 的复查结论如下:

1. 写入是单事务 `executemany`，作用域局限于当前批次。
2. 函数不会缓存历史批次的 `rows`，上一批完成后 Python 层引用即释放。
3. 当前路径没有额外的应用层结果缓存、全量样本缓存或长生命周期队列。

结论:
- DuckDB 写入本身会吃内存，但这里没有发现“应用层把所有批次对象留住不放”的问题。
- 导入阶段的主要风险来自两部分叠加：
  - 输入 chunk 在 Python 侧的解析与对象膨胀。
  - 超大 `raw_json` 列写入 DuckDB 后带来的持续驻留增长。

进一步对照实验显示：

- 当表只保留 `sample_uid + raw_json` 时，1920 行数据的当前 RSS 约为 1.84 GB。
- 当表去掉 `raw_json`，仅保留派生元数据字段时，1920 行数据的当前 RSS 约为 471 MB。
- 当只向 DuckDB 写入极简的 `sample_uid` 单列、总计 1920 行时，当前 RSS 也会从约 38 MB 升到约 119 MB，说明 DuckDB 连接、列缓冲和执行器本身就有一个非零的基础驻留成本。
- 当只做 `_parse_jsonl_chunk`、并且整个进程始终只保留“最后一个 chunk 的 rows”时，30 个 chunk 后当前 RSS 仍会停在约 520 MB，说明 Python 对超大 JSON 行的解析对象和分配器 arena 也会留下明显驻留，不会随着 `gc.collect()` 立即回落。
- 在仅保留元数据字段的写入实验里，DuckDB 连接关闭前当前 RSS 约为 491 MB，而关闭连接后回落到约 136 MB，说明这 400 MB 级别占用里同时包含了：
  - Python 解析侧未立刻归还给操作系统的大对象内存。
  - DuckDB 连接生命周期内持有的表数据、执行缓冲和缓存页。

这说明 `raw_json` 大字段本身就是当前导入内存问题的主驱动因素，派生字段只占较小的一部分。

更精确地说，当前导入阶段的内存放大来源至少有三层：

- 第一层是 Python 解析放大。
  原始 JSONL 行是连续文本，但 `json.loads` 之后会变成大量 dict、list、str、整数和小对象；这些对象的头部开销、指针和 Unicode 存储都会让内存显著大于原始文本体积。
- 第二层是导入参数编组放大。
  在 [claw_data_filter/importers/jsonl_importer.py](claw_data_filter/importers/jsonl_importer.py) 里，每一行在 `_parse_jsonl_chunk` 后会同时保留 sample_uid、重新 `json.dumps` 后的 raw_json 字符串、以及多个派生字段字符串；在真正进入 DuckDB 前，这些 Python tuple 会先在进程里形成一份完整参数集。
- 第三层是 DuckDB 连接内驻留。
  在 [claw_data_filter/storage/duckdb_store.py](claw_data_filter/storage/duckdb_store.py) 里，`executemany` 会把 Python 参数再复制进 DuckDB 的向量化执行路径；只要连接还开着，这些新写入列、事务/WAL 缓冲和缓存页就会继续占用进程 RSS，直到连接关闭后才明显回落。

因此，“停用 raw_json 写入”不是万能根治，但它仍然是最有效的单点降压手段，因为它去掉的是当前最大的一块持续驻留。即便去掉 raw_json，超大 JSON 行的 Python 解析和 DuckDB 写入基础成本仍会保留，所以真正的根源修复应该是两步同时做：

- 减少入库字段体积，尤其避免把完整超大 raw_json 常驻写入主表。
- 把导入改成更低峰值的路径，尽量减少 Python 中间对象和 DuckDB 连接内缓存同时存在的时间窗口。

### 6.5 round feedback 阶段内存审查

审查对象:
- claw_data_filter/processors/round_feedback.py
- claw_data_filter/storage/duckdb_store.py
- claw_data_filter/cli.py

关键实现事实:

1. `claim_unprocessed_samples(limit=batch_size)` 会一次性从 DuckDB 取出一个 batch 的 `raw_json`，并立刻 `json.loads` 成 Python dict。
2. manydata 正式配置的 round feedback 是 `workers = 2`、`batch_size = 4`。
3. `process_sample` 会先构建该样本的全部 `response_contexts`、`episode_contexts`，再一次性构建 `prepared_plan`，也就是先把所有 prompt 字符串都生成出来，然后再 `asyncio.gather` 发起执行。
4. 单样本处理完成后，结果通过 `replace_round_feedback_results` 原子落库，没有长期缓存已完成样本的 judgments 列表。

### 6.6 基于真实 manydata 数据的 feedback 量级估算

对真实 manydata 数据做 prompt 采样后的结果如下:

- prompt 长度 p50: 1968 字符
- prompt 长度 p95: 8125 字符
- prompt 长度 max: 11387 字符
- 单样本 response contexts p95: 244
- 单样本 response contexts max: 256
- 单样本 episode contexts p95: 23
- 单样本 episode contexts max: 29

由此估算:

1. 在当前真实数据分布下，单样本 prompt 文本总量大致是:
   - p95: `(244 + 23) * 8125 ≈ 2.1 MB`
   - 样本内极端观测值: `(256 + 29) * 11387 ≈ 3.1 MB`
2. 再加上上下文对象、原始消息列表、LLM 返回结果对象和 Python 容器开销，单样本 feedback 阶段通常会上升到数 MB 到数十 MB 量级。
3. 在正式配置 `batch_size = 4`、`max_concurrency = 2` 下，batch 级峰值会进一步放大，但通常仍应停留在“几十 MB 到低百 MB”这一档，而不是导入阶段旧并行路径的多 GB 风险档。

还有一个理论上界需要说明:

- `RoundFeedbackProcessor` 对单个 prompt 设置了 `prompt_char_limit = 100000` 的硬上限，超出会直接失败，不会继续放大单 prompt。
- 但代码没有对“单样本 prompt 总字符数”设置单独上限，因此如果未来出现极端长会话、同时拥有大量 response / episode context，单样本仍可能在 prompt 预构建阶段占用较高内存。

这属于“有边界但仍需关注的峰值问题”，不属于当前看到的典型泄漏模式。

### 6.7 是否发现不释放内存或无界累积

本轮 review 没有发现以下类型的问题:

- 全量读取整个 JSONL 文件到内存
- 导入完成后仍持有历史 chunk / rows 的长期引用
- round feedback 在 batch 之间缓存全部历史结果
- Web 或存储层对已完成 judgment 做额外常驻缓存

因此，更准确的结论是:

- 导入阶段旧并行路径的问题是“峰值过高”，不是“越跑越涨且不释放”的典型泄漏。
- round feedback 当前实现的问题是“单样本先全量构建上下文和 prompt”，但在当前配置与真实数据下，峰值仍明显低于旧并行导入路径。

### 6.8 已完成修复与当前路线结论

在内存专项补充 review 完成后，可以把目前已经落地的事项和后续路线判断明确收口如下：

已完成并应视为 review 结论一部分的内容：

1. importer 已补齐 `max_pending_chunks` 与 `reconnect_every_chunks`，manydata 正式配置已收紧到 `workers = 2`、`chunk_size = 4`、`max_pending_chunks = 2`。
2. importer 热路径已从“每行构造完整 `Sample.from_dict`”切到“直接提取导入字段”，确认能降低一部分 Python 对象构造成本。
3. pipeline 文件级状态已经从“导入完成即 completed”收紧为 `imported` / `exported` / `failed`。
4. Docker 启动入口、调度参数透传、冷启动目录初始化与正式受控启动脚本都已落地。
5. 正式 Docker 受控启动已经实证：当前真实运行路径会继续爬升到 `260.2 GiB`，随后被阈值保护主动 kill；这证明 guard 有效，但也证明根因尚未消除。

基于这些已完成事实，当前路线判断已经明确：

1. route A 可以视为已经做到可做的主要止血项，不再适合作为后续主线继续扩展。
2. route A 的价值是证明问题主放大点在哪里，并把系统从“极易失控”拉回到“可以继续诊断”，不是作为最终完成态。
3. 接下来的唯一合理主线应当是 route B，也就是移除 `raw_json` 运行时依赖，改为结构化字段主输入，并通过新库重建完成切换。
4. 由于导出功能已经明确不再需要 `raw_jsonl`，route B 也不再需要保留 payload loader 或 locator 回放链路作为兼容妥协。

---

## 7. 仍然保留的风险与未完成项

### 7.1 processing 状态卡死恢复机制尚未实现

当前批处理在 claim 后会将样本标为 processing。如果进程在 claim 完成后、样本处理完成前直接崩溃，这些样本会停留在 processing。

影响:
- 后续批处理默认不会重新领取这些样本。

建议:
- 增加 processing 超时回收机制。
- 例如基于 processing_updated_at 实现 lease 过期重领。

### 7.2 导入阶段并行模式的内存风险仍然存在于代码能力层

当前正式部署已经用环境变量把导入强制切回串行，这解决的是运行时配置风险，不是代码层并行模式被彻底删除。

影响:
- 只要未来再次在大样本 manydata 场景下启用 `workers > 1`，导入阶段仍可能重新出现多 GB 级内存峰值。

建议:
- manydata 正式环境保持 `CLAW_IMPORT_FORCE_SERIAL=1` 不变。
- 若未来必须恢复并行导入，需要先重构为更严格的有界流水线，至少避免 `workers * 2` 个大 chunk 同时在父子进程间复制。
- 即便保持串行，也需要进一步削减 `raw_json` 入库路径的内存成本，否则超大 `items.jsonl` 仍可能在长文件导入时把 RSS 推到不可接受的水平。

### 7.3 多进程/多实例并发领取仍未完全验证

当前方案对单进程 asyncio 场景有效，但如果未来同时运行多个 round-feedback 进程或多个实例，DuckDB 的并发写入和 claim 事务语义仍需进一步验证。

建议:
- 明确运行模型是否只允许单实例。
- 如果需要多实例，需重新设计领取机制或改用更适合并发 claim 的存储。

### 7.4 round feedback 仍存在中等峰值内存风险

当前 round feedback 未发现泄漏，但它会在单样本内部先构建全部 context 和全部 prompt，再统一 `gather` 执行。

影响:
- 在极端长会话、极高 batch_size 或更高并发配置下，feedback 峰值内存仍可能明显上升。

建议:
- 保持 manydata 正式配置中的 `batch_size = 4`、`workers = 2` 这类保守值。
- 如果后续需要扩容，优先把 `prepared_plan` 改为流式生成和消费，避免单样本一次性持有全部 prompt。
- 若需要更严格控制上界，可增加“单样本总 prompt 字符数”或“单样本最大 contexts 数”的硬限制。

### 7.5 Web 页面缺少自动化测试

虽然 Web 侧已切到统一后端语义，但当前没有针对 Streamlit 页面行为的自动化测试。

建议:
- 至少增加页面级 smoke test。
- 核心关注 detail 页面、导出页和筛选页的基本加载与参数联动。

### 7.6 Web 页面仍有体验级改进空间

当前剩余问题主要偏低风险:
- detail 页面直接访问无 sample_id 时仍比较简陋。
- 页面未加缓存策略。
- 长文本展示和大数据量交互还有优化空间。

---

## 8. 文件级结论

### 8.1 核心修复文件

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

### 8.2 同步修复文件

- claw_data_filter/web/pages/filter.py
- claw_data_filter/web/pages/export.py
- claw_data_filter/web/pages/sample_detail.py
- claw_data_filter/web/pages/overview.py
- claw_data_filter/web/pages/tables.py

### 8.3 测试补充文件

- tests/test_models.py
- tests/test_jsonl_importer.py
- tests/test_duckdb_store.py
- tests/test_query_filter.py
- tests/test_exporters.py
- tests/test_round_feedback.py

---

## 9. 最终结论

本次 review 的核心结论是:

1. 最初存在的主要问题集中在数据语义不一致、处理完成定义错误、重跑不可靠、SQL 查询不够安全，以及 Web 页面和后端逻辑分叉。
2. 这些问题中的主风险项已经完成修复，并形成了一套更一致的主链路: 导入 -> claim -> round feedback -> completed/failed -> 筛选/导出 -> Web 展示。
3. 当前系统已经从“功能基本可跑”提升到“主链路可维护、可验证、可重跑”的状态。
4. 从内存管理角度看，旧并行导入路径曾具备把 manydata 正式环境推向多 GB 峰值的现实能力，因此不能排除之前的宿主机内存爆炸就是由本服务造成。
5. 进一步定位表明，哪怕在串行导入下，超大 `raw_json` 持续写入 DuckDB 仍会随已处理行数推高 RSS；当前问题已缩小到 DuckDB 写入超大原始 JSON 的驻留成本，而不再是单纯的解压或 Python 解析问题。
6. round feedback 阶段目前更接近“有边界的峰值问题”，而不是“持续泄漏问题”。
7. route A 相关止血项已经完成，但正式 Docker 实证说明它不能作为最终交付态；后续主线应明确切到 route B。
8. 后续最值得优先继续做的不是再加新功能，而是补上 processing 超时回收、完成去 `raw_json` 的结构化主链路切换，以及 Web 页面 smoke test，这几项会同时提升真实运行稳定性和可解释性。

综合评级:
- 修复完成度: 高
- 数据正确性: 中高
- 并发与恢复能力: 中
- 可维护性: 中高
- 剩余风险等级: 中

---

审查与修复结论人: GitHub Copilot
模型: GPT-5.4
结论日期: 2026-04-22