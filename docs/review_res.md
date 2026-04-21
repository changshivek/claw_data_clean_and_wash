# Pipeline 代码审查结论

本次 review 基于原始计划文档，对完整 pipeline 相关实现做了静态代码审查。审查范围不只依赖现有测试，而是重点检查以下方面：

- 增量幂等与并发安全
- 导入、评分、导出链路的一致性
- 失败恢复与状态可追溯性
- Docker/cron 部署链路的实际可用性

审查范围主要包括：

- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py)
- [claw_data_filter/pipeline/config.py](claw_data_filter/pipeline/config.py)
- [claw_data_filter/storage/duckdb_store.py](claw_data_filter/storage/duckdb_store.py)
- [claw_data_filter/exporters/unified_exporter.py](claw_data_filter/exporters/unified_exporter.py)
- [claw_data_filter/importers/jsonl_importer.py](claw_data_filter/importers/jsonl_importer.py)
- [scripts/docker_run_incremental_pipeline.sh](scripts/docker_run_incremental_pipeline.sh)
- [docker/entrypoint.sh](docker/entrypoint.sh)
- [tests/test_pipeline_service.py](tests/test_pipeline_service.py)

## 结论摘要

本次 review 发现 5 个需要优先处理的问题：

1. 高危：增量 pipeline 对重入/并发运行没有锁保护，可能重复处理同一归档文件。
2. 高危：Docker 启动脚本没有把 LLM 环境变量传入容器，README 中的启动方式在默认实现下并不可靠。
3. 高危：全新容器 / 空 runtime 下，若不存在 db 目录与解压目录，pipeline 初始化顺序会导致冷启动直接失败。
4. 中危：pipeline 在真正导入前做了一次更严格的整文件预解析，绕过了 `skip_errors` 语义。
5. 中危：文件级运行记录会在导出前先被标记为 completed，导出失败时状态可能失真。

现有测试主要覆盖了“单进程、单次运行、输入干净、导出成功”的 happy path；没有覆盖重入、冷启动空 runtime、坏行容错、容器环境注入和文件级失败状态一致性。

## Findings

### 1. 高危：重入/并发运行会重复处理同一 source file

位置：

- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L242)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L263)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L281)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L694)

问题：

`_discover_source_files()` 只在“同一路径、同一 fingerprint、状态为 completed”时跳过文件。也就是说，如果上一个 run 还处于 `pending`、`processing` 或者刚刚开始但尚未完成，新的 cron run 仍然会再次把同一个文件加入待处理列表。

这和计划文档里“每次只增量导出新增数据”的目标是冲突的，尤其在以下场景下会出问题：

- cron 周期短于单次 pipeline 执行时长
- 容器内 cron 与人工手动触发 `pipeline-run` 重叠
- 上一轮 run 已经开始处理但还没把 `pipeline_source_files.status` 落到 `completed`

进一步的问题是，当前 run 对“本次新增样本”的归属判断依赖 `_new_sample_uids()` 在导入前做的快照判断。如果两个 run 重叠，后来的 run 可能把前一个 run 实际导入的 sample_uid 误当成“本次 run 的新增样本”，从而造成重复导出或错误归属。

影响：

- 同一个 tar 包被重复解压、重复评分、重复导出
- `pipeline_run_samples` 可能错误归属 sample_uid
- 导出的批次不再严格对应某一次 run 的真实新增数据

建议：

- 在 `pipeline_runs` 或 `pipeline_source_files` 级别增加显式锁或 claim 机制。
- `_discover_source_files()` 至少应跳过 `processing` 状态且 fingerprint 未变化的文件。
- 更理想的做法是对 source file 做原子 claim，而不是先查再处理。

### 2. 高危：Docker 启动脚本没有把 LLM 环境变量传入容器

位置：

- [scripts/docker_run_incremental_pipeline.sh](scripts/docker_run_incremental_pipeline.sh#L25)
- [scripts/docker_run_incremental_pipeline.sh](scripts/docker_run_incremental_pipeline.sh#L28)
- [claw_data_filter/pipeline/config.py](claw_data_filter/pipeline/config.py#L95)
- [claw_data_filter/pipeline/config.py](claw_data_filter/pipeline/config.py#L96)
- [claw_data_filter/pipeline/config.py](claw_data_filter/pipeline/config.py#L98)

问题：

README 和使用说明都假定下面的方式可用：

```bash
LLM_API_KEY=... bash scripts/docker_run_incremental_pipeline.sh
```

但 `scripts/docker_run_incremental_pipeline.sh` 实际只向容器传了 `CONFIG_PATH`，没有传 `LLM_API_KEY`、`LLM_MODEL_ID`、`LLM_ENDPOINT`。而 `PipelineConfig.model_post_init()` 又明确依赖容器内环境变量来补齐这些字段。

因此默认 Docker 启动路径下，除非：

- TOML 里直接写死了 endpoint/model/api_key，或
- 镜像/容器另有外部注入机制，

否则 round feedback 阶段会因为缺少模型配置或 API key 而失败。

影响：

- README 中推荐的容器启动方式与实际脚本行为不一致
- 容器部署看起来能启动，但在真正调用 LLM 时才失败
- 问题暴露时间点偏后，排查成本高

建议：

- 在 `docker run` 中显式透传 `LLM_API_KEY`、`LLM_MODEL_ID`、`LLM_ENDPOINT`。
- 或者把脚本改为只接受完整 TOML 配置，并在脚本入口明确校验所需字段是否已在配置中给出。

### 3. 高危：全新容器 / 空 runtime 下冷启动会直接失败

位置：

- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L70)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L72)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L75)
- [docker/entrypoint.sh](docker/entrypoint.sh#L36)

问题：

`PipelineService.__init__()` 里先执行 `DuckDBStore(config.paths.db_path)`，之后才执行 `_ensure_directories()`。这意味着如果 `db_path.parent` 还不存在，DuckDB 连接会在建目录之前就失败。

这在以下场景中会直接触发：

- 新鲜启动容器，runtime volume 为空
- 容器里没有现成的 `db`、`unpack`、`work`、`export`、`logs` 子目录
- 使用 named volume 做隔离验证，且 volume 里还没初始化目录结构

`docker/entrypoint.sh` 当前只会执行 `mkdir -p /app/runtime`，不会按照 TOML 中的 `paths.*` 自动创建 `db_path.parent`、`unpack_dir`、`work_dir`、`export_dir`、`log_dir`。而 service 的初始化顺序又要求这些目录在 `DuckDBStore` 建连前就存在，因此冷启动容器会偏离“首次启动即可自动初始化”的预期。

这个问题已经在实际 Docker cron 隔离验证中暴露过：当 runtime 使用全新 named volume 时，首次定时执行会报 DuckDB `No such file or directory`，只有手动先创建 `db/export/logs/unpack/work` 目录后，后续 cron run 才能正常执行。

影响：

- 新环境首次启动不能自举成功
- 冷启动失败点发生在 service 初始化阶段，run 级日志和 file 级状态都来不及完整落盘
- Docker E2E、named volume 验证、全新部署都会踩到这个问题

建议：

- 将 `PipelineService.__init__()` 的顺序改为先 `_ensure_directories()`，再初始化 `DuckDBStore` 和 `UnifiedExporter`。
- 或者在 `DuckDBStore` 外层先确保 `db_path.parent.mkdir(parents=True, exist_ok=True)`。
- `docker/entrypoint.sh` 也应考虑在启动时根据配置文件提前创建运行目录，而不是只创建 `/app/runtime`。

### 4. 中危：pipeline 的预扫描绕过了 `skip_errors` 语义

位置：

- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L307)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L315)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L317)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L614)
- [claw_data_filter/importers/jsonl_importer.py](claw_data_filter/importers/jsonl_importer.py#L115)

问题：

`_process_source_file()` 在调用 importer 之前，先通过 `_collect_sample_uids()` 整文件遍历一次 `items.jsonl`。这一步直接做 `json.loads()` 和 `Sample.from_dict()`，没有复用 importer 的容错逻辑，也没有接收 `skip_errors` 配置。

这意味着：

- 即使 `import.skip_errors = true`
- importer 本身本来可以跳过坏行继续导入

pipeline 仍然会在预扫描阶段先因为一条坏行直接失败，整包无法处理。

这使得 pipeline 的容错性比 CLI importer 更差，也违反了配置层面对“是否跳过坏行”的统一语义。

影响：

- 一条坏行可以导致整个 tar 包处理失败
- `skip_errors` 在 pipeline 路径下并不真正生效
- 当前 tests 没有覆盖这个差异，容易长期隐藏

建议：

- 不要在导入前做第二遍更严格的整文件解析。
- 若确实需要 sample_uid 预判，应把这一步并入 importer，确保与 `skip_errors` 使用同一套解析和容错规则。

### 5. 中危：文件级状态在导出前提前写成 completed，失败时会失真

位置：

- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L339)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L348)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L452)
- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py#L500)

问题：

`_process_source_file()` 在“解压 + 导入”结束后就把 `pipeline_run_files` 和 `pipeline_source_files` 写成 `completed`。但真正的业务闭环还包括：

- 筛选 qualified samples
- 导出 openai_round_feedback
- 转换 Unisound

如果上述任何一步在 `_export_file_result()` 中抛错，整个 `pipeline_runs` 会被标记为 failed，但文件级记录已经在之前被写成 `completed`。除非异常恰好发生在 `_export_file_result()` 成功回写之后，否则 file-level 状态会和 run-level 状态不一致。

影响：

- 排障时会出现“run 失败，但文件记录显示 completed”的矛盾状态
- 文件级状态无法准确表示“导入成功但导出失败”这类中间失败

建议：

- 将 file-level 状态拆分为至少 `imported` / `exported` / `failed` 三阶段。
- 或者在 `_process_source_file()` 完成后仅写 `imported`，由 `_export_file_result()` 结束后再写最终 `completed`。

## 测试缺口

现有 [tests/test_pipeline_service.py](tests/test_pipeline_service.py) 主要覆盖：

- 单次运行成功
- 第二次运行跳过已完成 archive
- CLI 能读取 TOML 并执行一次 happy path

但以下关键场景没有覆盖：

- 两次 `pipeline-run` 重叠执行时的 source file claim 行为
- 全新容器 / 空 runtime volume 下首次启动时的目录初始化行为
- `items.jsonl` 含坏行且 `skip_errors = true` 时的 pipeline 表现
- Docker 启动脚本是否真的把 LLM 环境变量带进容器
- 导出 / Unisound 转换失败时 `pipeline_run_files` 的状态一致性

建议后续补充针对这些场景的回归测试，否则目前的测试通过不足以证明“增量 + 定时 + 容错 + 部署”这几个目标已经稳固。

## 总体评价

当前实现已经具备完整功能链路，且单次 happy path 基本打通；但如果以“长期定时增量服务”的标准衡量，仍有两类问题需要优先修正：

1. 并发/重入安全
2. 冷启动自举能力与部署入口配置一致性

在这两类问题解决前，代码更适合受控环境下的串行运行，不适合把 cron 周期压得很短或和人工补跑并行使用。