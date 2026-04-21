# Pipeline 修复方案

本文基于 [docs/review_res.md](docs/review_res.md) 中的审查结论，给出面向落地执行的修复方案。目标不是泛泛建议，而是把每个问题拆成：

- 修复目标
- 建议改法
- 涉及文件
- 需要补的测试
- 验收标准

## 修复优先级

建议按以下顺序推进：

1. P0: 增量 pipeline 重入/并发 claim 问题
2. P0: Docker 启动脚本未透传 LLM 环境变量
3. P0: 全新容器 / 空 runtime 下的冷启动初始化失败
4. P1: pipeline 预扫描绕过 `skip_errors`
5. P1: 文件级状态过早写成 completed

原因：

- 前两项会直接影响“定时服务是否可靠可用”。
- 第三项会直接影响新环境首次部署和 named volume 冷启动。
- 后两项会影响“容错性和状态一致性”，对长期运行同样重要，但可以在重入安全修复后继续推进。

## 方案一：修复 source file 的重入 / 并发重复处理

### 目标

避免两个 run 同时处理同一个 source file，确保：

- 同一 fingerprint 的文件只会被一个 run claim。
- cron 周期短于单次 run 时不会重复导入、重复评分、重复导出。
- `pipeline_run_samples` 中 sample_uid 的归属与真实 run 一致。

### 建议改法

当前 `_discover_source_files()` 的逻辑是“先查状态，再决定是否处理”，这不是原子的。建议改成显式 claim 模型。

推荐方案：

1. 为 `pipeline_source_files` 的状态语义收敛为：
   - `pending`
   - `processing`
   - `completed`
   - `failed`

2. 引入单文件原子 claim 方法，例如：
   - `_try_claim_source_file(plan, run_id, now) -> bool`

3. claim 逻辑改成：
   - 若不存在记录：插入 `processing`
   - 若 fingerprint 相同且状态为 `completed`：跳过
   - 若 fingerprint 相同且状态为 `processing`：跳过
   - 若 fingerprint 不同：允许更新并重新处理
   - 若 fingerprint 相同且状态为 `failed`：允许当前 run 重新 claim

4. 所有“是否处理该文件”的判断都通过 claim 结果决定，不再先查后处理。

### 涉及文件

- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py)

### 需要补的测试

1. 两次连续 run，第二次遇到 `processing` 状态且 fingerprint 未变化时应跳过。
2. `failed` 状态且 fingerprint 未变化时，应允许重新处理。
3. fingerprint 变化时，即使同一路径也应重新处理。
4. `pipeline_run_samples` 只记录真正被当前 run claim 并成功导入的 sample_uid。

### 验收标准

- 定时触发与手动补跑重叠时，同一个 tar 不会被两个 run 重复处理。
- 第二个 run 不会把第一个 run 正在导入的数据误判为自己的增量样本。

## 方案二：修复 Docker 启动脚本的 LLM 环境变量透传

### 目标

保证 README 和脚本行为一致，使下面的方式真正可用：

```bash
LLM_API_KEY=... \
LLM_ENDPOINT=... \
LLM_MODEL_ID=... \
bash scripts/docker_run_incremental_pipeline.sh
```

### 建议改法

推荐做法是显式透传环境变量，而不是依赖镜像外部环境偶然继承。

修改 `scripts/docker_run_incremental_pipeline.sh`：

1. 在脚本开头读取并透传：
   - `LLM_API_KEY`
   - `LLM_ENDPOINT`
   - `LLM_MODEL_ID`

2. `docker run` 增加：
   - `-e LLM_API_KEY`
   - `-e LLM_ENDPOINT`
   - `-e LLM_MODEL_ID`

3. 如果用户既没有在 TOML 中配置，也没有提供环境变量，脚本应在启动前给出明确错误，而不是让容器运行到 round feedback 才失败。

更稳妥的增强：

4. 在脚本中增加一个轻量配置检查：
   - 只要 `round_feedback.enabled=true`，就要求 endpoint/model/api_key 三者至少能从“配置或环境变量”中解析出来。

### 涉及文件

- [scripts/docker_run_incremental_pipeline.sh](scripts/docker_run_incremental_pipeline.sh)
- [README.md](README.md)

### 需要补的测试

这部分较难做纯单元测试，建议至少补两类验证：

1. shell 脚本静态回归：保证新增环境变量处理后仍可执行。
2. Docker E2E 验证：容器内执行 `env | grep LLM_` 可看到透传结果。

### 验收标准

- README 中的启动命令与实际脚本行为一致。
- 容器内 `PipelineConfig.model_post_init()` 能拿到所需环境变量。

## 方案三：修复全新容器 / 空 runtime 的冷启动初始化

### 目标

保证在以下场景下无需人工预创建目录也能直接启动成功：

- 新鲜启动容器
- runtime volume 为空
- 尚不存在任何 DuckDB 文件
- 尚不存在任何解压结果或导出目录

### 建议改法

根因是 `PipelineService.__init__()` 当前先初始化 `DuckDBStore`，后创建运行目录。应将顺序调整为“先建目录，再连库”。

推荐方案：

1. 修改 `PipelineService.__init__()`：
   - 先 `self._ensure_directories()`
   - 再 `self.store = DuckDBStore(...)`
   - 再初始化 exporter 和 pipeline schema

2. 将 `_ensure_directories()` 明确视为 service 初始化的前置条件，而不是 store 初始化后的补救步骤。

3. 在 `docker/entrypoint.sh` 中进一步增强：
   - 解析 `CONFIG_PATH`
   - 按 `paths.unpack_dir`、`paths.work_dir`、`paths.export_dir`、`paths.log_dir`、`paths.db_path.parent` 预创建目录

4. 如果要保持 entrypoint 尽量薄，也至少要确保 service 侧顺序修正，这样即使 entrypoint 不主动建目录，首次 run 也能自举成功。

### 涉及文件

- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py)
- [docker/entrypoint.sh](docker/entrypoint.sh)

### 需要补的测试

1. `db_path.parent` 不存在时，初始化 `PipelineService` 仍可成功创建数据库。
2. `unpack/work/export/log` 目录都不存在时，首次 `run_once()` 仍可成功启动。
3. 使用全新 runtime volume 的 Docker E2E 中，不应再需要手工 `mkdir db export logs unpack work`。

### 验收标准

- 新环境首次启动无需手工准备 runtime 子目录。
- DuckDB 文件能在首次 run 中自动创建。
- 冷启动容器不会因为 `No such file or directory` 在 service 初始化阶段失败。

## 方案四：移除导入前的严格预扫描，统一 `skip_errors` 语义

### 目标

保证 pipeline 和 CLI importer 对坏行的处理语义一致：

- `skip_errors=true` 时跳过坏行继续处理
- `skip_errors=false` 时遇错失败

### 建议改法

当前问题的根因是 `_collect_sample_uids()` 在 importer 之前独立做了一次严格解析。建议删掉这条链路，把“识别本次新增 sample_uid”收敛到导入结果本身。

推荐方案：

1. 修改 importer，让其在 `import_lines()` 或 `insert_sample_batch()` 路径上返回更丰富的结果，而不仅是 count。

例如返回：

```python
{
    "imported_count": ...,
    "imported_sample_uids": [...],
    "skipped_count": ...,
    "error_count": ...,
}
```

2. pipeline 不再调用 `_collect_sample_uids()` 与 `_new_sample_uids()` 组合预判新增样本，而是直接使用 importer 实际成功插入的 sample_uid 列表。

3. 如果暂时不想大改 importer，也可以退一步：
   - 先导入
   - 再用 `pipeline_run_samples` 或 `imported_at` 范围查询本次成功插入的 sample_uid

但从长期看，直接由 importer 返回插入结果会更清晰。

### 涉及文件

- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py)
- [claw_data_filter/importers/jsonl_importer.py](claw_data_filter/importers/jsonl_importer.py)
- [claw_data_filter/storage/duckdb_store.py](claw_data_filter/storage/duckdb_store.py)

### 需要补的测试

1. `skip_errors=true` 且 JSONL 中夹杂坏行时，pipeline 应继续导入其余合法样本。
2. `skip_errors=false` 时，pipeline 应在坏行处失败。
3. 本次导出的 sample_uid 只应来自本次真正插入成功的样本。

### 验收标准

- pipeline 与 CLI importer 对坏行处理行为一致。
- 一条坏行不会在 `skip_errors=true` 时导致整包失败。

## 方案五：收敛文件级状态机，避免导出前过早 completed

### 目标

让 `pipeline_run_files` 和 `pipeline_source_files` 的状态能够准确反映文件在整个链路中的实际阶段。

### 建议改法

推荐把文件级状态拆成更清晰的阶段：

- `processing`
- `imported`
- `exported`
- `failed`

推荐调整方式：

1. `_process_source_file()` 成功后只写：
   - `status = imported`

2. `_export_file_result()` 成功后再写：
   - `status = exported` 或 `completed`

3. `_export_file_result()` 内部若抛错，应把对应 run_file/source_file 标记为 `failed` 并记录 error_message。

4. `pipeline_runs.status` 继续保留 run 级别汇总，不替代 file 级真实状态。

### 涉及文件

- [claw_data_filter/pipeline/service.py](claw_data_filter/pipeline/service.py)

### 需要补的测试

1. 导入成功、导出失败时：
   - `pipeline_runs.status = failed`
   - `pipeline_run_files.status = failed`
   - `pipeline_source_files.status = failed` 或 `imported`，取决于最终状态设计，但必须和代码一致。

2. 导入成功、无 qualified samples 时：
   - 状态应明确是“成功但无导出”，不能和失败混淆。

### 验收标准

- run 级状态和 file 级状态不再相互矛盾。
- 排障时可以看清是卡在导入、筛选、导出还是 Unisound 转换。

## 建议实施顺序

### 第 1 步

先修方案一、方案二和方案三。

原因：

- 这是让 cron 服务真正可上线的最低条件。
- 不解决它们，系统在短周期调度、容器部署和新环境首次启动下仍然不稳定。

### 第 2 步

再修方案四。

原因：

- 这是导入链路语义统一问题。
- 一旦后续数据源质量波动，这会是高频故障点。

### 第 3 步

最后修方案五，并补完整状态一致性测试。

原因：

- 这是可观测性和故障定位质量问题。
- 虽然不一定第一时间导致功能不可用，但会显著增加维护成本。

## 建议的交付拆分

建议拆成 4 个提交：

1. `fix: claim source files atomically in incremental pipeline`
2. `fix: pass llm env vars into docker pipeline container`
3. `fix: bootstrap pipeline runtime directories before first run`
4. `fix: align pipeline import error handling and file status transitions`

这样可以保持每次修改的目的清晰，也方便单独回归和回滚。