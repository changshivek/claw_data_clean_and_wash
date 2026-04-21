# Claw Data Filter

面向 agent 对话数据的筛选、评分、导出与增量处理工具。

当前仓库覆盖两类主要使用方式：

1. 普通 JSONL 数据导入 DuckDB，执行 session merge、round feedback、筛选导出。
2. 监控 manydata 下新增 tar 数据，按增量 pipeline 自动完成导入、评分、导出和 Unisound 转换。

## 功能概览

- 导入 OpenAI / UniRouter 风格 JSONL 数据。
- 写入 DuckDB，并生成稳定的 sample_uid。
- 标记 empty_response 样本。
- 执行 session merge，去掉重复会话快照。
- 执行 round feedback，生成 response_progress 和 user_satisfied 两层 judgment。
- 按统一条件导出 raw_jsonl 或 openai_round_feedback。
- 将 openai_round_feedback JSONL 转成 Unisound JSONL。
- 通过 Streamlit Web 页面查看、筛选和导出数据。
- 通过增量 pipeline 处理新增 tar 包，并支持容器内 cron 定时执行。

## 环境准备

项目要求：

- Python >= 3.10
- 使用 uv 管理环境
- 使用仓库内虚拟环境 .venv

初始化方式：

```bash
uv venv .venv
./.venv/bin/pip install -e ".[dev]"
```

安装后可直接使用两种入口：

- `claw-filter`
- `./.venv/bin/python -m claw_data_filter.cli`

如果你更希望避免环境路径问题，后文命令优先使用 `./.venv/bin/python -m ...` 形式。

## 快速开始

### 1. 导入数据

```bash
./.venv/bin/python -m claw_data_filter.cli \
  --db-path data.duckdb \
  import data/test_input_single.jsonl
```

### 2. 检查 LLM 连通性

```bash
export LLM_ENDPOINT=http://127.0.0.1:8000/v1
export LLM_MODEL_ID=qwen35
export LLM_API_KEY=dummy

./.venv/bin/python -m claw_data_filter.cli \
  --db-path data.duckdb \
  pressure-test
```

### 3. 执行 session merge

```bash
./.venv/bin/python -m claw_data_filter.cli \
  --db-path data.duckdb \
  session-merge \
  --workers 4 \
  --batch-size 512 \
  --min-prefix-turns 2
```

### 4. 执行 round feedback

```bash
./.venv/bin/python -m claw_data_filter.cli \
  --db-path data.duckdb \
  round-feedback \
  --workers 16 \
  --batch-size 20
```

### 5. 查看统计

```bash
./.venv/bin/python -m claw_data_filter.cli \
  --db-path data.duckdb \
  stats
```

### 6. 筛选并导出

```bash
./.venv/bin/python -m claw_data_filter.cli \
  --db-path data.duckdb \
  filter \
  --session-merge-keep true \
  --empty-response false \
  --num-turns-min 3 \
  --has-error false \
  --export-format openai_round_feedback \
  --export data/exported_round_feedback.jsonl \
  --report data/export_report.json
```

## 常用 CLI 命令

```bash
claw-filter import <file>
claw-filter pressure-test
claw-filter session-merge
claw-filter round-feedback
claw-filter round-feedback-sample
claw-filter filter --export <file>
claw-filter stats
claw-filter info
claw-filter pipeline-run --config <toml>
```

常用场景：

- `import`: 导入 JSONL。
- `pressure-test`: 在正式跑批前验证 LLM 端点可用性。
- `session-merge`: 标记重复会话快照。
- `round-feedback`: 生成评分结果。
- `round-feedback-sample`: 将单个 sample_uid 抽到隔离 DuckDB 单独复现。
- `filter`: 按条件导出样本。
- `pipeline-run`: 手动执行一次增量 tar pipeline。

## 常见使用流程

### 普通 JSONL 流程

适用于手动导入、人工筛选和离线分析：

1. `import`
2. `pressure-test`
3. `session-merge`
4. `round-feedback`
5. `stats`
6. `filter`

### 单样本隔离复现

适用于长样本、异常样本或 prompt 过长样本排查：

```bash
./.venv/bin/python -m claw_data_filter.cli \
  --db-path data.duckdb \
  round-feedback-sample \
  --sample-uid <sample_uid> \
  --isolated-db-path data/isolated/sample.duckdb \
  --workers 1
```

### Web 页面

```bash
DB_PATH=data.duckdb ./.venv/bin/streamlit run claw_data_filter/web/app.py --server.port 5000
```

页面包括：

- overview：统计概览
- filter：筛选与导出
- tables：数据表预览
- detail：样本详情

## 增量 Pipeline

增量 pipeline 用于 manydata 下新增 tar 数据的持续处理，链路包括：

1. 扫描新 tar 包
2. 递归解压
3. 导入 JSONL
4. session merge
5. round feedback
6. 增量筛选导出
7. Unisound 转换
8. 记录运行状态与日志

默认配置文件：

- `configs/autoprocess.pipeline.toml`
- `configs/unisound_export.autoprocess.json`

执行一次增量 pipeline：

```bash
export LLM_ENDPOINT=http://127.0.0.1:8000/v1
export LLM_MODEL_ID=qwen35
export LLM_API_KEY=dummy

./.venv/bin/python -m claw_data_filter.cli \
  pipeline-run \
  --config configs/autoprocess.pipeline.toml
```

也可以直接使用脚本：

```bash
bash scripts/run_incremental_pipeline.sh
```

默认 manydata 路径：

- source_dir=/kanas/nlp/liuchang/manydata/unirouter
- unpack_dir=/kanas/nlp/liuchang/manydata/unirouter_uncompress
- work_dir=/kanas/nlp/liuchang/manydata/unirouter_in_process
- db_path=/kanas/nlp/liuchang/manydata/unirouter_duckdb/incremental_pipeline.duckdb
- export_dir=/kanas/nlp/liuchang/manydata/unirouter_unisound_format

运行状态会写入 DuckDB 中的 pipeline 相关表，并在配置的 log_dir 下生成逐次运行日志。

## Docker 部署

仓库已提供：

- `Dockerfile`
- `docker/entrypoint.sh`
- `docker/pipeline.cron`
- `scripts/docker_build_incremental_pipeline.sh`
- `scripts/docker_run_incremental_pipeline.sh`

构建镜像：

```bash
bash scripts/docker_build_incremental_pipeline.sh
```

启动容器：

```bash
LLM_ENDPOINT=http://127.0.0.1:8000/v1 \
LLM_MODEL_ID=qwen35 \
LLM_API_KEY=your_key \
bash scripts/docker_run_incremental_pipeline.sh
```

容器默认行为：

- 前台启动 Streamlit Web
- 后台通过 cron 定时执行 `pipeline-run`
- 不自动复用或删除已有同名容器

可通过环境变量覆盖：

- `CONFIG_PATH`
- `CRON_SCHEDULE`
- `RUN_ON_START`
- `STREAMLIT_PORT`

## Unisound 离线转换

输入格式为 openai_round_feedback JSONL。

常用命令：

```bash
# 校验输入
./.venv/bin/python scripts/unisound_export.py validate-input \
  --input data/exported_round_feedback.jsonl

# 转换
./.venv/bin/python scripts/unisound_export.py convert \
  --input data/exported_round_feedback.jsonl \
  --output data/exported_unisound.jsonl \
  --config configs/unisound_export.autoprocess.json \
  --report data/exported_unisound.report.json

# 校验输出
./.venv/bin/python scripts/unisound_export.py validate-output \
  --input data/exported_unisound.jsonl
```

## 脚本入口

仓库内常用脚本：

- `scripts/run_import_to_stats.sh`
  适合普通 JSONL 的 import -> pressure-test -> round-feedback -> stats。
- `scripts/run_export.sh`
  适合导出和报告生成。
- `scripts/validate_pipeline_100.sh`
  适合 100 条样本的小规模链路验证。
- `scripts/run_incremental_pipeline.sh`
  适合手动执行一次增量 pipeline。

## 100 条验证集

用于小样本链路验证，不建议作为正式全量跑批入口。

```bash
LLM_ENDPOINT=http://127.0.0.1:8000/v1 \
LLM_API_KEY=dummy \
LLM_MODEL_ID=qwen35 \
MAX_CONCURRENCY=16 \
BATCH_SIZE=20 \
bash scripts/validate_pipeline_100.sh
```

默认产物：

- `data/pipeline_e2e/e2e_100_progress.duckdb`
- `data/pipeline_e2e/validation_progress/exported_round_feedback.jsonl`
- `data/pipeline_e2e/validation_progress/exported_unisound.jsonl`
- `data/pipeline_e2e/validation_progress/export_report_round_feedback.json`

验证脚本的最终导出条件：

- `session_merge_keep = true`
- `empty_response = false`
- `num_turns >= 3`

## 数据格式

支持两类输入：

- OpenAI 风格 `messages`
- UniRouter 风格 `request.bodyJson.messages`

最小 OpenAI 示例：

```json
{
  "messages": [
    {"role": "user", "content": "用户问题"},
    {"role": "assistant", "content": "回复"}
  ]
}
```

更完整的导出字段说明见：

- `docs/export-format.md`

## 常用维护命令

回填老库中的 empty_response：

```bash
./.venv/bin/python scripts/mark_empty_response.py --db-path data/your.duckdb --dry-run
./.venv/bin/python scripts/mark_empty_response.py --db-path data/your.duckdb
```

## 开发与测试

安装开发依赖：

```bash
./.venv/bin/pip install -e ".[dev]"
```

运行测试：

```bash
./.venv/bin/pytest tests/ -v
```

推荐最小回归：

```bash
./.venv/bin/pytest tests/test_cli.py tests/test_pipeline_service.py tests/test_unisound_export.py -q
```

## 相关文档

- `docs/export-format.md`
- `docs/autoprocess_imcremental_data_pipeline_plan.md`
- `docs/unisound-export-migration-plan.md`
- `docs/unisound-export-test-notes.md`