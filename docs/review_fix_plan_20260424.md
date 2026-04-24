# Fix Plan — 2026-04-24（收敛版）

输入来源：
- `docs/review_by_deepseek.md`（DeepSeek review 报告）
- GPT-5.4 对 DeepSeek 报告的交叉审查意见
- 线上暴露的 pending 数据回头消费问题

原则：
- 只修实锤的 blocker 和真实 bug，不夹带重构
- 调度/日志/Docker 加固等方向正确但量大，拆成独立批次
- 以代码现状为准

---

## Round 1: 立即修复（生产相关）

### 1.1 `run_pipeline_if_due.sh` 状态文件时序修复

**位置**: `docker/run_pipeline_if_due.sh:26-27`

**问题**: `date +%s` 写入状态文件在 `pipeline-run` 执行之前。pipeline 失败时时间戳已更新，下一轮调度因间隔检查跳过，失败的 run 永不重试。

**修复**: 改为成功后写入。

```bash
python -m claw_data_filter.cli pipeline-run --config "${CONFIG_PATH}" && date +%s > "${STATE_FILE}"
```

### 1.2 `processing` 状态超时回收（samples 侧）

**位置**: `storage/duckdb_store.py:1065-1075`

**问题**: `claim_unprocessed_samples` 只领取 `pending`/`failed`。若进程在 claim 之后、标记 `completed`/`failed` 之前崩溃，样本永久停留在 `processing`，无恢复路径。

**修复**:
- `DuckDBStore` 新增 `reclaim_stale_processing_samples(stale_minutes)` 作为独立公开方法 —— 将超过阈值的 `processing` 样本回退为 `pending`
- **不在 `claim_unprocessed_samples` 内部自动调用回收** —— 回收逻辑作为独立方法，由调用方显式调用，避免污染 store 的基础语义
- CLI `round-feedback` 在批处理循环开始前显式调用 `store.reclaim_stale_processing_samples()`，由 `--reclaim-stale / --no-reclaim-stale` 控制（默认开启）
- Pipeline `_run_round_feedback_for_samples` 同样在领取前显式调用回收

```python
def reclaim_stale_processing_samples(self, stale_minutes: int = 120) -> int:
    cutoff = datetime.now() - timedelta(minutes=stale_minutes)
    self.conn.execute(
        "UPDATE samples SET processing_status = 'pending' "
        "WHERE processing_status = 'processing' AND processing_updated_at < ?",
        [cutoff],
    )
    return self.conn.changes()
```

### 1.3 `processing` 状态超时回收（pipeline source file 侧）

**位置**: `pipeline/service.py:303-313`

**问题**: `_claim_source_file` 遇到 fingerprint 匹配 + status=`"processing"` 时直接跳过，无超时回收。与 1.2 同因。

**修复**: 在 claim 判断中增加 `last_seen_at` 超时（如 2 小时），超过阈值视为 stale 允许重新 claim。

### 1.4 `claim_unprocessed_samples` pre-merge 盲区提示

**位置**: `cli.py:320-323`（round-feedback 命令调用点）

**问题**: `claim_unprocessed_samples` 的 WHERE 包含 `session_merge_keep = TRUE`。导入后默认 NULL，session-merge 运行前所有样本被静默跳过。CLI 无任何提示，用户看到 "0 samples claimed" 后退出。这是设计意图但 UX 表现为 bug。

**修复**: 不修改 claim WHERE 条件（语义正确），仅在 CLI 侧增加检测：若 claim 返回空，再查一次是否存在 `pending`/`failed` 但 `session_merge_keep IS NULL` 的样本，若有则打印提示 `"有 N 条样本尚未经过 session merge，请先运行 session-merge"`。

### 1.5 `detail_builder.py` falsy 0 值修正

**位置**: `web/services/detail_builder.py:67-71`

**问题**:
```python
expected_judgment_count=sample_record.get("expected_judgment_count") or (len(...) + len(...))
```
`0 or X` 求值为 X。数据库中合法值 `0` 被 `or` 回退逻辑错误替换为计算值。

**修复**: 改为显式 `is not None` 判断。

```python
expected_judgment_count=(
    sample_record.get("expected_judgment_count")
    if sample_record.get("expected_judgment_count") is not None
    else (len(response_steps) + len(user_episodes))
),
```

同样修正 `expected_response_judgment_count`、`expected_episode_judgment_count`。

### 1.6 删除 `add_progress_score_filter` 死代码

**位置**: `filters/query.py:118-149`

**问题**: 方法存在但字段名 `"progress_score"` 不在 `ALLOWED_FIELDS` 中，调用即 `ValueError`。当前无 production 调用点，是死代码。

**修复**: 直接删除。

---

## Round 1 涉及文件

| 文件 | 改动 |
|------|------|
| `docker/run_pipeline_if_due.sh` | 1.1 时序修复 |
| `storage/duckdb_store.py` | 1.2 reclaim 方法 + claim 入口调用 |
| `pipeline/service.py` | 1.3 source file stale 回收 |
| `cli.py` | 1.2 reclaim 选项 + 1.4 pre-merge 提示 |
| `web/services/detail_builder.py` | 1.5 falsy 0 值 |
| `filters/query.py` | 1.6 删除死代码 |

---

## Round 1 验收标准

1. `run_pipeline_if_due.sh` pipeline 失败时状态文件不更新，下一轮调度可重试
2. kill -9 round-feedback 进程后，下一次 run 在 stale_timeout 后回收 processing 样本
3. 未运行 session-merge 直接执行 CLI round-feedback 时打印明确提示（需覆盖"确实存在 pending/failed 且 session_merge_keep 为空"的场景，而非仅空库）
4. `expected_judgment_count=0` 的样本在 detail 页正确显示 0
5. `add_progress_score_filter` 已删除
6. 全量回归通过：
   ```bash
   .venv/bin/pytest tests/ -q
   ```
   核心测试集必须通过：
   ```bash
   .venv/bin/pytest tests/test_cli.py tests/test_pipeline_service.py tests/test_unisound_export.py tests/test_exporters.py -q
   ```

---

## Round 2: 独立整理批次（不混入 Round 1）

以下问题方向正确，但改动面大，应该等 Round 1 完全验证通过后单开一批：

Round 2 的目标不是继续救火，而是把当前已经验证可用的 runtime / logging / maintenance 路径收口成更稳的默认行为，并且避免再次把不同性质的问题混成一个大提交。

### Round 2 前提

进入 Round 2 之前，以下事项已完成：
- Round 1 三个主提交已落地
- sample heartbeat follow-up 已落地
- 全量测试已通过（当前基线：`156 passed`）

Round 2 应继续遵守两个边界：
- 不回滚或重写 Round 1 / follow-up 的语义
- 每个批次只处理一种主问题，先跑窄验证，再做总体验证

---

## Post-Review Follow-up: sample processing heartbeat

在 Round 1 的 3 个提交完成后，复审发现 samples 侧 stale reclaim 仍有一个高风险遗漏：

### F1. `processing_updated_at` 没有运行中心跳，长任务会被误回收

**现状**:
- `claim_unprocessed_samples` 在样本进入 `processing` 时写入一次 `processing_updated_at`
- 样本只有在最终 `completed` / `failed` 落库时才再次更新时间戳
- `reclaim_stale_processing_samples` 仅根据 `processing_updated_at < cutoff` 判断是否回收

**问题**:
- 若一次正常的 round-feedback 处理超过 `stale_minutes`，后续 CLI / pipeline 进程会把仍在运行中的样本误回收到 `pending`
- 这会导致样本被重复领取，产生重复处理和最终写回竞争

**根因**:
- 当前 reclaim 模型缺少“活任务”和“死任务”的区分信号，`processing_updated_at` 只是 claim 时戳，不是最近存活时间

### 修复方案

#### F1.1 存储层新增 sample heartbeat touch

在 `DuckDBStore` 新增独立方法，例如：

```python
def touch_processing_sample(self, sample_uid: str) -> None:
    self.conn.execute(
        "UPDATE samples "
        "SET processing_updated_at = ? "
        "WHERE sample_uid = ? AND processing_status = 'processing'",
        [datetime.now(), sample_uid],
    )
```

要求：
- 只刷新 `processing` 样本
- 不改变 `processing_status`
- 不挪动 reclaim 入口位置

#### F1.2 processor 在 sample 生命周期内定期发送 heartbeat

在 `RoundFeedbackProcessor.process_sample` 生命周期内启动一个轻量后台 heartbeat task：

- 每隔 30-60 秒刷新当前 sample 的 `processing_updated_at`
- sample 正常完成或失败后停止 heartbeat
- heartbeat 失败只记录日志，不改变本次业务处理结果

这样 `reclaim_stale_processing_samples` 仍保持现有语义，但判断依据从“claim 时间”升级为“最近存活时间”。

#### F1.3 reclaim 入口保持显式调用，不塞回 claim 方法内部

这一点沿用当前 Round 1 的修复结果，不再变更：

- CLI `round-feedback` 入口显式 reclaim
- pipeline `_run_round_feedback_for_samples` 入口显式 reclaim
- `claim_unprocessed_samples` 保持纯 claim 语义

### Follow-up 涉及文件

| 文件 | 改动 |
|------|------|
| `claw_data_filter/storage/duckdb_store.py` | 新增 `touch_processing_sample` |
| `claw_data_filter/processors/round_feedback.py` | sample 生命周期 heartbeat |
| `tests/test_duckdb_store.py` | 存储层 touch/reclaim 回归测试 |
| `tests/test_round_feedback.py` | processor heartbeat 回归测试 |

### Follow-up 验收标准

1. 正在运行中的 sample 会周期性刷新 `processing_updated_at`
2. 处理时间超过 `stale_minutes` 的正常任务不会被后续 reclaim 误回收
3. 无 heartbeat 的旧 `processing` 样本仍可按原语义被 reclaim
4. 相关回归测试通过：

```bash
.venv/bin/pytest tests/test_duckdb_store.py tests/test_round_feedback.py -q
```

### Follow-up 建议提交

**提交消息**:

```text
fix: heartbeat active round-feedback samples
```

### 2.1 调度模式默认值收口

**现状**: `--user` 已经在 `docker_run_incremental_pipeline.sh:104-105` 传入；loop 模式已存在且能跑（`scheduler_loop.sh`）。真正未收口的是**默认值**——`entrypoint.sh:10` 默认 `SCHEDULER_MODE=cron`，`docker_run_incremental_pipeline.sh:15` 也是 `cron`。

**方案**: 默认值切为 `loop`。cron 路径保留不删（可能有依赖方），但降级为非默认。

**建议文件范围**:
- `docker/entrypoint.sh`
- `scripts/docker_run_incremental_pipeline.sh`
- 如有默认值透传，补看 `scripts/docker_run_incremental_pipeline_guarded.sh`

**完成标准**:
- 不显式传 `SCHEDULER_MODE` 时，容器默认走 loop 调度
- 显式传 `SCHEDULER_MODE=cron` 时，旧路径仍可用
- README / runtime 文档后续单独更新，不挟带进本 commit

### 2.2 `scheduler_loop.sh` 信号处理加固

增加 SIGTERM/SIGINT trap，子脚本失败不终止循环（`|| true`）。

**建议文件范围**:
- `docker/scheduler_loop.sh`

**完成标准**:
- 调度循环收到 SIGTERM/SIGINT 时可干净退出
- 单次 due-check / pipeline-run 失败不会把整个 loop 打死
- 不引入新的后台孤儿进程

### 2.3 日志管理统一

- 新建 `logging_config.py` 集中配置
- 增加日志轮转（`RotatingFileHandler`）
- pipeline log 自动清理（保留最近 N 个 run）
- `_tail_lines` seek-based 替代全量读入

**建议文件范围**:
- `claw_data_filter/logging_config.py`
- CLI / pipeline 入口文件
- `claw_data_filter/web/services/database_access.py`
- 必要时补 `report_exporter.py` / Web 读取日志的相关位置

**完成标准**:
- CLI 与 pipeline 至少共享同一套 formatter / handler 初始化逻辑
- Web 端 tail 日志不再整文件读入内存
- 引入轮转或清理机制后，不破坏当前 Web 的日志查看路径

### 2.4 死代码与代码重复清理

- 删除 `count_expected_judgments`、`detect_empty_response`、`_first_non_empty`、`JSON_FIELDS`、未使用的 `build_where_clause`
- 合并 `_build_tool_stats` 双份实现
- 提取 `ResponseProgressJudgmentProcessor` / `UserSatisfiedJudgmentProcessor` 公共基类

**建议文件范围**:
- `claw_data_filter/models/sample.py`
- `claw_data_filter/empty_response.py`
- `claw_data_filter/filters/query.py`
- `claw_data_filter/exporters/unified_exporter.py`
- `claw_data_filter/processors/round_feedback.py`
- `claw_data_filter/storage/duckdb_store.py`

**完成标准**:
- 只删除已确认无 production 调用的死代码
- 重复代码合并不改变对外行为和统计口径
- 若牵涉测试改写，保持提交聚焦在“维护性收口”，不额外修改业务语义

### 2.5 错误处理加固

- `duckdb_store.py` init_schema: bare `except:` → `except Exception:`，包裹事务
- `session_merge.py` `ensure_session_merge_schema`: 至少 log warning
- `report_exporter.py`: `json.loads` 加 try/except，`export_report` 加 `mkdir`

**建议文件范围**:
- `claw_data_filter/storage/duckdb_store.py`
- `claw_data_filter/session_merge.py`
- `claw_data_filter/exporters/report_exporter.py`

**完成标准**:
- schema migration 失败不会留下静默半迁移状态
- session merge 和 report export 的异常至少可观测
- 不在这一批混入结构性重构

### 2.6 Docker 安全加固

- `HEALTHCHECK` 指令
- base image digest 锁定
- `.dockerignore`

**建议文件范围**:
- `Dockerfile`
- `.dockerignore`

**完成标准**:
- 不改变当前容器运行用户模型（仍由 `--user` 主导）
- `HEALTHCHECK` 与当前 streamlit 启动方式兼容
- `.dockerignore` 不误排除构建所需源码 / 配置

### 2.7 `entrypoint.sh` 清理

- TOML 解析增加错误处理
- 移除 HOME 覆写 hack

**建议文件范围**:
- `docker/entrypoint.sh`

**完成标准**:
- 配置解析失败时入口脚本清晰退出
- 不再覆写 `HOME`，同时保留当前 streamlit 运行所需目录可写性

### Round 2 建议执行批次

#### Batch A: Docker 默认行为收口

范围：2.1 + 2.2 + 2.7

理由：
- 这三项共同决定容器启动后的默认调度行为和退出语义
- 都属于 docker/runtime 主路径，不应与日志统一或代码清理混提

建议验证：

```bash
.venv/bin/pytest tests/test_pipeline_service.py -q
```

以及一次容器级 smoke check：默认不传 `SCHEDULER_MODE` 时 loop 生效，停止容器时 scheduler 不残留。

#### Batch B: 日志统一与 Web tail 优化

范围：2.3

理由：
- 这是独立的观测性改进面
- 一旦混入 Docker 默认值切换，定位回归会变难

建议验证：

```bash
.venv/bin/pytest tests/test_web_database_access.py tests/test_pipeline_service.py -q
```

#### Batch C: 维护性收口

范围：2.4 + 2.5 + 2.6

理由：
- 这些项的共同目标是减少技术债和提升健壮性
- 其风险主要在“是否改变了原有行为”，适合在 Docker/runtime 稳定后单开一批

建议验证：

```bash
.venv/bin/pytest tests/ -q
```

必要时再补相关窄测试集。

---

## Round 2 Post-Review Follow-up

在 Round 2 提交完成后，复审发现仍有 2 个需要收口的问题：

### F2. Docker runtime 的 Streamlit writable-home fallback 实际无效

**涉及提交**:
- `7621793 fix: default to loop scheduler and harden Docker runtime`

**现状**:
- `entrypoint.sh` 不再覆写 `HOME`，转而创建并导出 `STREAMLIT_HOME`
- 当前环境中的 Streamlit 仍通过 `Path.home()` 解析 `~/.streamlit`，并不识别 `STREAMLIT_HOME`

**问题**:
- 当容器内 `HOME` 不可写时，当前实现会再次回到“Streamlit 试图写入不可写 `~/.streamlit`”的旧问题
- 也就是说，这一提交在受限 HOME / root_squash / 非 root 场景下存在功能回归风险

**修复原则**:
- 不恢复脚本级全局 `HOME` hack
- 只对最终启动的 Streamlit 进程提供一个确定可写的 `HOME`
- 其他子进程在 entrypoint 执行期仍尽量保留原始 `HOME`

**建议修复方案**:

#### F2.1 为 Streamlit 子进程单独计算有效 HOME

在 `entrypoint.sh` 中区分：
- `EFFECTIVE_HOME`：当前 shell / pipeline 使用的原始 `HOME`
- `STREAMLIT_EFFECTIVE_HOME`：仅供最终 `exec streamlit` 使用的可写 HOME

逻辑建议：
- 若当前 `HOME` 存在且可写，则 `STREAMLIT_EFFECTIVE_HOME="${HOME}"`
- 否则令 `STREAMLIT_EFFECTIVE_HOME="${PIPELINE_DIRS[2]}"`
- 显式创建 `"${STREAMLIT_EFFECTIVE_HOME}/.streamlit"`

#### F2.2 仅在 exec streamlit 时覆写 HOME

建议改成：

```bash
HOME="${STREAMLIT_EFFECTIVE_HOME}" exec streamlit run ...
```

这样：
- Streamlit 会继续通过 `Path.home()` 落到可写目录
- entrypoint 前半段不会把全局 `HOME` 永久改掉
- 语义上比导出一个 Streamlit 不识别的 `STREAMLIT_HOME` 更准确

#### F2.3 验收重点

- `HOME` 可写时，仍使用原始 `HOME`
- `HOME` 不可写时，Streamlit 成功落到 work_dir 下的 `.streamlit`
- 默认 loop scheduler 行为不受影响

**建议文件范围**:
- `docker/entrypoint.sh`

### F3. `session_merge` schema 变更异常仍然被整体吞掉

**涉及提交**:
- `1bc9404 fix: harden error handling in schema migration and report export`

**现状**:
- `ensure_session_merge_schema` 将所有 `ALTER TABLE` 异常统一捕获
- 当前仅记录 debug 日志：`session_merge schema column may already exist`

**问题**:
- 如果失败原因不是“列已存在”，而是权限、磁盘、数据库损坏或 SQL 语法问题，当前实现仍会继续执行
- 这会把“已存在”与“真实失败”混在一起，不能算真正的错误处理加固

**修复原则**:
- 不再把异常当正常控制流
- 只对“列已存在”这一种可接受情况做跳过
- 其他异常必须原样抛出，阻止 session merge 在半可用 schema 上继续运行

**建议修复方案**:

#### F3.1 用 schema introspection 代替异常分支

推荐优先方案：
- 在执行 `ALTER TABLE ... ADD COLUMN ...` 前，先查询现有列集合
- 只有缺列时才执行 `ALTER TABLE`

例如：

```python
existing_columns = {
    row[1]
    for row in conn.execute("PRAGMA table_info('samples')").fetchall()
}
if "session_merge_status" not in existing_columns:
    conn.execute("ALTER TABLE samples ADD COLUMN session_merge_status TEXT")
```

优点：
- 逻辑直接
- 不需要依赖 DuckDB 异常消息文本
- 真异常自然向上抛出

#### F3.2 若暂时保留 try/except，则只放过 duplicate-column

如果为了最小改动暂不做 introspection，也至少要：
- 精确判断异常是否为“column already exists”
- 否则重新抛出异常

但这只是次优方案，不建议长期保留。

#### F3.3 验收重点

- 缺列时能正常补列
- 已有列时平稳跳过
- 非 duplicate-column 错误不会被吞掉

**建议文件范围**:
- `claw_data_filter/session_merge.py`
- 如需补窄测试，再纳入对应测试文件

### Round 2 Follow-up 建议执行顺序

#### Follow-up A: 修正 Streamlit writable-home fallback

**提交消息**:

```text
fix: restore writable home fallback for streamlit
```

**文件范围**:
- `docker/entrypoint.sh`

**建议验证**:
- 容器启动 smoke check
- 不可写 HOME 场景下 Web 正常启动

#### Follow-up B: 只放过 duplicate-column 的 session_merge schema 迁移

**提交消息**:

```text
fix: stop swallowing session-merge schema errors
```

**文件范围**:
- `claw_data_filter/session_merge.py`
- 必要时补对应测试文件

**建议验证**:

```bash
.venv/bin/pytest tests/ -q
```

如可单独补测试，更推荐加一个窄测试覆盖：
- 列已存在时平稳通过
- 模拟真实异常时应抛出

---

## 建议提交拆分

本节补足 Git 执行层，目标是让 Round 1 可以按小步、可验证的方式落地，避免最后重新混成一个大提交。

原则：
- 先提交流程正确性和失败重试，再提交 processing 回收闭环，最后提交局部 correctness / dead code 清理
- 每个 commit 都应有单独的可验证点；若某项必须跨多个文件才能形成闭环，则作为同一 commit 提交
- Round 2 不混入本轮提交计划，待 Round 1 全部验证通过后另开 fix plan 或补充章节

### Commit 1: 调度失败后允许重试

**提交消息**:

```text
fix: retry scheduled pipeline runs after failures
```

**文件范围**:
- `docker/run_pipeline_if_due.sh`

**包含内容**:
- 仅修正状态文件写入时序：`pipeline-run` 成功后才更新时间戳

**提交后验证**:

```bash
.venv/bin/pytest tests/test_pipeline_service.py -q
```

如无直接覆盖该 shell 路径的测试，则至少执行一次定向脚本/容器内手工验证，确认 pipeline 失败时状态文件不更新。

### Commit 2: 修复 processing 状态卡死与 pre-merge 提示

**提交消息**:

```text
fix: recover stale processing samples before round feedback
```

**文件范围**:
- `claw_data_filter/storage/duckdb_store.py`
- `claw_data_filter/cli.py`
- `claw_data_filter/pipeline/service.py`

**包含内容**:
- samples 侧 stale `processing` 回收
- pipeline source file 侧 stale `processing` 回收
- CLI `round-feedback` 的 `--reclaim-stale / --no-reclaim-stale`
- CLI 在 pre-merge 盲区下打印显式提示
- pipeline `_run_round_feedback_for_samples` 在领取前显式执行回收

**提交后验证**:

```bash
.venv/bin/pytest tests/test_cli.py tests/test_pipeline_service.py -q
```

如测试缺口存在，补一个最小回归测试优先于手工验证；至少应覆盖：
- stale sample 可被回收
- stale source file 可重新 claim
- `session_merge_keep IS NULL` 的 pending/failed 样本会触发提示

### Commit 3: 修正 detail 页 0 值语义并清理死代码入口

**提交消息**:

```text
fix: preserve zero counts in detail view
```

**文件范围**:
- `claw_data_filter/web/services/detail_builder.py`
- `claw_data_filter/filters/query.py`

**包含内容**:
- `expected_*_judgment_count` 改为显式 `is not None` 判断
- 删除 `add_progress_score_filter` 死代码入口

**提交后验证**:

```bash
.venv/bin/pytest tests/test_web_detail_builder.py tests/test_cli.py tests/test_exporters.py -q
```

若 `add_progress_score_filter` 仅有测试间接覆盖，则需同步修正相关测试断言。

### Round 1 收口验证

以上 3 个 commit 全部完成后，再执行一次 Round 1 总体验收：

```bash
.venv/bin/pytest tests/ -q
```

核心测试集至少应再次通过：

```bash
.venv/bin/pytest tests/test_cli.py tests/test_pipeline_service.py tests/test_unisound_export.py tests/test_exporters.py -q
```

### Round 2 建议提交计划

#### Commit A: 切换默认调度到 loop 并加固 scheduler 退出行为

**提交消息**:

```text
fix: default docker scheduler to loop mode
```

**文件范围**:
- `docker/entrypoint.sh`
- `docker/scheduler_loop.sh`
- `scripts/docker_run_incremental_pipeline.sh`
- 如确有默认值透传改动，再纳入 `scripts/docker_run_incremental_pipeline_guarded.sh`

**包含内容**:
- 默认 `SCHEDULER_MODE` 从 `cron` 切到 `loop`
- `scheduler_loop.sh` 增加信号处理与失败后续跑能力
- `entrypoint.sh` 做最小必要的启动/退出清理

#### Commit B: 统一 runtime logging 并优化 Web 日志读取

**提交消息**:

```text
refactor: centralize runtime logging
```

**文件范围**:
- `claw_data_filter/logging_config.py`
- CLI / pipeline 入口文件
- `claw_data_filter/web/services/database_access.py`
- 必要的相关测试文件

**包含内容**:
- 引入集中 logging 配置
- 统一主要 runtime 路径的日志初始化
- 优化 Web 端 tail 读取，避免整文件读入

#### Commit C: 清理维护性债务并补强错误处理

**提交消息**:

```text
refactor: reduce round-feedback maintenance debt
```

**文件范围**:
- `claw_data_filter/models/sample.py`
- `claw_data_filter/empty_response.py`
- `claw_data_filter/filters/query.py`
- `claw_data_filter/exporters/unified_exporter.py`
- `claw_data_filter/processors/round_feedback.py`
- `claw_data_filter/storage/duckdb_store.py`
- `claw_data_filter/session_merge.py`
- `claw_data_filter/exporters/report_exporter.py`
- `Dockerfile`
- `.dockerignore`

**包含内容**:
- 删除已确认死代码
- 合并关键重复实现
- 收紧错误处理与导出容错
- 做最小 Docker build hygiene 收口

如 Commit C 变大，应继续拆成两个提交：
- `refactor: remove dead code in round feedback stack`
- `fix: harden schema and report export error handling`

---

## 不纳入修复计划的项目（说明理由）

| 项 | 理由 |
|----|------|
| `session_merge.py:405` 直接 `duckdb.connect` | 连接管理一致性隐患，非数据损坏风险；不影响当前功能正确性，Round 2 或以后整理 |
| 列位置索引改为命名访问 | 维护性改善项，不影响功能；已知耦合点注释清楚即可，不必为此重构 |
| 删除 cron 调度模式 | Round 2 只改默认值，不删代码（可能有依赖） |
| Pydantic 字段 `Field(ge=...)` 约束 | 参数校验改善项，不影响当前功能 |
| `Dockerfile` 增加 `USER` 指令 | `--user` 已在启动脚本传参，Dockerfile 层面改动可延后 |
