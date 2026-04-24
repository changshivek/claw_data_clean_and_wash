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
def reclaim_stale_processing_samples(self, stale_minutes: int = 30) -> int:
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

### 2.2 `scheduler_loop.sh` 信号处理加固

增加 SIGTERM/SIGINT trap，子脚本失败不终止循环（`|| true`）。

### 2.3 日志管理统一

- 新建 `logging_config.py` 集中配置
- 增加日志轮转（`RotatingFileHandler`）
- pipeline log 自动清理（保留最近 N 个 run）
- `_tail_lines` seek-based 替代全量读入

### 2.4 死代码与代码重复清理

- 删除 `count_expected_judgments`、`detect_empty_response`、`_first_non_empty`、`JSON_FIELDS`、未使用的 `build_where_clause`
- 合并 `_build_tool_stats` 双份实现
- 提取 `ResponseProgressJudgmentProcessor` / `UserSatisfiedJudgmentProcessor` 公共基类

### 2.5 错误处理加固

- `duckdb_store.py` init_schema: bare `except:` → `except Exception:`，包裹事务
- `session_merge.py` `ensure_session_merge_schema`: 至少 log warning
- `report_exporter.py`: `json.loads` 加 try/except，`export_report` 加 `mkdir`

### 2.6 Docker 安全加固

- `HEALTHCHECK` 指令
- base image digest 锁定
- `.dockerignore`

### 2.7 `entrypoint.sh` 清理

- TOML 解析增加错误处理
- 移除 HOME 覆写 hack

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

### Round 2 的提交原则

Round 2 暂不在本节展开具体 commit 列表，只定义边界：
- 调度默认值收口、`scheduler_loop.sh` 加固、`entrypoint.sh` 清理应作为 Docker/runtime 主题提交
- 日志统一应单独成批，不与 Docker 默认值切换混提
- 死代码/重复代码清理和错误处理加固应拆成维护性提交，不挟带功能行为变化

---

## 不纳入修复计划的项目（说明理由）

| 项 | 理由 |
|----|------|
| `session_merge.py:405` 直接 `duckdb.connect` | 连接管理一致性隐患，非数据损坏风险；不影响当前功能正确性，Round 2 或以后整理 |
| 列位置索引改为命名访问 | 维护性改善项，不影响功能；已知耦合点注释清楚即可，不必为此重构 |
| 删除 cron 调度模式 | Round 2 只改默认值，不删代码（可能有依赖） |
| Pydantic 字段 `Field(ge=...)` 约束 | 参数校验改善项，不影响当前功能 |
| `Dockerfile` 增加 `USER` 指令 | `--user` 已在启动脚本传参，Dockerfile 层面改动可延后 |
