# UniRouter 解压同步与全量重建计划

> 目标: 校验 manydata/unirouter 与 manydata/unirouter_uncompress 的同步状态，补齐缺失解压，汇总所有 items.jsonl，重建新 DuckDB，并在 5000 端口切换 Web 到新库。

## 当前执行状态

更新时间: 2026-04-11

### 已确认现状

- 源 tar 目录: `/kanas/nlp/liuchang/manydata/unirouter`
- 解压目录: `/kanas/nlp/liuchang/manydata/unirouter_uncompress`
- 当前共发现 15 份 `.tar` 包。
- 解压目录当前只同步到 `2026-04-07`，缺少以下 3 份增量包对应的 `items.jsonl`:
  - `request-logs-cmn9pe6bg00ksmoa266ms55rp-incremental-2026-04-08T20-00-10-654Z.tar`
  - `request-logs-cmn9pe6bg00ksmoa266ms55rp-incremental-2026-04-09T20-00-29-855Z.tar`
  - `request-logs-cmn9pe6bg00ksmoa266ms55rp-incremental-2026-04-10T20-00-10-370Z.tar`
- `scripts/run_import_to_stats.sh` 已在本轮更新为临时 DB + 原子替换的安全写库模式。
- `scripts/rebuild_unirouter_dataset.sh` 已新增，可执行 tar 同步、`items.jsonl` 汇总与全量重建。

### 本次计划

1. 对齐 scripts
   - 将 `scripts/run_import_to_stats.sh` 更新为安全写库模式。
   - 增加面向 unirouter tar/uncompress 的同步与重建脚本。

2. 补齐解压
   - 仅对缺失的 tar 执行解压。
   - 确保每个目标目录最终存在 `items.jsonl`。

3. 重建新库
   - 收集 `unirouter_uncompress` 下全部 `items.jsonl`。
   - 生成新的合并输入文件。
   - 使用指定 LLM 端点执行 import -> pressure-test -> session-merge -> round-feedback -> stats。

4. Web 切换
   - 在 5000 端口将 Streamlit 切换到新生成的 DuckDB。
   - 确认运行中的 `DB_PATH` 指向新库。

### 执行记录

- 2026-04-11 初始盘点: 源 tar 15 份，缺失解压 3 份，尚未开始重建。
- 2026-04-11 scripts 对齐完成: `run_import_to_stats.sh` 已安全化，`rebuild_unirouter_dataset.sh` 已通过 bash 语法检查。
- 2026-04-11 解压同步完成: 缺失的 `2026-04-08`、`2026-04-09`、`2026-04-10` 三份增量包均已补齐到 `unirouter_uncompress`，当前 15 份 tar 均可落到对应 `items.jsonl`。
- 2026-04-11 汇总完成: `data/unirouter_refresh/items_merged.jsonl` 已完成生成，体积约 23G。
- 2026-04-11 preflight 完成: 256 并发下真实端点已通过 pressure-test，当前已进入 merged JSONL 的 import 阶段。
- 2026-04-11 等待当前长流程期间，已并行准备下一轮优化路径: import 改为“多进程解析 + DuckDB 单写者批量插入”，`session-merge` 默认 worker 数提升为按 CPU 自适应上限配置，以避免与 DuckDB 单写者能力冲突。
- 2026-04-11 已进一步把默认 CPU 占用策略改为共享机约 70% 预算，并补充 import / session-merge / pipeline shell 的阶段化与进度日志，后续可直接通过日志判断当前阶段与推进速度。
- 2026-04-11 已使用新流程对 `data/pipeline_e2e/items_100.jsonl` 完成 100 条全链路验证: preflight / import / session-merge / round-feedback / stats / 双导出全部成功。结果为 `total_samples=100`、`26 success / 0 failures`、`avg_response_progress_rate=0.1722`、`avg_response_regress_rate=0.0971`、`avg_user_satisfied_rate=0.0114`、`avg_user_negative_feedback_rate=0.1809`、`error_count=0`，产物已写回 `data/pipeline_e2e/e2e_100_progress.duckdb` 与 `data/pipeline_e2e/validation_progress/`。
- 2026-04-11 用户要求放弃旧的长导入结果，不再沿用 `unirouter_refresh.duckdb.tmp.38724`；重跑将直接从已生成的 `data/unirouter_refresh/items_merged.jsonl` 开始，使用新并行 import / session-merge 路径，并将全量 stdout/stderr 落到独立日志文件供监控。
- 2026-04-11 已停止并清理旧长流程及其临时库 `data/unirouter_refresh/unirouter_refresh.duckdb.tmp.38724*`，保留 `data/unirouter_refresh/items_merged.jsonl` 作为新的重跑输入。
- 2026-04-11 已从 merged JSONL 重新启动新流程，运行日志写入 `data/unirouter_refresh/logs/rebuild_20260411_151055.log`，当前终端会话 ID 为 `eea9d341-d5b3-4155-9c90-05b7dc26aa07`。
- 2026-04-11 重跑当前状态: preflight pressure-test 已通过，仍处于 import 阶段；最新观测进度为 `chunks=520`、`processed_lines=33280`、`imported=33280`、`errors=0`，输入文件总行数为 `43728`，约完成 `76.1%`。后半段吞吐较前段放缓，但日志与写库仍持续推进；临时库 `data/unirouter_refresh/unirouter_refresh.duckdb.tmp.49501` 已增长至约 35.4 GB，最新写入时间为 15:57:22。
- 2026-04-13 vLLM 端点迁移恢复: 原 round-feedback 运行因旧端点故障中断后，端点已切换至 `http://182.242.159.76:31866/v1`。先前一次恢复误用了 `llm_concurrency=256`，已主动停止；现已按更保守的 `MAX_CONCURRENCY=96`、`BATCH_SIZE=96` 重新启动完整恢复流程，跳过 preflight pressure-test，新日志为 `data/unirouter_refresh/logs/rebuild_20260413_102531_resume_with_96.log`，当前重新从 import 阶段开始，临时库为 `data/unirouter_refresh/unirouter_refresh.duckdb.tmp.19082`。
- 2026-04-13 保库停机排障: 因怀疑存在个别 case 反复打爆 vLLM，已在不删除临时库的前提下安全停止当前恢复任务。做法是先冻结父脚本与 tee，停止 round-feedback 子进程，再将 `data/unirouter_refresh/unirouter_refresh.duckdb.tmp.19082` 与对应 WAL 改名保留为 `data/unirouter_refresh/unirouter_refresh_preserved_after_vllm_crash_20260413_1702.duckdb` 与 `.wal`，最后用不会触发 cleanup trap 的方式结束父脚本。当前已无活跃的重建流水线进程。
- 2026-04-13 保留库盘点: 保留库中 `session_merge_keep=TRUE` 总量为 `6009`，其中 `processing_status=completed` 为 `4778`，`pending` 为 `1209`，`processing` 为 `22`；待补跑的 round-feedback 样本共 `1231`。后续恢复时应直接基于保留库补跑 round-feedback 与 stats，而不是重新执行 import / session-merge。

### 2026-04-13 急救补丁计划（仅规划，暂未改代码）

目标:
- 对 round-feedback 的 prompt 组装增加硬性止血规则，优先避免超长 case 继续打爆 vLLM。
- 保持补丁范围尽量小，仅触及 round-feedback 组装/判定链路与必要测试，不改 import / session-merge / DuckDB schema。

拟实施策略:
1. `episode` 轮次限制
   - 在 `TurnContextBuilder.extract_episode_contexts()` 内，对单个 episode 可纳入 prompt 的执行链设置硬上限，先按“最多保留 10 轮”规划。
   - 倾向保留起始用户请求 + 最近若干轮执行链，而不是单纯保留最前面的 10 轮，避免丢掉尾部真正决定满意度的内容。

2. `prompt` 总长闸门
   - 在 `build_response_progress_prompt()` / `build_user_satisfied_prompt()` 产出最终 prompt 后，增加统一总长度检查。
   - 先按字符数 `100000` 作为硬上限规划；后续如需更精细，再考虑接 tokenizer 或 token 预算器。

3. 截断后仍超长则直接失败
   - 对经过局部截断、轮次裁剪后仍超过总长度阈值的样本，不再继续向 LLM 发请求。
   - 直接在样本结果中落失败，并通过 `tool_stats.error_reason` 区分原因，例如 `prompt_too_long_after_truncation`。

4. 快速一致性补救
   - `response_progress` 路径中的 `feedback_payload` 当前可能保留全文；急救补丁中应使其与 `execution_trace` 一样走截断逻辑，避免 tool result 被重复全文拼入 prompt。
   - `user_satisfied` 路径中的 `execution_trace` 应同时受“单条片段限长 + 轮次上限 + 总 prompt 上限”三层控制。

预计波及文件:
- `claw_data_filter/processors/round_feedback.py`
  - `TurnContextBuilder.extract_episode_contexts()`
  - `TurnContextBuilder.build_response_progress_prompt()`
  - `TurnContextBuilder.build_user_satisfied_prompt()`
  - 可能新增统一的 prompt 裁剪 / 长度检查辅助函数
- `tests/test_round_feedback.py`
  - 新增 episode 轮次裁剪测试
  - 新增超长 prompt 直接 fail 的测试
  - 更新 `feedback_payload` 截断相关断言
- `tests/test_duckdb_store.py`
  - 复用现有 `mark_sample_processing_failed()` 断言模式，补一个 error_reason 可区分的验证

验证计划:
1. 先用单元测试覆盖以下场景:
   - 普通样本不受影响
   - 超长 episode 被裁到 10 轮以内
   - 超长 `feedback_payload` 不再全文进入 prompt
   - 最终 prompt 超过 `100000` 字符时直接标记失败，并带上明确原因
2. 再基于保留库中的 22 条重型 `processing` 样本做针对性抽样验证，观察是否仍会形成超长 prompt。
3. 若急救补丁有效，再基于保留库补跑剩余 `1231` 条 round-feedback，之后再执行 `stats`。
