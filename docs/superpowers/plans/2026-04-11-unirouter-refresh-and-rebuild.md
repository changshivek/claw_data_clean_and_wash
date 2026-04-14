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

### 2026-04-13 急救补丁执行进展

- 已完成代码实现:
   - `claw_data_filter/processors/round_feedback.py` 已加入 `episode_round_limit=10` 的执行链裁剪，保留最近 10 轮 assistant/tool 执行链进入 `user_satisfied` prompt。
   - `response_progress` prompt 的紧邻反馈块已改为走统一截断渲染，不再把原始超长全文直接拼入 prompt。
   - `RoundFeedbackProcessor` 已加入 `prompt_char_limit=100000` 的硬闸门；若裁剪后仍超长，会在发起 LLM 请求前直接失败，并通过 `error_reason` 区分为 `response_progress_prompt_too_long_after_truncation` 或 `user_satisfied_prompt_too_long_after_truncation`。

- 已完成测试验证:
   - 定向测试 `tests/test_round_feedback.py`、`tests/test_duckdb_store.py` 已通过，当前为 `39 passed`。
   - 已补充覆盖 episode 最近 10 轮裁剪、反馈块截断、超预算 fail-fast、失败原因入库等路径。

- 已完成保留库离线核验:
   - 针对保留库中原先卡住的 `22` 条 `processing` 重型样本，使用新逻辑离线构造 prompt 并计算长度，结果 `over_limit_rows=0`。
   - 该批样本中观测到的最大 `response_progress` prompt 长度约 `5237` 字符，最大 `user_satisfied` prompt 长度约 `10046` 字符，均明显低于 `100000` 字符硬上限。

- 下一步:
   - 直接基于保留库重置残留 `processing` 状态并补跑剩余 `1231` 条 round-feedback。
   - round-feedback 跑完后继续执行 `stats`，再评估是否需要进入更系统的 token 预算方案开发。

### 2026-04-13 保库停机快照（vLLM 再次异常后）

- 已执行保库停止:
   - 当前补跑进程已停止，保留库仍为 `data/unirouter_refresh/unirouter_refresh_preserved_after_vllm_crash_20260413_1702.duckdb`。
   - 本轮补跑日志停在 `data/unirouter_refresh/logs/preserved_resume_retry_20260413_181527.log`，最后一段日志再次出现连续 `All connection attempts failed`，说明停止原因仍是 vLLM 端点异常，而不是 prompt 超长保护触发。

- 停机时保留库状态:
   - `session_merge_keep=TRUE` 范围内当前状态为: `completed=5031`、`pending=927`、`processing=51`。
   - 仍需继续处理的 kept 样本总数为 `978`。
   - `completed` 中当前 `has_error=false` 为 `5009`，`has_error=true` 为 `22`。
   - `completed && has_error=true` 的原因分布为: `no_messages=20`、`error_reason为空=2`。
   - 当前 `failed` 状态样本为 `0`。

- 当前统计快照（kept 样本、且已有 `tool_stats`）:
   - `avg_response_progress_rate=0.46668750497474204`
   - `avg_response_regress_rate=0.0952991981930318`
   - `avg_user_satisfied_rate=0.12069702603775835`
   - `avg_user_negative_feedback_rate=0.3468783947198448`
   - `has_error` 样本数为 `50`

- 后续续跑建议:
   - 等 vLLM 新镜像更新完毕后，先确认端点稳定，再将残留 `processing=51` 复位为 `pending` 后继续补跑。
   - 视新镜像恢复情况，再决定是否把 `completed && has_error=true && error_reason为空` 的 2 条样本一起复位重跑；`no_messages=20` 可继续保留，不必重跑。

### 2026-04-14 超时 / 重试策略排查与修正

- 排查结论:
   - 当前 `round-feedback` 并不是因为 `LLM_TIMEOUT=60s` 这一项单独过短而立刻失败，主要问题在于失败后的回退等待过短。
   - 原实现里，单次调用失败后只按 `1s -> 2s` 退避，且 `Config.max_retries` 没有真正传到 `RoundFeedbackProcessor`，导致运行时即便配置了更保守的重试策略，实际仍按处理器默认值执行。
   - 在 vLLM 出现 `kvcache` 紧张或瞬时抖动时，这种短退避会让同一批请求迅速回打，容易把服务从“抖动”放大为“雪崩”。

- 已完成代码修正:
   - `config.max_retries` 现已接入 `round-feedback` 实际处理链路。
   - 新增可配置退避参数:
      - `LLM_RETRY_BASE_DELAY`，默认 `5s`
      - `LLM_RETRY_MAX_DELAY`，默认 `30s`
   - `ResponseProgressJudgmentProcessor` / `UserSatisfiedJudgmentProcessor` 的重试等待已改为指数退避上限模式，而不是原来的 `1s/2s`。
   - `AsyncLLMClient` 现在会把 vLLM 的真实 HTTP 状态码和响应体摘要带进异常消息；例如后续若返回 `503` 或明确的 `kvcache exhausted` 文本，日志里会直接看到，不再只剩空白 warning。

- 验证情况:
   - 定向测试 `tests/test_async_client.py`、`tests/test_config.py`、`tests/test_round_feedback.py` 已通过，结果为 `26 passed`。

### 2026-04-14 32 并发补跑失败快照（vLLM 再次掉线）

- 运行结果:
   - 本轮 `32` 并发补跑在执行中后段再次遇到 `All connection attempts failed`，说明 vLLM 端点在运行过程中掉线。
   - 失败窗口内，处理器按新退避策略走到了 `Attempt 4/4`，日志中能直接看到 `ConnectError contacting http://182.242.159.76:31866/v1/chat/completions`，说明新的错误可观测性已经生效。
   - `round-feedback` 进程随后在写回失败样本时触发 DuckDB 内部致命错误并退出，不是正常收尾退出。

- 新暴露的问题:
   - 终端末尾出现 DuckDB `PRIMARY KEY or UNIQUE constraint violation`，冲突键为 `resp:e53b29a761f6a6e142b92038f59917c68fb8252b508b62c02bb99df7a675c0a2:0`。
   - 该错误发生在 `Replacing round feedback results` 之后、事务提交阶段，说明当前除了 vLLM 稳定性外，还需要继续排查 round-feedback 失败样本回写时的重复插入/事务回滚一致性问题。

- 当前保留库状态:
   - `session_merge_keep=TRUE` 范围内当前状态为: `completed=5091`、`pending=909`、`processing=9`。
   - 相比 32 并发启动前的 `completed=5068 / pending=941`，本轮至少净推进了 `23` 条 kept 样本。
   - `has_error=true` 当前为 `26` 条；原因分布为 `no_messages=20`、`error_reason为空=6`。

- stats 快照:
   - 虽然 `round-feedback` 失败退出，但命令链路里的 `stats` 仍已执行完成。
   - 当前输出为:
      - `avg_response_progress_rate=0.4653583076811547`
      - `avg_response_regress_rate=0.09449400479455392`
      - `avg_user_satisfied_rate=0.11969928708759765`
      - `avg_user_negative_feedback_rate=0.34632803248644917`
      - `error_count=26`
      - `total_samples=43728`

- 本次顺手改进:
   - CLI 日志初始化已下调第三方 HTTP 客户端日志级别，后续 `httpx` / `httpcore` 的 `200 OK` 信息不会再刷满日志，只保留 `WARNING` 及以上事件。

### 2026-04-14 round-feedback 库内结果排查汇总

- 当前 kept 样本状态:
   - 基于保留库 `data/unirouter_refresh/unirouter_refresh_preserved_after_vllm_crash_20260413_1702.duckdb`，`session_merge_keep=TRUE` 范围内当前状态为: `completed=5091`、`pending=909`、`processing=9`。
   - 当前并不是“大量样本都已经被 LLM fail 打坏”，而是少数超大样本上聚集了很多 judgment 级失败。

- response progress judgment 统计:
   - 总 judgment 数: `439535`
   - `llm_error=true` 数: `10549`，占比约 `2.400%`
   - 标签分布:
      - `yes=383611`
      - `no=44943`
      - `uncertain=432`
      - `null=10549`
   - `null` 与 `llm_error=true` 对齐，说明这些空标签基本就是 LLM 调用失败后留下的 judgment 空洞。

- user satisfy judgment 统计:
   - 总 judgment 数: `180057`
   - `llm_error=true` 数: `4552`，占比约 `2.528%`
   - 标签分布:
      - `no=163651`
      - `uncertain=8246`
      - `yes=3602`
      - `neutral=6`
      - `null=4552`
   - 同样，`null` 基本对应 LLM 调用失败而未拿到有效判断。

- 样本级影响面:
   - `completed` kept 样本总数为 `5091`。
   - 至少包含一次 response-progress LLM fail 的样本数为 `17`。
   - 至少包含一次 user-satisfied LLM fail 的样本数为 `19`。
   - 任一维度出现过 LLM fail 的样本总数为 `19`，占全部 `completed` kept 样本约 `0.373%`。
   - 某一维度 judgment 全部失败的样本数为 `12`，说明问题集中在极少量超大样本上，而不是均匀扩散到大多数 completed 样本。

- `has_error` 与 LLM fail 的关系:
   - 当前 `has_error=true` 的 kept completed 样本总数为 `26`，且全部仍为 `processing_status='completed'`。
   - 原因分布为:
      - `no_messages=20`
      - `error_reason为空=6`
   - 这说明样本级 `has_error` 主要仍是老的 `no_messages` 问题，并不是本轮 LLM fail 大面积升级成样本级错误。
   - 但确实有 `6` 条 `error_reason为空` 的 completed 样本与 judgment 级 LLM fail 同时出现，属于当前最值得重点复查的一批异常样本。

- 重点异常样本特征:
   - judgment 级失败高度集中在少数超大样本上，例如部分样本出现 `response_llm_errors` 超过 `700`、`episode_llm_errors` 超过 `300`，且错误率可达 `100%`。
   - 目前观察到的最重样本包括:
      - `96b7a3c3283abc3a851985c3485caa745dd1bda973cce271f5febb821135756f`
      - `a3617ebe229fe56cf594b2f2b7c97c6777b3bcc968eb5c7ec88dce13cdad41cf`
      - `c9e72aa583f8f59cf4f005638703a4322e0c55ecacd4f315e34bb5e6fa83d5ed`
      - `48fd315b595cdc6169db2298e5f7e656b615ee90e3639eec23b5a57f573a083a`
   - 这些样本更像是后续修复和定向重跑时的优先排查对象，而不是要把所有 completed 样本都视作被污染。

- 当前判断:
   - 从 judgment 数量看，LLM fail 确实存在，且 response / episode 两侧比例都在 `2.4% - 2.5%` 左右。
   - 从样本影响面看，问题并不广泛，当前主要是少数长样本集中承受了连接失败/服务掉线带来的判断空洞。
   - 因此后续续跑策略上，更合理的方向应是优先处理残留 `pending/processing`、并对这批少量重灾样本定向复位或复查，而不是默认整库 completed 结果已经失真。
