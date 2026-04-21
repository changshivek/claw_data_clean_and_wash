# 2026-04-14 Round-Feedback 长样本失败隔离

## 现状概述

- 当前 UniRouter 保留恢复库仍为 `data/unirouter_refresh/unirouter_refresh_preserved_after_vllm_crash_20260413_1702.duckdb`。
- 截至本阶段启动时，`session_merge_keep=TRUE` 范围内样本状态为：`completed=5091`、`pending=909`、`processing=9`。
- 最近一轮 `32` 并发补跑已经证明：
  - prompt 超长并不是当前唯一核心问题；
  - vLLM 掉线会导致 judgment 级 `llm_error`；
  - 少数超大样本会集中积累大量 LLM fail；
  - 失败窗口内还暴露了 DuckDB 写回阶段的重复主键冲突问题。
- 当前已知 judgment 级统计：
  - `response_progress`: 总数 `439535`，`llm_error=true` 为 `10549`，占比约 `2.400%`
  - `user_satisfied`: 总数 `180057`，`llm_error=true` 为 `4552`，占比约 `2.528%`
- 当前已知样本级判断：
  - 出现过任一维度 LLM fail 的 completed kept 样本仅 `19` 条，占 `5091` 条 completed kept 样本约 `0.373%`
  - 问题不是“大面积 completed 样本结果已经失真”，而是“少数超长/超重样本集中承受失败”

## 当前问题

1. 目前还不能确认这些重灾样本的主因到底是单条样本本身过长、单样本内部 task 数过多，还是并发条件下放大了服务不稳定。
2. 目前也还不能确认，未来优化重点应该放在：
   - token / prompt budget 级别的更细粒度并发控制；
   - 长样本拆分或分段写回；
   - 单样本内部 judgment task 的节流；
   - 或 DuckDB 写回一致性修复。
3. 在没有隔离实验之前，继续直接整库补跑，信息增益有限，风险仍高。

## 阶段目标

本阶段先不急于继续整库补跑，优先回答两个问题：

1. 把若干“高失败长样本”单独跑 `round-feedback` 流程时，是否仍然失败？
2. 如果单样本、低并发条件下仍失败，则问题更偏向样本自身长度 / task 数；如果单样本稳定、批量并发才失败，则问题更偏向并发与调度策略。

只有这个判断清楚后，后续才好决定下一步改进方向是 token 预算级并发控制，还是别的方案。

## 实验对象

当前优先关注的高失败样本：

- `96b7a3c3283abc3a851985c3485caa745dd1bda973cce271f5febb821135756f`
- `a3617ebe229fe56cf594b2f2b7c97c6777b3bcc968eb5c7ec88dce13cdad41cf`
- `c9e72aa583f8f59cf4f005638703a4322e0c55ecacd4f315e34bb5e6fa83d5ed`
- `48fd315b595cdc6169db2298e5f7e656b615ee90e3639eec23b5a57f573a083a`

这些样本具有以下特征：

- `response_llm_errors` 可超过 `700`
- `episode_llm_errors` 可超过 `300`
- 某些样本在 judgment 级别错误率接近或达到 `100%`

## 计划

### 计划 A：补一个长样本隔离测试 / 实验入口

目标：

- 从保留库中按 `sample_uid` 读取原始 `raw_json`
- 在隔离环境中只跑单个样本的 `RoundFeedbackProcessor.process_sample()`
- 用最小并发设置先验证“样本本身是否稳定可跑”

设计要求：

- 不直接污染当前保留库，优先写入临时 DuckDB
- 支持指定样本 UID 列表重复复现
- 能记录：
  - response contexts 数量
  - episode contexts 数量
  - prompt 长度上界
  - judgment 成功 / `llm_error` / 异常退出情况

优先判断：

- 如果 `workers=1`、单样本、低并发下仍然稳定失败，则优先怀疑样本长度 / task 数 / 单样本内部流程设计
- 如果单样本可跑，多个重样本并行才失败，则优先怀疑并发与调度策略

### 计划 B：基于实验结果决定下一步优化方向

若实验显示单样本也会失败：

- 重点讨论：
  - token 预算级调度
  - 长样本内部 task 分批
  - response / episode judgment 分阶段执行
  - 超长样本降级策略

若实验显示单样本稳定、并发下才失败：

- 重点讨论：
  - 全局并发控制改成基于估算 token 成本的 budget 调度
  - 长样本权重化限流
  - 大样本与普通样本分池执行

若实验显示写回阶段也会独立出错：

- 重点讨论：
  - `replace_round_feedback_results()` 的事务一致性
  - 重复插入 / 回滚后状态恢复问题
  - 长样本失败后是否应先写临时结果，再统一提交

## 执行记录

### 2026-04-14 初始化

- 已完成：
  - 建立本阶段独立跟踪文档
  - 从前一阶段恢复文档中抽取当前已知事实，明确问题边界
  - 确认优先排查对象为少数高失败长样本，而非整库 completed 结果
  - 实现隔离实验入口 `round-feedback-sample`：可按 `sample_uid` 从源库抽样，写入隔离 DuckDB，并逐条执行 `RoundFeedbackProcessor.process_sample()`
  - 为隔离入口补充 CLI 测试，确认它能在不污染源库的情况下完成单样本 round-feedback 复现

- 当前下一步：
  - 先对 2 到 4 条重样本做单样本低并发复现实验，再决定继续往 token budget 控制还是写回一致性方向推进

### 2026-04-14 首条低并发隔离实验结果

- 实验对象:
  - `sample_uid=96b7a3c3283abc3a851985c3485caa745dd1bda973cce271f5febb821135756f`
  - 该样本此前在保留库中属于最重灾样本之一，历史统计为 `response_llm_errors=705`、`episode_llm_errors=349`，且两侧错误率都接近 `100%`。

- 实验配置:
  - 运行方式: 单样本隔离直跑，不经过整库 claim/batch 流程
  - LLM 侧参数:
    - `workers=1`
    - `LLM_TIMEOUT=120`
    - `MAX_RETRIES=3`
    - `LLM_RETRY_BASE_DELAY=5`
    - `LLM_RETRY_MAX_DELAY=30`
  - 输出隔离库:
    - `data/unirouter_refresh/isolated/isolated_direct_96b7_20260414_173836.duckdb`
  - 实验日志:
    - `data/unirouter_refresh/logs/isolated_direct_96b7_20260414_173832.log`

- 样本体量:
  - `raw_json` 大小约 `934716` 字符
  - `message_count=1411`
  - `assistant_response_units=705`
  - `user_episode_count=349`

- 实验结果:
  - 单样本、`workers=1` 条件下，该样本成功完整跑通。
  - 完成耗时约 `125.58s`
  - 最终结果为:
    - `response_judgments=705`
    - `episode_judgments=349`
    - `response_llm_errors=0`
    - `episode_llm_errors=0`
    - `has_error=False`
    - `response_progress_rate=0.9177304964539007`
    - `user_satisfied_rate=0.0`

- 当前判断更新:
  - 这条结果强烈说明，至少对最重样本之一而言，问题并不是“单条样本天然过长，低并发下也无法完成”。
  - 当前更优先的怀疑方向，已经从“单样本长度绝对超限”转向“并发/调度策略在长样本下放大了服务不稳定性”。
  - 换句话说，后续优化优先级应明显上移到：
    - token / cost-aware 的并发控制
    - 长样本权重化限流
    - 大样本与普通样本分池执行

- 额外观察:
  - 在实验脚手架里，如果对该样本先做完整 prompt 摘要，预处理阶段本身就会消耗明显 CPU / IO。
  - 因此后续隔离实验工具不应默认对超大样本做重型 prompt 预摘要；更合理的是把“直跑复现”和“详细 prompt 分析”拆成两个模式。

### 2026-04-14 同样本 32 并发隔离实验结果

- 实验对象:
  - `sample_uid=96b7a3c3283abc3a851985c3485caa745dd1bda973cce271f5febb821135756f`
  - 仍使用与前一实验相同的重样本，目的是复现“上次整库失败时的并发等级”，但去掉整库调度和大保留库读取干扰。

- 实验配置:
  - 运行方式: 单样本隔离直跑
  - 输入源库: `data/unirouter_refresh/isolated/isolated_direct_96b7_20260414_173836.duckdb`
  - 输出目标库: `data/unirouter_refresh/isolated/isolated_from_smallsrc_96b7_w32_20260414_175756.duckdb`
  - 日志文件: `data/unirouter_refresh/logs/isolated_from_smallsrc_96b7_w32_20260414_175753.log`
  - 并发参数:
    - `workers=32`
    - `LLM_TIMEOUT=120`
    - `MAX_RETRIES=3`
    - `LLM_RETRY_BASE_DELAY=5`
    - `LLM_RETRY_MAX_DELAY=30`

- 实验结果:
  - 该样本在单样本隔离环境下，`workers=32` 也成功完整跑通。
  - 完成耗时约 `27.25s`
  - 最终结果为:
    - `response_judgments=705`
    - `episode_judgments=349`
    - `response_llm_errors=0`
    - `episode_llm_errors=0`
    - `has_error=False`
    - `response_progress_rate=0.9106382978723404`
    - `user_satisfied_rate=0.0`

- 当前判断进一步收敛:
  - 对同一条最重样本而言，`workers=1` 与 `workers=32` 的单样本隔离实验都能稳定完成，且没有任何 `llm_error`。
  - 因此当前问题已经不太像“单条长样本 + 32 并发本身就会失败”。
  - 更值得优先怀疑的方向是整库运行时的系统级因素，例如：
    - 多个重样本同时在 flight 时的总 token / prompt 成本失控
    - claim/batch 调度把大样本堆在同一批次
    - 整库运行中的 DuckDB 写回竞争或回滚一致性问题
    - 样本级并发之外的批次级放大效应

- 对下一步的影响:
  - 后续实验不应再只比较“单样本 1 并发 vs 单样本 32 并发”，因为这条路径已经证明两者都可跑通。
  - 更有信息增益的下一步，应转向：
    - 多个重样本同时隔离并发
    - 或在整库逻辑里引入基于估算成本的限流 / 分池，然后再复测

### 2026-04-14 多重样本并发复现实验结果

- 实验对象:
  - `96b7a3c3283abc3a851985c3485caa745dd1bda973cce271f5febb821135756f`
  - `a3617ebe229fe56cf594b2f2b7c97c6777b3bcc968eb5c7ec88dce13cdad41cf`
  - `c9e72aa583f8f59cf4f005638703a4322e0c55ecacd4f315e34bb5e6fa83d5ed`
  - `48fd315b595cdc6169db2298e5f7e656b615ee90e3639eec23b5a57f573a083a`

- 预备动作:
  - 从保留恢复库抽取上述 4 条重样本，生成小型源库 `data/unirouter_refresh/isolated/heavy_samples_source_4.duckdb`
  - 4 条样本 `raw_json` 体量都在约 `87.9w` 到 `89.9w` 字符之间

- 实验一：单命令多样本 `round-feedback-sample --workers 32`
  - 目标：先快速验证“把 4 条重样本同时交给实验入口”是否会复现问题
  - 结果：没有复现失败，但日志显示样本之间是严格串行执行，而不是多样本真正同时在飞
  - 结论：这个命令只能证明“样本内并发”，不能用于验证“样本间并发”

- 实验二：4 进程并发、每进程 `workers=8`、共享同一个 source DB
  - 目标：构造总并发约 `32` 的真实多进程实验
  - 结果：其中 3 个进程立即失败，报错为 DuckDB 文件锁冲突：
    - `Could not set lock on file ... heavy_samples_source_4.duckdb`
  - 结论：首次失败不是 vLLM 或 round-feedback 写回失败，而是实验入口读取 source DB 时错误持有可写锁
  - 处理：已将 `round-feedback-sample` 的 source DB 打开方式修正为 `read_only=True`

- 实验三：4 进程并发、每进程 `workers=8`、每进程独立 source DB 副本
  - 目标：排除 DuckDB 锁干扰，复现“多个重样本同时在飞、总并发约 32”
  - 结果：4 个进程全部成功退出，且 4 条样本都为：
    - `response_llm_errors=0`
    - `episode_llm_errors=0`
    - `has_error=False`
  - 代表日志：
    - `data/unirouter_refresh/logs/parallel_heavy4_w8each_sepdb_20260414_182854_96b7a3c3.log`
    - `data/unirouter_refresh/logs/parallel_heavy4_w8each_sepdb_20260414_182854_a3617ebe.log`
    - `data/unirouter_refresh/logs/parallel_heavy4_w8each_sepdb_20260414_182854_c9e72aa5.log`
    - `data/unirouter_refresh/logs/parallel_heavy4_w8each_sepdb_20260414_182854_48fd315b.log`

- 实验四：4 进程并发、每进程 `workers=32`、每进程独立 source DB 副本
  - 目标：把总请求压力进一步提升到远高于历史整库 `workers=32` 的水平，验证是否能压出 vLLM 失效模式
  - 结果：4 个进程全部成功退出，且 4 条样本都为：
    - `response_llm_errors=0`
    - `episode_llm_errors=0`
    - `has_error=False`
  - 代表日志：
    - `data/unirouter_refresh/logs/parallel_heavy4_w32each_sepdb_20260414_183120_96b7a3c3.log`
    - `data/unirouter_refresh/logs/parallel_heavy4_w32each_sepdb_20260414_183120_a3617ebe.log`
    - `data/unirouter_refresh/logs/parallel_heavy4_w32each_sepdb_20260414_183120_c9e72aa5.log`
    - `data/unirouter_refresh/logs/parallel_heavy4_w32each_sepdb_20260414_183120_48fd315b.log`

- 当前判断再次收敛:
  - 目前已经排除了以下解释：
    - “单个重样本本身必然失败”
    - “4 个重样本同时在飞、总并发约 32 就会失败”
    - “4 个重样本同时在飞、总并发远高于 32 就会失败”
  - 因此历史整库失败更像是整库运行特有的系统级条件触发，而不是单纯的多重样本 LLM 压力；更可能的因素包括：
    - claim / batch 调度把更多重样本和普通样本长时间混跑，形成更长的压力窗口
    - 整库处理中的共享写回路径、失败重试、状态恢复与去重逻辑共同作用
    - 历史失败依赖于“长时间运行 + 共享处理器实例 + 持续批次领取”，而不仅仅是某一时刻的并发量

- 对下一步的影响:
  - 下一步高信息增益实验，不应继续只做 isolated 单样本或 isolated 多进程压测。
  - 更值得推进的是：
    - 在整库 `round-feedback` 路径上加入“仅处理指定 sample_uid 集合”的 claim 过滤能力，复现真实 batch/claim 调度
    - 或在整库路径中加样本体量分池 / token budget 限流，再对比是否能消除历史失败窗口

### 2026-04-15 主流程补跑重试结果（新 vLLM 部署参数）

- 背景:
  - 本轮不是 isolated 实验，而是回到正式主流程 `round-feedback`，尝试直接把保留库里未跑完的样本继续补完。
  - 运行前确认新部署后的 `vLLM` 端点健康，可正常返回 `/v1/models`。

- 运行前处理:
  - 保留库 `data/unirouter_refresh/unirouter_refresh_preserved_after_vllm_crash_20260413_1702.duckdb` 在 `session_merge_keep=TRUE` 范围内原状态为：
    - `completed=5091`
    - `pending=909`
    - `processing=9`
  - 由于正式主流程只会 claim `pending/failed`，先将 9 条上次崩溃遗留的 `processing` 样本回收到 `pending`。
  - 回收后状态变为：
    - `completed=5091`
    - `pending=918`

- 主流程配置:
  - 命令: `round-feedback --workers 32 --batch-size 32`
  - LLM 参数:
    - `LLM_TIMEOUT=120`
    - `MAX_RETRIES=3`
    - `LLM_RETRY_BASE_DELAY=5`
    - `LLM_RETRY_MAX_DELAY=30`
  - 日志文件:
    - `data/unirouter_refresh/logs/preserved_resume_retry_32_20260415_101306.log`

- 运行现象:
  - 主流程启动后，前几分钟主要消耗在保留库读取/启动阶段，随后成功进入真实 batch 流程：
    - `Claiming unprocessed samples: limit=32`
    - `Claimed unprocessed samples: claimed=32`
    - `Round feedback batch start: batch_size=32`
  - 第一批 32 条样本已经被正式主流程领取并启动处理，说明这次尝试确实打到了真实 `claim -> process_batch -> process_sample` 链路，而不是停留在 isolated 脚手架。

- 失败结果:
  - 在第一批次里，首条样本 `e53b29a761f6a6e142b92038f59917c68fb8252b508b62c02bb99df7a675c0a2` 刚开始写回时，再次触发 DuckDB fatal duplicate-key 异常：
    - `duplicate key "resp:e53b29...:0"`
  - 该异常与 2026-04-14 历史主流程失败中的主键冲突模式一致，且依旧发生在 `replace_round_feedback_results()` 写回阶段。
  - 由于异常是 DuckDB `FatalException`，进程在首批写回阶段即被打断，本轮未能继续验证新 `vLLM` 部署参数在长时运行中的效果。

- 当前判断进一步收敛:
  - 新 `vLLM` 部署参数至少没有消除当前最先触发的主流程故障；本轮失败在第一批写回阶段就被 DuckDB duplicate-key 中断，而不是先出现大规模 `llm_error` 或明显的 `vLLM` 不可用。
  - 这说明当前主流程“跑不完未完成 case”的第一优先级阻塞项，依然是 DuckDB 写回一致性 / 去重路径，而不是 isolated 实验里讨论的 token/prefill 压力。
  - 换句话说，即使 `vLLM` 侧部署参数已经改善，正式主流程也会先被写回 fatal 异常拦住，导致我们很难观察到 deploy 参数调整的真实收益。

- 对下一步的影响:
  - 如果目标是先把库里剩余 case 跑完，下一步优先级应切回：
    - 修复 `replace_round_feedback_results()` 在 delete/insert/rollback 路径上的重复主键问题
    - 或增加更保守的幂等写回策略，避免单条样本写回失败直接触发 DuckDB fatal
  - 在这个问题修掉之前，继续用正式主流程验证 `vLLM` 部署参数，信号会持续被写回 fatal 异常截断。

### 2026-04-15 事故样本单条修复结果

- 修复对象:
  - `sample_uid=e53b29a761f6a6e142b92038f59917c68fb8252b508b62c02bb99df7a675c0a2`

- 先验判断:
  - 这条样本的 `raw_json` 本身没有损坏。
  - 同一条样本从保留库抽到 isolated DB 后，可以独立完整跑通：
    - `messages=173`
    - `response_contexts=86`
    - `episode_contexts=40`
    - `response_llm_errors=0`
    - `episode_llm_errors=0`
    - `has_error=False`
  - 代表日志：
    - `data/unirouter_refresh/logs/isolated_e53b_20260415_103820.log`

- 沙箱验证结果:
  - 在主库副本上直接尝试把 isolated 结果补回时，仍会立刻触发 duplicate-key，说明问题不在样本内容，而在主库 judgment 表状态。
  - 对 judgment 表做重建后，可以观察到该样本原先“看不见”的脏残留 actually 已存在：
    - `assistant_response_judgments=86`
    - `user_episode_judgments=40`
  - 这些残留并不完整可靠，其中 response judgment 里可见大量 `llm_error=true`，属于主流程 fatal 前留下的半截写入。
  - 在全新沙箱中同时重建两张 judgment 表、删除这条样本的脏残留、再回填 isolated 干净结果后，单样本修复成功。

- 真实保留库修复动作:
  - 在真实主库修复前，先创建快照备份：
    - `data/unirouter_refresh/unirouter_refresh_preserved_before_e53b_repair_20260415_104435.duckdb`
  - 修复步骤与沙箱验证一致：
    - 重建 `assistant_response_judgments`
    - 重建 `user_episode_judgments`
    - 删除事故样本在两张 judgment 表中的脏残留
    - 回填 isolated DB 中该样本的完整 judgment 与 sample 聚合结果

- 修复结果:
  - 真实保留库中，该样本已成功修复为：
    - `processing_status=completed`
    - `expected_response_judgment_count=86`
    - `expected_episode_judgment_count=40`
    - `response_progress_rate=0.9767441860465116`
    - `user_satisfied_rate=0.02702702702702703`
    - `response_llm_errors=0`
    - `episode_llm_errors=0`
  - 修复后全库 kept 范围状态变为：
    - `completed=5092`
    - `pending=886`
    - `processing=31`

- 当前结论:
  - 这条事故样本“能修”，但修复对象不是它的原始对话数据，而是主库 judgment 表的损坏状态与该样本的半截脏写入。
  - 这也进一步证明，当前首要问题不是个别 sample 的 raw_json 异常，而是主流程异常中断后，DuckDB judgment 表可能进入“查询不可见但唯一约束仍生效”的损坏状态。

### 经验沉淀（供后续参考）

- 当主流程在 `replace_round_feedback_results()` 附近报 DuckDB duplicate-key fatal，而只读查询又显示事故样本在 judgment 表中是 `0` 行时，不要先怀疑 sample 的 `raw_json` 损坏。
- 对这类样本，优先做两步验证：
  - 先把样本抽到 isolated DB 单独重跑，判断原始数据本身是否可跑通
  - 再在主库副本上重建 judgment 表，检查是否会“重新看见”原先不可见的脏 judgment 残留
- 如果 isolated 能跑通，而 judgment 表重建后能看到该样本的旧 judgment，则基本可以判定：
  - 问题在主库 judgment 表 / 唯一索引状态
  - 不在 sample 原始数据本身
- 单样本修复的安全顺序应为：
  - 先做主库快照备份
  - 在沙箱副本上完整验证修复动作
  - 同时重建 `assistant_response_judgments` 与 `user_episode_judgments`
  - 删除目标样本的脏 judgment 残留并单独提交
  - 再从 isolated DB 回填该样本的干净 judgment 与 sample 聚合结果
- 关键经验：
  - 这类问题不是“表面 0 行就真的没有残留”，DuckDB 可能处于“查询不可见但唯一约束仍生效”的索引损坏状态
  - 因此直接对主库做单事务 delete+insert 修复往往还会再次撞同样的 duplicate-key
  - 先重建 judgment 表，再做样本级删除与回填，成功率显著更高

### 2026-04-15 vLLM 启动参数新发现

- 当前新增判断:
  - 造成前面一系列 `vLLM` engine 失活的更深层原因，已基本定位为 `vLLM` 默认开启 compile 与 CUDA graph capture 后，在突然遇到长样本时触发重新 compile / recapture。
  - 在 round-feedback 这种平时多数样本较短、但会突然混入长样本的流量形态下，这种“运行中遇到长样本再重编译 / 重捕获”的行为，会把 engine 推到超时死亡。

- 已执行变更:
  - `vLLM` 重启时新增了 `enforce_eager` 启动参数，以关闭这条 compile / CUDA graph capture 相关路径。

- 当前观察:
  - 加上 `enforce_eager` 并重启后，主流程日志已经开始恢复正常推进，说明这次变更至少显著改善了此前“长样本触发 engine 死亡”的问题。
  - 目前仍能看到一些 warning，但尚不能完全确认这些 warning 是：
    - 重启前遗留的历史影响
    - 还是重启后的新增 warning
  - 截至当前观察，`vLLM` 自身日志没有再出现新的 error。

- 现阶段结论:
  - 关于 `vLLM` 侧，这次问题不再只是“并发过高”或“backoff 不合理”，而更像是默认 compile / CUDA graph capture 策略与长样本流量分布不匹配。
  - 对当前 round-feedback 工作负载，`enforce_eager` 是一个高价值的稳定性开关，应作为后续恢复与复现实验的默认基线配置之一。

- 对后续排查的影响:
  - 后续如果再出现 warning，需要优先区分：
    - 是否为旧 run 残留
    - 是否伴随新的 `vLLM` engine error
    - 是否仍会导致主流程推进停滞
  - 只要 `vLLM` 无新的 engine error 且主流程持续推进，就不应把零散 warning 直接等同于旧问题复发。

### 2026-04-15 库内失败样本复盘与补跑分波

- 当前库状态复盘:
  - 当前保留库 `session_merge_keep=TRUE` 范围内样本已全部处于 `completed`，总数 `6009`。
  - 当前 `processing_status='failed'` 样本数为 `0`，因此不能通过直接重跑 `failed` 样本来覆盖历史问题样本。
  - 当前 `completed && tool_stats.has_error=true` 样本数为 `50`。
  - 这 `50` 条中：
    - `error_reason='no_messages'` 有 `20` 条，更像原始数据无消息，不属于优先补跑对象。
    - `error_reason` 为空有 `30` 条，这 `30` 条同时带 judgment 级 `llm_error`，才是本轮真正应补跑的失败样本集合。

- 当前对补跑对象的判断:
  - 这 `30` 条不是 `processing_status` 层面的失败，而是“completed 但 judgment 明细存在 LLM fail”。
  - 因此不能依赖主流程默认 claim 逻辑自动捞出它们；如果要用正式主流程重跑，必须先把目标样本显式重置为 `pending`。

- 体量分布:
  - `xl` 超重样本 `16` 条：`expected_response_judgment_count >= 700` 或 `expected_episode_judgment_count >= 340`
  - `l` 大样本 `11` 条：`expected_response_judgment_count >= 450` 或 `expected_episode_judgment_count >= 220`
  - `m` 中样本 `1` 条
  - `s` 小样本 `2` 条
  - 失败严重度上，`30` 条中有 `19` 条接近“整条样本几乎全坏”，说明不适合一次性大并发补跑。

- 当前补跑策略收敛:
  - 不采用“一次性把 30 条全部改回 pending 再整批跑”的方式。
  - 采用“按样本体量分波、每波先重置少量目标样本为 pending，再用正式主流程领取”的方式。
  - 这样可以同时保留：
    - 正式 `claim -> process_batch -> process_sample -> replace_round_feedback_results` 链路
    - 以及对重样本窗口大小的显式控制

- 分波建议:
  - 第一波：先跑 4 条已做过 isolated 验证的超重样本
    - `96b7a3c3283abc3a851985c3485caa745dd1bda973cce271f5febb821135756f`
    - `a3617ebe229fe56cf594b2f2b7c97c6777b3bcc968eb5c7ec88dce13cdad41cf`
    - `c9e72aa583f8f59cf4f005638703a4322e0c55ecacd4f315e34bb5e6fa83d5ed`
    - `48fd315b595cdc6169db2298e5f7e656b615ee90e3639eec23b5a57f573a083a`
  - 第二波：剩余 `12` 条超重样本，继续按每批 `4` 条推进
  - 第三波：`11` 条大样本，按每批 `5` 到 `6` 条推进
  - 第四波：`3` 条中小样本收尾

- 参数建议:
  - 超重样本批次：`workers=16`，`batch-size=4`
  - 大样本批次：`workers=24`，`batch-size=5` 或 `6`
  - 中小样本收尾：`workers=32`，`batch-size=3`

- 安全执行原则:
  - 每一波都应先做主库快照备份。
  - 每一波只重置该波目标样本，避免与已经干净的 `5979` 条样本混跑。
  - 若任一批次再次触发 DuckDB duplicate-key 或其他主库写回异常，应立即停止批量补跑，回到 isolated 单样本修复路径。

### 2026-04-15 第一波补跑启动

- 本波目标:
  - 使用正式主流程补跑 4 条已 isolated 证明可恢复的超重样本。
- 计划参数:
  - `workers=16`
  - `batch-size=4`
- 执行顺序:
  - 先创建主库快照
  - 将 4 条目标样本重置为 `pending`
  - 启动正式 `round-feedback` 主流程
  - 运行中持续观察是否出现：
    - 新的 DuckDB writeback fatal
    - 大规模 `ConnectTimeout/ConnectError`
    - 或 4 条样本全部正常提交

- 已执行:
  - 已创建主库快照：
    - `data/unirouter_refresh/unirouter_refresh_before_wave1_rerun_20260415_164515.duckdb`
  - 已将 4 条第一波目标样本重置为 `pending`
  - 已启动正式主流程补跑：
    - 日志：`data/unirouter_refresh/logs/wave1_rerun_4heavy_w16_20260415_165951.log`
    - 参数：`workers=16`、`batch-size=4`

- 当前运行中观察:
  - 主流程已按预期只 claim 到 `4` 条样本：
    - `Claimed unprocessed samples: claimed=4`
  - 当前并未误领取其他样本，说明“按波次重置 pending，再交给主流程 claim”的组织方式是生效的。
  - 截至当前记录时，以下 3 条样本已正常写回并提交成功，且 `has_error=False`：
    - `96b7a3c3283abc3a851985c3485caa745dd1bda973cce271f5febb821135756f`
    - `48fd315b595cdc6169db2298e5f7e656b615ee90e3639eec23b5a57f573a083a`
    - `a3617ebe229fe56cf594b2f2b7c97c6777b3bcc968eb5c7ec88dce13cdad41cf`
  - 剩余最后 1 条样本仍在运行中：
    - `c9e72aa583f8f59cf4f005638703a4322e0c55ecacd4f315e34bb5e6fa83d5ed`
  - 运行期间出现了零星 `RemoteProtocolError('Server disconnected without sending a response.')` 的 `Attempt 1/4` 告警，但截至当前记录尚未阻止样本正常提交，也未出现新的 DuckDB writeback fatal。

### 2026-04-15 第一波补跑完成与余量自动推进

- 第一波最终结果:
  - 4 条目标超重样本已全部通过正式主流程补跑成功。
  - 主流程正常收尾：
    - `Round feedback processing complete: 4 success, 0 failures`
  - 第一波 4 条样本在主库中均已恢复为：
    - `processing_status=completed`
    - `tool_stats.has_error=false`
    - `response_llm_errors=0`
    - `episode_llm_errors=0`

- 对整体状态的影响:
  - judgment 级失败样本数已从 `30` 条下降到 `26` 条。
  - `completed && tool_stats.has_error=true` 样本数已从 `50` 条下降到 `46` 条。
  - 说明当前“按波次重置 pending，再用正式主流程重跑”的路径对重样本是有效的，且不需要立即回退到 isolated 回写。

- 后续推进方式更新:
  - 已启动剩余 `26` 条样本的自动分波补跑脚本。
  - 自动脚本策略：
    - 按当前 live 库状态重新抽取候选样本
    - 逐波重置为 `pending`
    - 每波调用正式 `round-feedback` 主流程
    - 每波结束后立即校验目标样本是否已清零 `llm_error`
    - 若任一波主流程返回非零或校验未清零，则立即停止，转入 isolated 副本回写路径

- 自动脚本当前状态:
  - 运行日志：`data/unirouter_refresh/logs/wave_remainder_runner_20260415_171753.log`
  - 当前规划总波次：`8`
  - 当前已开始第 `2` 波，仍为超重样本批次：
    - `04bd648b21846c5bb453c1240887803d01a31e9dfdf2d3be5ddf65b328f3c149`
    - `05016c110e2042d8bfe4ba8474e6ae911226076ca07dbe14bd2e77364f8fd6e4`
    - `0b815cc81b7a4664692893593ccf83300eb2d325e4a66bd55355abacabe93f51`
    - `1cef4e23a94ac7aa5ff3cae8bc1ec93400eb6b898caf47a053a1346b878764fb`

- 自动脚本增量进展:
  - 第 `2` 波已通过正式主流程补跑完成，结果为：
    - `Round feedback processing complete: 4 success, 0 failures`
  - 第 `2` 波 4 条样本均已清零 judgment 级 `llm_error`，未触发 isolated 副本回写分支。
  - 第 `3` 波也已通过正式主流程补跑完成，结果为：
    - `Round feedback processing complete: 4 success, 0 failures`
  - 第 `3` 波 4 条样本均已清零 judgment 级 `llm_error`，未触发 isolated 副本回写分支：
    - `2426e487d7536b51a29641a1f089d2107efa433e8f1d0905f37a160f4c5ded62`
    - `29118d34513ecb96cce436c1bab18f44bf2933976cc21579cbda1f6776e14def`
    - `39250d26193d2b198aa417a22580b9732df9599f295c35ea0fa79fdf6fbc56b3`
    - `8f50f8ce781a8c539fcc1c078a0704080e3a3e72bff026b5bf0391a44ce1f12c`
  - 截至本次更新，前 `3` 波已累计清理 `12` 条超重样本。
  - 自动脚本已继续进入第 `4` 波，当前目标样本为：
    - `a85305df42a98356ea817cf3a729841db4a5e78502dce372df848d868cb48b6f`
    - `af2621c1f15f22001bb3147936a80cddc347696f243e52336ec2414b43b9b613`
    - `bb732b7677fde8fa4ed4dd8da9b652c67e9407e8fcf06f0c6194e5d3331abfd7`
    - `c1254266497a362f2f4a5b4ed3c9f97b8ae5373025c0e04e7f0b3db075c76f3b`
  - 截至当前记录，自动脚本仍按“单波完成后立即校验，失败才切 isolated 回写”的策略继续推进。

### 2026-04-15 自动分波补跑全部完成

- 自动脚本最终结果:
  - 第 `4` 波到第 `9` 波均已通过正式主流程补跑完成，且每波校验均通过。
  - 总控日志最终写出：
    - `ALL_REMAINING_WAVES_COMPLETED`
  - 末波日志：
    - `data/unirouter_refresh/logs/wave9_s_w32_2_20260415_181053.log`
  - 总控日志：
    - `data/unirouter_refresh/logs/wave_remainder_runner_20260415_171753.log`

- 第 `4` 波到第 `9` 波结果摘要:
  - 第 `4` 波：`4 success, 0 failures`
  - 第 `5` 波：`5 success, 0 failures`
  - 第 `6` 波：`5 success, 0 failures`
  - 第 `7` 波：`1 success, 0 failures`
  - 第 `8` 波：`1 success, 0 failures`
  - 第 `9` 波：`2 success, 0 failures`

- 最终主库状态:
  - 原先用于补跑的 `30` 条 judgment 级失败样本已全部清零。
  - 当前主库中“completed 且 `error_reason` 为空、但 judgment 仍有 `llm_error`”样本数为 `0`。
  - `session_merge_keep=TRUE` 范围内样本状态已收敛为：
    - `completed=6009`
  - 当前仍保留 `20` 条 `completed && tool_stats.has_error=true` 样本，但它们属于前面已排除的 `error_reason='no_messages'` 桶，不属于本轮 judgment 级失败补跑对象。

- 回写分支结果:
  - 本轮自动分波补跑没有触发 isolated 副本结果回写。
  - 除前面单独修复的事故样本 `e53b29...` 外，后续所有波次均直接在正式主流程中完成清理。

- 结论:
  - 这轮“按波次重置 pending，再走正式主流程 claim/process/writeback”的组织方式已成功清理主库全部 judgment 级未完成 case。
  - 本轮可以视为主库目标范围内补跑完成。