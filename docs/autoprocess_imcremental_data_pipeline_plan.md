# 目标：针对新增数据的定期运行pipeline服务构建

# 概述：

1. 本任务立足利用现有的数据导入，round feedback，筛选，导出，unisound格式转换等流程，组建为一个定期自动运行的对指定数据源路径的监控和导入处理流程；
2. 监控的数据路径，导出数据路径，筛选导出条件，数据库文件地址和其他一些运行环境量应当可以配置；
3. 任务开发完成后应当实现容器化打包，便于部署和维护；容器运行期间，web streamlit页面应当常态启动并映射到端口，对外暴露提供访问。

# 任务细节：
1. 基本流程：定时触发运行 -> 检查解压路径和源数据路径，如果有新增文件 -> 逐级解压新的文件，获得items.jsonl -> 执行导入、roundfeedback等流程 -> 按照预先设定的筛选条件执行筛选导出 -> 对筛选导出文件执行unisound格式转换 -> 运行结束，等待下次执行
2. 运行期间需要记录日志，记录每个步骤的运行状态和结果，便于监控状态和故障排查
3. 数据导出筛选应当基于运行记录，每次只增量导出新增数据中符合筛选条件的数据为单独的文件，并参考源文件的文件名进行适当命名

# 开发期间的初始默认环境
- python环境：/kanas/nlp/liuchang/claw/claw_data_clean_and_wash/.venv
- 导入、round feedback、导出、格式转换脚本位置：/kanas/nlp/liuchang/claw/claw_data_clean_and_wash/scripts
- 需要监控的新增数据路径（只筛选其中的tar文件，不允许写，只能读）：/kanas/nlp/liuchang/manydata/unirouter
- 可以使用的解压路径：/kanas/nlp/liuchang/manydata/unirouter_uncompress
- 建议的数据库存放路径：/kanas/nlp/liuchang/manydata/unirouter_duckdb
- 建议使用的中间文件临时存放路径：/kanas/nlp/liuchang/manydata/unirouter_in_process
- 最终的unisound格式数据导出路径：/kanas/nlp/liuchang/manydata/unirouter_unisound_format

# 开发流程规划
1. 确认文档需求，明确任务目标，建立执行计划细则
2. 从源数据采样建立少量测试数据，用于开发使用
3. 开发实现串联执行脚本，能够触发后完成整个流程的执行；
4. 基于测试数据和真实数据完成充分的测试
5. 基于当前环境建立dockerfile，构建服务镜像，在镜像中利用crontab建立定时执行机制，在镜像入点设置好启动命令，使得镜像启动即服务启动；
6. 基于镜像实现启动脚本，脚本中暴露配置区，传入文件路径、配置文件、端口等配置
7. 测试镜像部署流程是否能够通过
8. 报告开发结束。

# 开发约束
1. docker命令使用禁忌：严格禁止使用docker rm,  docker rmi, docker prune， docker stop, docker restart等可能对现状docker其他服务造成影响的命令；
2. docker操作执行方法：只能通过shell脚本或者python代码编写docker操作指令，严禁直接从命令行操作docker；所使用的docker操作脚本应当进行约束，使得操作只影响当前任务相关镜像建立的容器，或者当前任务的dockerfile建立的镜像，如果不满足约束，宁可不执行；
3. autopilot在确认执行docker脚本时应当进行审计，如果可能危害其他docker容器的运行或者整个docker环境，必须驳回运行请求，要求改正增加约束。
4. 在遭遇运行环境问题，数据路径问题，权限问题等阻碍开发执行，连续三次不同的重试不能解决的情况下，停止任务并等待人类给出进一步指示。
5. 整个开发流程应当建立专门的文档，记录目标、发现、开发计划、当前进展、遇到的问题等，并在每一步开发推进前予以维护；在contex window 压缩后应当读取文档恢复压缩前现状，接续开发
6. 每一步开发基于git进行代码维护，不同的开发阶段需要提交git commit 固定状态，开发遇到错误可以利用git回滚现场

# 当前发现
1. 现有仓库已经具备可复用的核心能力：JSONL 导入、session merge、round feedback、筛选导出、Unisound 转换、Streamlit Web 页面均已存在；缺失的是面向增量 tar 数据源的编排、状态记账、定时调度和容器化运行层。
2. 导入层已基于 sample_uid 做样本幂等去重，因此增量服务需要额外实现文件级处理状态与导出批次状态，避免重复解压、重复运行和重复导出。
3. Web 页面入口已存在于 claw_data_filter.web.app，可作为容器常驻前台服务的一部分直接启动。
4. 当前仓库尚未提供面向该任务的 Dockerfile、容器入口脚本和受约束的 docker 启动脚本。

# 设计决策
1. 新增一个增量 pipeline 编排模块，负责扫描 tar 文件、解压、定位 items.jsonl、调用已有导入/处理/导出能力，并记录运行结果。
2. 状态记账优先放入同一个 DuckDB 中，新增文件处理表、运行记录表、导出批次表，保证部署后单库可追溯。
3. 导出阶段采用“按本次运行新增样本 sample_uid 集合导出”的策略，确保输出严格对应当前新增数据，不混入历史已处理样本。
4. 容器内使用 crontab 触发批处理，入口脚本同时启动 cron 和 Streamlit；不在开发过程中直接执行高风险 docker 命令。

# 分阶段执行细则
1. 第一阶段：补充配置模型、状态表结构与编排服务骨架。
2. 第二阶段：打通单次运行链路，包括扫描、解压、导入、session merge、round feedback、增量导出、Unisound 转换与日志落盘。
3. 第三阶段：提供 CLI 入口、默认配置文件、示例脚本与测试数据路径约定。
4. 第四阶段：补充单元测试与最小集成测试，覆盖状态记账、增量筛选和输出命名。
5. 第五阶段：补充 Dockerfile、entrypoint、cron 模板与受约束的启动脚本。
6. 第六阶段：更新 README 与本计划文档，记录验证结果、遗留问题和部署方式。

# 当前进展
1. 已确认现有主链路和 Web 能力可复用。
2. 已确认导入幂等基于 sample_uid，可作为增量编排的基础保证。
3. 已完成增量编排服务、CLI、测试以及容器部署骨架的实现，当前处于 README、脚本审计和真实 LLM 连通性验证阶段。
4. OpenRouter 免费模型 google/gemma-4-26b-a4b-it:free 当前仅保留给小样本验证使用；正式增量 pipeline 默认配置已恢复为独立正式 LLM 服务占位值，API key 仍仅通过环境变量注入，不写入配置文件。

# 风险与备注
1. 文档要求“每一步开发基于 git commit 维护”，但当前会话执行策略不包含自动提交；代码实现会继续推进，commit 需在你明确要求后执行。
2. round feedback 依赖外部 LLM 服务；OpenRouter 免费模型存在明显速率限制，因此仅用于小样本验证脚本，不能作为全量跑批默认依赖。