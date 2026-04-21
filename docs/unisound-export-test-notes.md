# Unisound 导出测试说明

更新时间：2026-04-20

## 目标

本说明用于记录 Unisound 离线转换的测试范围、验证命令和实际结果。

## 测试范围

- 输入 `openai_round_feedback_v2` 记录的数据模型校验
- 输出 Unisound 记录的数据模型校验
- `system_prompt`、`tools` 与 `dialog` 分离
- `<think></think>` 拆分逻辑
- `Tool` 结构化对象数组保留
- `round_feedback` 到重建轮次的映射
- 10 条抽样全流程验证
- 全量文件转换与输出校验

## 测试输入

- 输入文件：`data/exported_0415_all_except_user_unsatisfy_gt_2ep.jsonl`
- 配置文件：`scripts/unisound_export_config.exported_0415.json`

## 计划执行命令

```bash
./.venv/bin/python scripts/unisound_export.py validate-input \
  --input data/exported_0415_all_except_user_unsatisfy_gt_2ep.jsonl

./.venv/bin/python scripts/unisound_export.py validate-output \
  --input data/exported_0415_all_except_user_unsatisfy_gt_2ep.sample10.unisound.jsonl

./.venv/bin/python scripts/unisound_export.py convert \
  --input data/exported_0415_all_except_user_unsatisfy_gt_2ep.jsonl \
  --output data/exported_0415_all_except_user_unsatisfy_gt_2ep.sample10.unisound.jsonl \
  --config scripts/unisound_export_config.exported_0415.json \
  --limit 10

./.venv/bin/python scripts/unisound_export.py convert \
  --input data/exported_0415_all_except_user_unsatisfy_gt_2ep.jsonl \
  --output data/exported_0415_all_except_user_unsatisfy_gt_2ep.unisound.jsonl \
  --config scripts/unisound_export_config.exported_0415.json
```

## 当前状态

- 数据模型和转换脚本已实现
- 测试代码已补充并通过
- 新增显式输出校验入口 `validate-output`
- 10 条抽样验证已完成并通过
- 全量转换与输出校验已完成

## 当前测试结果

- `./.venv/bin/python -m pytest tests/test_unisound_export.py`
- 结果：`4 passed`

补充说明：

- 在本次仓库收尾阶段，又额外执行了 `./.venv/bin/python -m pytest tests/test_cli.py tests/test_unisound_export.py -q`
- 结果：`16 passed`

## 10 条抽样验证结果

- 输入校验：通过，`validated_records = 10`
- 输出校验：通过，`validated_records = 10`
- 转换输出：`data/exported_0415_all_except_user_unsatisfy_gt_2ep.sample10.unisound.jsonl`
- 转换报告：`data/exported_0415_all_except_user_unsatisfy_gt_2ep.sample10.unisound.report.json`

## 全量验证中发现的问题

- 全量输入校验首次运行时发现第 514 条记录存在 `developer` 角色
- 已将该角色纳入输入模型，并按顶级提示信息并入 `system_prompt`
- 修复后已补充单测覆盖该场景

## 全量转换结果

- 输入校验：通过，`validated_records = 1957`
- 输出校验：通过，`validated_records = 1957`
- 转换输出：`data/exported_0415_all_except_user_unsatisfy_gt_2ep.unisound.jsonl`
- 转换报告：`data/exported_0415_all_except_user_unsatisfy_gt_2ep.unisound.report.json`
- 报告摘要：`count = 1957`，`english_count = 1742`

## 顶级 tools 结构调整

- 顶级 `tools` 已改为扁平结构
- sample10 已验证首个工具项不再包含 `type` 和 `function` 字段
- 相关回归测试已通过