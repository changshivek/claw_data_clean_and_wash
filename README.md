# Claw Data Filter

LLM-powered agent conversation data filtering tool. Import JSONL files, run round feedback judgments, filter and export high-quality data.

## Quick Start

```bash
# 1. 导入数据
claw-filter import data.jsonl

# 2. 运行 round feedback 评分（需要 LLM 服务器）
claw-filter pressure-test  # 先测试稳定性
claw-filter round-feedback --workers 32  # 并发32处理

# 3. 查看统计
claw-filter stats

# 4. 筛选导出
claw-filter filter --response-helpful-rate ">=0.7" --export filtered.jsonl
```

## 数据格式

支持 OpenAI 格式和 UniRouter 格式（自动转换）：

```json
{"messages": [
  {"role": "user", "content": "用户问题"},
  {"role": "assistant", "content": "回复", "tool_calls": [...]},
  {"role": "tool", "content": "工具结果", "tool_call_id": "..."}
]}
```

UniRouter 格式自动从 `request.bodyJson.messages` 提取。

## 评分维度

每轮 assistant 回复判断两个指标：

| 维度 | 值 | 说明 |
|------|-----|------|
| **response_helpful** | yes/no/uncertain | 回复对用户是否有帮助 |
| **user_satisfied** | yes/no/uncertain/neutral | 基于后续用户行为判断满意度 |

**user_satisfied 判定：**
- 用户追问/澄清 → no
- 用户确认/继续 → yes
- 用户转新话题 → neutral
- 无明确信号 → uncertain

## 筛选字段

| 字段 | 来源 | 说明 |
|------|------|------|
| response_helpful_rate | samples.tool_stats | helpful=yes 比例 |
| user_satisfied_rate | samples.tool_stats | satisfied=yes 比例 |
| task_type | samples | 任务类型 |
| num_turns | samples | 轮次数 |

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `LLM_ENDPOINT` | http://localhost:8000/v1 | LLM API 地址 |
| `LLM_API_KEY` | - | API 密钥 |
| `DB_PATH` | ./data.duckdb | 数据库路径 |
| `MAX_CONCURRENCY` | 10 | 最大并发数 |
| `LLM_TIMEOUT` | 60.0 | 超时（秒） |

## CLI 命令

```bash
claw-filter import <file>              # 导入 JSONL
claw-filter pressure-test              # LLM 稳定性测试
claw-filter round-feedback            # 运行 round feedback 评分
claw-filter stats                     # 查看统计
claw-filter filter [options] --export <file>  # 筛选导出
claw-filter info                     # 数据库信息
```

### Filter 选项

```bash
# 按 response_helpful_rate 筛选
claw-filter filter --response-helpful-rate ">=0.7" --export out.jsonl

# 按 user_satisfied_rate 筛选
claw-filter filter --user-satisfied-rate ">=0.7" --export out.jsonl

# 按 task_type 筛选
claw-filter filter --task-type coding --export out.jsonl

# 组合筛选
claw-filter filter --response-helpful-rate ">=0.7" --user-satisfied-rate ">=0.5" --task-type coding --export out.jsonl

# 带统计报告
claw-filter filter --response-helpful-rate ">=0.7" --export out.jsonl --report stats.json
```

## 目录结构

```
claw_data_filter/
├── cli.py              # CLI 命令
├── config.py           # 配置
├── models/             # 数据模型
├── importers/          # JSONL 导入
├── processors/         # RoundFeedback 处理器
├── storage/            # DuckDB 操作
├── filters/            # 筛选查询
├── exporters/          # 导出
└── llm/                # LLM 客户端
```

## 开发

```bash
pip install -e ".[dev]"
pytest tests/ -v           # 运行测试
SKIP_INTEGRATION=1 pytest tests/ -v  # 跳过集成测试
```
