# 数据库可视化页面设计

## 1. 概述

为 claw-data-filter 项目添加 Streamlit Web 可视化界面，实现数据库数据的查看、筛选和导出功能。

## 2. 技术栈

- **前端框架**：Streamlit
- **数据库**：DuckDB（现有）
- **运行环境**：使用项目 `.venv`

## 3. 页面结构

### 3.1 导航栏（左侧）
- 📊 统计概览
- 🔍 数据筛选
- 📤 数据导出
- 📋 数据表预览

### 3.2 页面路由
通过 Streamlit `query_params` 实现页面跳转：
- `/` - 统计概览
- `/?page=filter` - 数据筛选
- `/?page=export` - 数据导出
- `/?page=tables` - 数据表预览
- `/?page=detail&sample_id=XXX` - Sample 详情页

## 4. 功能模块

### 4.1 统计概览
- 复用现有 `DuckDBStore.get_stats()` 逻辑
- 展示：
  - 总样本数
  - 已处理样本数
  - 平均 response_helpful_rate
  - 平均 user_satisfied_rate
  - 错误样本数

### 4.2 数据筛选
**筛选条件：**
- `response_helpful_rate`: 比较符 + 值（如 `>= 0.7`）
- `user_satisfied_rate`: 比较符 + 值（如 `>= 0.5`）
- `task_type`: 文本匹配
- `num_turns`: 范围筛选
- 时间范围：`imported_at` 起止日期

**结果展示：**
- 分页表格（每页 20 条）
- 列：ID、task_type、num_turns、helpful_rate、satisfied_rate、操作（查看详情）
- 支持"应用筛选"、"重置"、"导出选中"按钮

### 4.3 数据导出
- 按筛选条件导出 JSONL
- 支持字段选择
- 显示导出预览（条数、文件大小估算）

### 4.4 数据表预览
- 下拉选择表：`samples`、`turn_judgments`
- 显示表结构（列名、类型）
- 数据预览（前 100 条，分页）

### 4.5 Sample 详情页
**URL**：`/?page=detail&sample_id=XXX`

**内容：**
- 基本信息卡片：task_type、num_turns、num_tool_calls、helpful_rate、satisfied_rate
- 时间范围筛选
- Turn 列表（按 turn_index 排序）
- 每个 Turn 展开显示：
  - User message
  - Assistant response（含 tool_calls）
  - Turn judgment：`response_helpful`、`user_satisfied`
  - `signal_from_users` 信号列表
  - `llm_error` 状态

## 5. 数据库 Schema（参考）

```sql
-- samples 表
CREATE TABLE samples (
    id INTEGER PRIMARY KEY,
    raw_json JSON,
    user_query TEXT,
    assistant_response TEXT,
    num_turns INTEGER,
    num_tool_calls INTEGER,
    has_error BOOLEAN,
    imported_at TIMESTAMP,
    tool_stats JSON,      -- {response_helpful_rate, user_satisfied_rate, total_turns, has_error}
    task_type TEXT
)

-- turn_judgments 表
CREATE TABLE turn_judgments (
    id INTEGER PRIMARY KEY,
    sample_id INTEGER,
    turn_index INTEGER,
    response_helpful TEXT,  -- yes/no/uncertain
    user_satisfied TEXT,    -- yes/no/uncertain/neutral
    signal_from_users JSON,
    llm_error BOOLEAN,
    created_at TIMESTAMP
)
```

## 6. 文件结构

```
claw_data_filter/
├── web/
│   ├── __init__.py
│   ├── app.py              # Streamlit 主应用
│   ├── pages/
│   │   ├── __init__.py
│   │   ├── overview.py      # 统计概览
│   │   ├── filter.py        # 数据筛选
│   │   ├── export.py        # 数据导出
│   │   ├── tables.py        # 数据表预览
│   │   └── sample_detail.py # Sample 详情
│   └── components/
│       ├── __init__.py
│       ├── stats.py          # 统计组件
│       └── table_viewer.py   # 通用表格组件
```

## 7. 启动方式

```bash
# 使用项目 venv 启动
cd /kanas/nlp/liuchang/claw/claw_data_clean_and_wash
source .venv/bin/activate
streamlit run claw_data_filter/web/app.py
```

## 8. 依赖

无新增外部依赖，Streamlit 应已安装。如需添加：
```bash
source .venv/bin/activate
pip install streamlit
```
