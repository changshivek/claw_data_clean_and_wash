# Database Visualization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Streamlit web UI for viewing, filtering, and exporting database data

**Architecture:** Multi-page Streamlit app with page routing via query_params. Sidebar navigation for main sections. Sample detail as separate page. Reuses existing DuckDBStore for data access.

**Tech Stack:** Python 3.12, Streamlit, DuckDB (existing)

---

## File Structure

```
claw_data_filter/web/
├── __init__.py
├── app.py              # Main entry, sidebar nav, page routing
├── config.py           # DB path config
├── pages/
│   ├── __init__.py
│   ├── overview.py     # Statistics overview
│   ├── filter.py        # Data filter with results table
│   ├── export.py        # Export with field selection
│   ├── tables.py        # Table schema viewer
│   └── sample_detail.py # Sample + turns detail view
└── components/
    ├── __init__.py
    └── sample_table.py  # Reusable sample table component
```

**Dependencies:**
- `streamlit` (not installed, needs `pip install streamlit`)

---

## Task 1: Project Setup

**Files:**
- Create: `claw_data_filter/web/__init__.py`
- Create: `claw_data_filter/web/config.py`

- [ ] **Step 1: Create web package directory**

```bash
mkdir -p claw_data_filter/web/pages claw_data_filter/web/components
touch claw_data_filter/web/__init__.py claw_data_filter/web/pages/__init__.py claw_data_filter/web/components/__init__.py
```

- [ ] **Step 2: Create config.py**

```python
"""Web app configuration."""
import os
from pathlib import Path

DB_PATH = Path(os.environ.get("DB_PATH", "data.duckdb"))
```

- [ ] **Step 3: Commit**

```bash
git add claw_data_filter/web/
git commit -m "feat(web): initial web package structure"
```

---

## Task 2: Main App with Navigation

**Files:**
- Create: `claw_data_filter/web/app.py`

```python
"""Streamlit main app with sidebar navigation."""
import streamlit as st
from pathlib import Path

# Initialize page
st.set_page_config(
    page_title="Claw Data Filter",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Sidebar navigation
st.sidebar.title("Claw Data Filter")
page = st.sidebar.radio(
    "导航",
    ["📊 统计概览", "🔍 数据筛选", "📤 数据导出", "📋 数据表预览"],
)

# Route to pages
if page == "📊 统计概览":
    from claw_data_filter.web.pages import overview
    overview.render()
elif page == "🔍 数据筛选":
    from claw_data_filter.web.pages import filter
    filter.render()
elif page == "📤 数据导出":
    from claw_data_filter.web.pages import export
    export.render()
elif page == "📋 数据表预览":
    from claw_data_filter.web.pages import tables
    tables.render()
```

- [ ] **Step 1: Create app.py**

Write the file above to `claw_data_filter/web/app.py`

- [ ] **Step 2: Test app starts**

```bash
source .venv/bin/activate
pip install streamlit
streamlit run claw_data_filter/web/app.py --server.headless true
# Press Ctrl+C to stop
```

Expected: App starts without errors

- [ ] **Step 3: Commit**

```bash
git add claw_data_filter/web/app.py claw_data_filter/web/config.py
git commit -m "feat(web): main app with sidebar navigation"
```

---

## Task 3: Statistics Overview Page

**Files:**
- Create: `claw_data_filter/web/pages/overview.py`
- Modify: `claw_data_filter/storage/duckdb_store.py` (add new methods if needed)

- [ ] **Step 1: Add helper methods to DuckDBStore**

Modify `claw_data_filter/storage/duckdb_store.py` - add these methods:

```python
def get_processed_count(self) -> int:
    """Get count of samples with tool_stats."""
    result = self.conn.execute(
        "SELECT COUNT(*) FROM samples WHERE tool_stats IS NOT NULL"
    ).fetchone()
    return result[0] if result else 0

def get_table_list(self) -> list[str]:
    """Get list of tables in database."""
    result = self.conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
    ).fetchall()
    return [row[0] for row in result]

def get_table_schema(self, table_name: str) -> list[dict]:
    """Get column schema for a table."""
    result = self.conn.execute(
        """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = ?
        ORDER BY ordinal_position
        """,
        [table_name],
    ).fetchall()
    return [{"name": row[0], "type": row[1]} for row in result]
```

- [ ] **Step 2: Create overview.py**

```python
"""Statistics overview page."""
import streamlit as st
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.config import DB_PATH


def render():
    st.title("统计概览")

    store = DuckDBStore(DB_PATH)
    stats = store.get_stats()
    processed_count = store.get_processed_count()

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("总样本数", stats["total_samples"])
    col2.metric("已处理", processed_count)
    col3.metric("平均 Helpful Rate", f"{stats['avg_response_helpful_rate']:.2f}")
    col4.metric("平均 Satisfied Rate", f"{stats['avg_user_satisfied_rate']:.2f}")

    st.divider()

    col5, col6 = st.columns(2)
    col5.metric("错误样本数", stats["error_count"])
    col6.metric("未处理", stats["total_samples"] - processed_count)
```

- [ ] **Step 3: Test page**

```bash
streamlit run claw_data_filter/web/app.py --server.headless true
# Navigate to http://localhost:8501
# Select "统计概览" from sidebar
# Verify stats display correctly
```

- [ ] **Step 4: Commit**

```bash
git add claw_data_filter/web/pages/overview.py
git add claw_data_filter/storage/duckdb_store.py
git commit -m "feat(web): statistics overview page"
```

---

## Task 4: Sample Table Component (Reusable)

**Files:**
- Create: `claw_data_filter/web/components/sample_table.py`

```python
"""Reusable sample table component."""
import streamlit as st
from typing import Callable


def render_samples_table(
    samples: list[dict],
    page: int,
    total_pages: int,
    on_detail_click: Callable[[int], None],
):
    """Render a paginated sample table.

    Args:
        samples: List of sample dicts with id, task_type, num_turns, etc.
        page: Current page number (1-indexed)
        total_pages: Total number of pages
        on_detail_click: Callback(sample_id) when detail is clicked
    """
    if not samples:
        st.info("没有找到匹配的样本")
        return

    # Table header
    cols = st.columns([0.5, 1, 0.8, 0.8, 0.8, 0.8, 1])
    headers = ["ID", "task_type", "num_turns", "helpful_rate", "satisfied_rate", "has_error", "操作"]
    for col, header in zip(cols, headers):
        col.markdown(f"**{header}**")

    # Table rows
    for sample in samples:
        cols = st.columns([0.5, 1, 0.8, 0.8, 0.8, 0.8, 1])
        cols[0].write(sample["id"])
        cols[1].write(sample.get("task_type", "-"))
        cols[2].write(sample.get("num_turns", 0))
        cols[3].write(f"{sample.get('helpful_rate', 0):.2f}")
        cols[4].write(f"{sample.get('satisfied_rate', 0):.2f}")
        cols[5].write("✓" if sample.get("has_error") else "-")
        if cols[6].button("详情", key=f"detail_{sample['id']}"):
            on_detail_click(sample["id"])

    # Pagination
    col_prev, col_page, col_next = st.columns([1, 2, 1])
    if page > 1:
        if col_prev.button("上一页"):
            st.session_state.page = page - 1
            st.rerun()
    col_page.markdown(f"第 {page} / {total_pages} 页")
    if page < total_pages:
        if col_next.button("下一页"):
            st.session_state.page = page + 1
            st.rerun()
```

- [ ] **Step 1: Create sample_table.py**

Write the file above

- [ ] **Step 2: Commit**

```bash
git add claw_data_filter/web/components/sample_table.py
git commit -m "feat(web): reusable sample table component"
```

---

## Task 5: Data Filter Page

**Files:**
- Create: `claw_data_filter/web/pages/filter.py`

- [ ] **Step 1: Add filter query method to DuckDBStore**

Add to `claw_data_filter/storage/duckdb_store.py`:

```python
def filter_samples(
    self,
    helpful_rate_op: str = ">=",
    helpful_rate_val: float = 0.0,
    satisfied_rate_op: str = ">=",
    satisfied_rate_val: float = 0.0,
    task_type: str | None = None,
    num_turns_min: int | None = None,
    num_turns_max: int | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    limit: int = 20,
    offset: int = 0,
) -> tuple[list[dict], int]:
    """Filter samples with various criteria.

    Returns (samples, total_count).
    """
    conditions = ["tool_stats IS NOT NULL"]

    if helpful_rate_op and helpful_rate_val is not None:
        conditions.append(
            f"CAST(json_extract(tool_stats, '$.response_helpful_rate') AS DOUBLE) {helpful_rate_op} {helpful_rate_val}"
        )
    if satisfied_rate_op and satisfied_rate_val is not None:
        conditions.append(
            f"CAST(json_extract(tool_stats, '$.user_satisfied_rate') AS DOUBLE) {satisfied_rate_op} {satisfied_rate_val}"
        )
    if task_type:
        conditions.append(f"task_type LIKE '%{task_type}%'")
    if num_turns_min is not None:
        conditions.append(f"num_turns >= {num_turns_min}")
    if num_turns_max is not None:
        conditions.append(f"num_turns <= {num_turns_max}")
    if date_from:
        conditions.append(f"imported_at >= '{date_from}'")
    if date_to:
        conditions.append(f"imported_at <= '{date_to}'")

    where_clause = " AND ".join(conditions)

    # Get total count
    count_result = self.conn.execute(
        f"SELECT COUNT(*) FROM samples WHERE {where_clause}"
    ).fetchone()
    total = count_result[0] if count_result else 0

    # Get samples
    query = f"""
        SELECT id, task_type, num_turns, num_tool_calls, has_error,
               CAST(json_extract(tool_stats, '$.response_helpful_rate') AS DOUBLE) as helpful_rate,
               CAST(json_extract(tool_stats, '$.user_satisfied_rate') AS DOUBLE) as satisfied_rate,
               imported_at
        FROM samples
        WHERE {where_clause}
        ORDER BY id DESC
        LIMIT {limit} OFFSET {offset}
    """
    rows = self.conn.execute(query).fetchall()

    samples = []
    for row in rows:
        samples.append({
            "id": row[0],
            "task_type": row[1],
            "num_turns": row[2],
            "num_tool_calls": row[3],
            "has_error": row[4],
            "helpful_rate": row[5] or 0.0,
            "satisfied_rate": row[6] or 0.0,
            "imported_at": row[7],
        })

    return samples, total
```

- [ ] **Step 2: Create filter.py**

```python
"""Data filter page."""
import streamlit as st
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.config import DB_PATH
from claw_data_filter.web.components.sample_table import render_samples_table


def render():
    st.title("数据筛选")

    # Filter controls
    with st.form("filter_form"):
        col1, col2, col3 = st.columns(3)

        helpful_op = col1.selectbox("Helpful Rate", [">=", "<=", "=", "!="], index=0)
        helpful_val = col1.number_input("值", min_value=0.0, max_value=1.0, value=0.7, step=0.1)

        satisfied_op = col2.selectbox("Satisfied Rate", [">=", "<=", "=", "!="], index=0)
        satisfied_val = col2.number_input("值", min_value=0.0, max_value=1.0, value=0.5, step=0.1)

        task_type = col3.text_input("task_type", placeholder="coding, general, ...")

        col4, col5, col6 = st.columns(3)
        num_turns_min = col4.number_input("最小轮次", min_value=0, value=0)
        num_turns_max = col5.number_input("最大轮次", min_value=0, value=100)
        date_range = col6.date_input("日期范围", value=(None, None))

        col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 4])
        submitted = col_btn1.form_submit_button("应用筛选")
        reset = col_btn2.form_submit_button("重置")

    # Initialize session state
    if "filter_params" not in st.session_state:
        st.session_state.filter_params = {
            "helpful_op": ">=",
            "helpful_val": 0.7,
            "satisfied_op": ">=",
            "satisfied_val": 0.5,
            "task_type": "",
            "num_turns_min": 0,
            "num_turns_max": 100,
            "date_from": None,
            "date_to": None,
        }
    if "page" not in st.session_state:
        st.session_state.page = 1

    # Handle form submission
    if submitted:
        st.session_state.filter_params = {
            "helpful_op": helpful_op,
            "helpful_val": helpful_val,
            "satisfied_op": satisfied_op,
            "satisfied_val": satisfied_val,
            "task_type": task_type,
            "num_turns_min": num_turns_min,
            "num_turns_max": num_turns_max,
            "date_from": str(date_range[0]) if date_range and date_range[0] else None,
            "date_to": str(date_range[1]) if date_range and date_range[1] else None,
        }
        st.session_state.page = 1

    if reset:
        st.session_state.filter_params = {
            "helpful_op": ">=",
            "helpful_val": 0.7,
            "satisfied_op": ">=",
            "satisfied_val": 0.5,
            "task_type": "",
            "num_turns_min": 0,
            "num_turns_max": 100,
            "date_from": None,
            "date_to": None,
        }
        st.session_state.page = 1

    # Query data
    params = st.session_state.filter_params
    store = DuckDBStore(DB_PATH)

    page_size = 20
    offset = (st.session_state.page - 1) * page_size

    samples, total = store.filter_samples(
        helpful_rate_op=params["helpful_op"],
        helpful_rate_val=params["helpful_val"],
        satisfied_rate_op=params["satisfied_op"],
        satisfied_rate_val=params["satisfied_val"],
        task_type=params["task_type"] or None,
        num_turns_min=params["num_turns_min"] or None,
        num_turns_max=params["num_turns_max"] or None,
        date_from=params["date_from"],
        date_to=params["date_to"],
        limit=page_size,
        offset=offset,
    )

    total_pages = max(1, (total + page_size - 1) // page_size)

    st.divider()
    st.markdown(f"**共 {total} 条结果**")

    # Render table
    def on_detail(sample_id):
        st.query_params["page"] = "detail"
        st.query_params["sample_id"] = str(sample_id)
        st.rerun()

    render_samples_table(samples, st.session_state.page, total_pages, on_detail)
```

- [ ] **Step 3: Commit**

```bash
git add claw_data_filter/web/pages/filter.py
git add claw_data_filter/storage/duckdb_store.py
git commit -m "feat(web): data filter page with pagination"
```

---

## Task 6: Data Export Page

**Files:**
- Create: `claw_data_filter/web/pages/export.py`

- [ ] **Step 1: Create export.py**

```python
"""Data export page."""
import streamlit as st
from pathlib import Path
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.config import DB_PATH
from claw_data_filter.exporters.jsonl_exporter import JSONLExporter


def render():
    st.title("数据导出")

    # Filter controls (same as filter page)
    with st.form("export_form"):
        col1, col2, col3 = st.columns(3)

        helpful_op = col1.selectbox("Helpful Rate", [">=", "<=", "=", "!="], index=0)
        helpful_val = col1.number_input("值", min_value=0.0, max_value=1.0, value=0.7, step=0.1)

        satisfied_op = col2.selectbox("Satisfied Rate", [">=", "<=", "=", "!="], index=0)
        satisfied_val = col2.number_input("值", min_value=0.0, max_value=1.0, value=0.5, step=0.1)

        task_type = col3.text_input("task_type", placeholder="coding, general, ...")

        col4, col5 = st.columns(2)
        num_turns_min = col4.number_input("最小轮次", min_value=0, value=0)
        num_turns_max = col5.number_input("最大轮次", min_value=0, value=100)

        output_path = st.text_input("输出文件路径", value="data/exported.jsonl")

        col_btn1, col_btn2 = st.columns(2)
        preview = col_btn1.form_submit_button("预览数量")
        export = col_btn2.form_submit_button("导出")

    # Build filter conditions
    store = DuckDBStore(DB_PATH)
    conditions = ["tool_stats IS NOT NULL"]

    if helpful_val is not None:
        conditions.append(
            f"CAST(json_extract(tool_stats, '$.response_helpful_rate') AS DOUBLE) {helpful_op} {helpful_val}"
        )
    if satisfied_val is not None:
        conditions.append(
            f"CAST(json_extract(tool_stats, '$.user_satisfied_rate') AS DOUBLE) {satisfied_op} {satisfied_val}"
        )
    if task_type:
        conditions.append(f"task_type LIKE '%{task_type}%'")
    if num_turns_min > 0:
        conditions.append(f"num_turns >= {num_turns_min}")
    if num_turns_max < 100:
        conditions.append(f"num_turns <= {num_turns_max}")

    where_clause = " AND ".join(conditions)
    filter_query = where_clause

    if preview:
        count_result = store.conn.execute(
            f"SELECT COUNT(*) FROM samples WHERE {where_clause}"
        ).fetchone()
        count = count_result[0] if count_result else 0
        st.info(f"预览: 将导出 {count} 条数据")

    if export:
        try:
            exporter = JSONLExporter(store)
            output = Path(output_path)
            output.parent.mkdir(parents=True, exist_ok=True)
            count = exporter.export(output, filter_query=filter_query)
            st.success(f"成功导出 {count} 条数据到 {output_path}")
        except Exception as e:
            st.error(f"导出失败: {str(e)}")
```

- [ ] **Step 2: Commit**

```bash
git add claw_data_filter/web/pages/export.py
git commit -m "feat(web): data export page"
```

---

## Task 7: Table Schema Viewer Page

**Files:**
- Create: `claw_data_filter/web/pages/tables.py`

- [ ] **Step 1: Create tables.py**

```python
"""Table schema viewer page."""
import streamlit as st
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.config import DB_PATH


def render():
    st.title("数据表预览")

    store = DuckDBStore(DB_PATH)
    tables = store.get_table_list()

    selected_table = st.selectbox("选择表", tables)

    if selected_table:
        schema = store.get_table_schema(selected_table)

        st.markdown("**表结构**")
        for col in schema:
            st.markdown(f"- `{col['name']}` : {col['type']}")

        st.divider()

        # Data preview
        rows = store.conn.execute(
            f"SELECT * FROM {selected_table} LIMIT 100"
        ).fetchall()

        if rows:
            # Get column names
            cols = [desc[0] for desc in store.conn.description]

            st.markdown(f"**数据预览 (前 {len(rows)} 条)**")

            # Display as table
            import pandas as pd
            df = pd.DataFrame(rows, columns=cols)
            st.dataframe(df, use_container_width=True)
        else:
            st.info("表中无数据")
```

- [ ] **Step 2: Commit**

```bash
git add claw_data_filter/web/pages/tables.py
git commit -m "feat(web): table schema viewer page"
```

---

## Task 8: Sample Detail Page

**Files:**
- Create: `claw_data_filter/web/pages/sample_detail.py`

- [ ] **Step 1: Create sample_detail.py**

```python
"""Sample detail page - shows all turns for a sample."""
import streamlit as st
import json
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.web.config import DB_PATH


def render():
    st.title("Sample 详情")

    # Get sample_id from query params
    sample_id = st.query_params.get("sample_id")
    if not sample_id:
        st.error("未指定 sample_id")
        return

    store = DuckDBStore(DB_PATH)

    # Get sample data
    row = store.conn.execute(
        "SELECT * FROM samples WHERE id = ?", [int(sample_id)]
    ).fetchone()

    if not row:
        st.error(f"Sample {sample_id} 不存在")
        return

    # Column names
    cols = [desc[0] for desc in store.conn.description]
    sample = dict(zip(cols, row))

    # Info card
    st.markdown("### 基本信息")
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.markdown(f"**task_type:** {sample.get('task_type', '-')}")
    col2.markdown(f"**num_turns:** {sample.get('num_turns', 0)}")
    col3.markdown(f"**num_tool_calls:** {sample.get('num_tool_calls', 0)}")

    tool_stats = json.loads(sample.get('tool_stats', '{}')) if sample.get('tool_stats') else {}
    col4.markdown(f"**helpful_rate:** {tool_stats.get('response_helpful_rate', 0):.2f}")
    col5.markdown(f"**satisfied_rate:** {tool_stats.get('user_satisfied_rate', 0):.2f}")

    st.divider()

    # Back button
    if st.button("← 返回列表"):
        st.query_params["page"] = "filter"
        st.query_params.pop("sample_id", None)
        st.rerun()

    st.markdown("### Turn 数据")

    # Get turn judgments
    judgments = store.get_turn_judgments(int(sample_id))

    # Get raw messages from raw_json
    raw_json = json.loads(sample.get('raw_json', '{}'))
    messages = raw_json.get('request', {}).get('bodyJson', {}).get('messages', [])

    # Build turn index -> messages mapping
    # Each turn is a user message + assistant response pair
    turns = []
    msg_idx = 0
    turn_index = 0
    while msg_idx < len(messages):
        msg = messages[msg_idx]
        if msg.get('role') == 'user':
            # Find corresponding assistant response
            assistant_msg = None
            tool_calls = []
            if msg_idx + 1 < len(messages) and messages[msg_idx + 1].get('role') == 'assistant':
                assistant_msg = messages[msg_idx + 1]
                tool_calls = assistant_msg.get('tool_calls', [])

            turns.append({
                'turn_index': turn_index,
                'user_message': msg.get('content', ''),
                'assistant_message': assistant_msg.get('content', '') if assistant_msg else '',
                'tool_calls': tool_calls,
            })
            turn_index += 1
            msg_idx += 2 if assistant_msg else 1
        else:
            msg_idx += 1

    # Render turns
    for turn in turns:
        turn_index = turn['turn_index']

        # Find judgment for this turn
        judgment = next((j for j in judgments if j.turn_index == turn_index), None)

        helpful = judgment.response_helpful if judgment else "-"
        satisfied = judgment.user_satisfied if judgment else "-"
        signals = judgment.signal_from_users if judgment else []
        llm_error = judgment.llm_error if judgment else False

        helpful_color = "green" if helpful == "yes" else ("orange" if helpful == "uncertain" else "red")
        satisfied_color = "green" if satisfied == "yes" else ("gray" if satisfied == "neutral" else "red")

        with st.expander(f"**Turn {turn_index}** — helpful: :{helpful_color}[{helpful}] | satisfied: :{satisfied_color}[{satisfied}]"):
            st.markdown(f"**User:** {turn['user_message'][:200]}{'...' if len(turn['user_message']) > 200 else ''}")

            if turn['tool_calls']:
                st.markdown(f"**Assistant (tool_calls):** {len(turn['tool_calls'])} calls")
            else:
                st.markdown(f"**Assistant:** {turn['assistant_message'][:300]}{'...' if len(turn['assistant_message']) > 300 else ''}")

            if judgment:
                st.markdown(f"**Signal from users:** {signals if signals else '无'}")
                if llm_error:
                    st.error("LLM Error")
```

- [ ] **Step 2: Update app.py to handle detail page routing**

Modify `claw_data_filter/web/app.py` to add detail page routing:

```python
# After the existing page routing, add detail page check
sample_id = st.query_params.get("sample_id")
if sample_id:
    from claw_data_filter.web.pages import sample_detail
    sample_detail.render()
elif page == "📊 统计概览":
    ...
```

- [ ] **Step 3: Commit**

```bash
git add claw_data_filter/web/pages/sample_detail.py
git add claw_data_filter/web/app.py
git commit -m "feat(web): sample detail page with turn data"
```

---

## Task 9: Final Integration Test

- [ ] **Step 1: Test full app flow**

```bash
source .venv/bin/activate
streamlit run claw_data_filter/web/app.py --server.headless true
```

Test:
1. Statistics overview displays correctly
2. Filter page shows samples with pagination
3. Export generates valid JSONL
4. Table viewer shows schema
5. Sample detail shows all turns

- [ ] **Step 2: Commit any remaining changes**

```bash
git status
git add -A
git commit -m "feat(web): complete database visualization app"
```

---

## Verification

Run the app and verify all pages work:

```bash
streamlit run claw_data_filter/web/app.py --server.headless true
```

Open http://localhost:8501 and test:
- [ ] Sidebar navigation works
- [ ] Statistics display correctly
- [ ] Filter with criteria returns results
- [ ] Export creates valid JSONL file
- [ ] Table schema viewer shows tables
- [ ] Sample detail shows turn messages and judgments
