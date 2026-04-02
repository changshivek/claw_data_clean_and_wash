# Simplified Round Feedback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove Evaluation layer, simplify RoundFeedback to two judgments (response_helpful, user_satisfied), unify data format at import.

**Architecture:** This refactoring removes the evaluation system (models/evaluation.py, processors/evaluator.py, prompts/evaluation_prompt.py) entirely. The round_judgment model is simplified to only response_helpful and user_satisfied. Format conversion (Anthropic→OpenAI) is added to Sample.from_dict(). The filter system is updated to query samples.tool_stats JSON fields.

**Tech Stack:** Python 3.12+, DuckDB, Pydantic, Click, httpx

---

## File Structure After Changes

```
claw_data_filter/
├── models/
│   ├── sample.py          # Add format detection/conversion
│   ├── round_judgment.py  # Remove need_tool, tool_correct
│   └── __init__.py
├── storage/
│   └── duckdb_store.py    # Add task_type, modify tool_stats
├── processors/
│   └── round_feedback.py  # Simplify to 2 judgments
├── filters/
│   └── query.py           # Update allowed fields
├── exporters/
│   ├── jsonl_exporter.py  # Remove evaluations JOIN
│   └── report_exporter.py # Update stats query
├── importers/
│   └── jsonl_importer.py
├── llm/
│   ├── client.py
│   └── async_client.py
├── cli.py                 # Remove evaluate, update filter/stats
└── config.py

# DELETED:
├── models/evaluation.py
├── processors/evaluator.py
└── prompts/evaluation_prompt.py

tests/
├── test_round_judgment.py # Update for simplified model
├── test_round_feedback.py # Update for 2-judgment system
├── test_evaluator.py      # DELETE
└── test_integration.py    # Update
```

---

## Task 1: Simplify RoundJudgment Model

**Files:**
- Modify: `claw_data_filter/models/round_judgment.py:1-24`
- Test: `tests/test_round_judgment.py`

- [ ] **Step 1: Write failing test for simplified RoundJudgment**

```python
# tests/test_round_judgment.py
def test_round_judgment_simplified():
    """Test RoundJudgment only has response_helpful and user_satisfied"""
    from claw_data_filter.models.round_judgment import RoundJudgment

    j = RoundJudgment(
        sample_id=1,
        turn_index=0,
        response_helpful="yes",
        user_satisfied="no",
        signal_from_users=["用户确认"],
        llm_error=False,
    )
    assert j.response_helpful == "yes"
    assert j.user_satisfied == "no"
    # These fields should not exist
    assert not hasattr(j, 'need_tool')
    assert not hasattr(j, 'tool_correct')
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_round_judgment.py::test_round_judgment_simplified -v`
Expected: FAIL - AssertionError: hasattr returned True

- [ ] **Step 3: Write simplified RoundJudgment model**

```python
# claw_data_filter/models/round_judgment.py
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field


class JudgmentValue(str, Enum):
    YES = "yes"
    NO = "no"
    UNCERTAIN = "uncertain"
    NEUTRAL = "neutral"  # For user_satisfied only


class RoundJudgment(BaseModel):
    """单轮判断结果（简化版）"""

    sample_id: int
    turn_index: int
    response_helpful: Optional[str] = None  # yes/no/uncertain
    user_satisfied: Optional[str] = None    # yes/no/uncertain/neutral
    signal_from_users: list[str] = Field(default_factory=list)
    llm_error: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now())
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_round_judgment.py::test_round_judgment_simplified -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add claw_data_filter/models/round_judgment.py tests/test_round_judgment.py
git commit -m "refactor: simplify RoundJudgment to response_helpful and user_satisfied only"
```

---

## Task 2: Add Format Detection and Conversion to Sample

**Files:**
- Modify: `claw_data_filter/models/sample.py:1-99`
- Test: `tests/test_models.py`

- [ ] **Step 1: Write failing test for Anthropic format detection**

```python
# tests/test_models.py
def test_sample_detect_anthropic_format():
    """Test Sample.from_dict detects Anthropic format"""
    anthropic_data = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "hello"}]},
            {"role": "assistant", "content": "hi"},
        ]
    }
    s = Sample.from_dict(anthropic_data)
    assert s.num_turns == 1

def test_sample_detect_openai_format():
    """Test Sample.from_dict detects OpenAI format"""
    openai_data = {
        "messages": [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi"},
        ]
    }
    s = Sample.from_dict(openai_data)
    assert s.num_turns == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_models.py::test_sample_detect_anthropic_format -v`
Expected: FAIL (current code doesn't handle Anthropic content list)

- [ ] **Step 3: Write format detection and conversion in Sample**

```python
# claw_data_filter/models/sample.py

def _detect_format(messages: list) -> str:
    """检测消息格式：返回 'openai' 或 'anthropic'"""
    for msg in messages:
        if msg.get("role") == "tool":
            return "openai"
        content = msg.get("content", [])
        if isinstance(content, list):
            for c in content:
                if isinstance(c, dict) and c.get("type") == "tool_result":
                    return "anthropic"
    return "openai"


def _anthropic_to_openai(messages: list) -> list:
    """将 Anthropic 格式转换为 OpenAI 格式"""
    result = []
    for msg in messages:
        role = msg.get("role")
        content = msg.get("content", [])

        if role == "user" and isinstance(content, list):
            tool_results = [c for c in content if c.get("type") == "tool_result"]
            text_parts = [c.get("text") for c in content if c.get("type") == "text" and c.get("text")]

            # 先输出 tool 消息
            for tr in tool_results:
                result.append({
                    "role": "tool",
                    "tool_call_id": tr.get("tool_use_id"),
                    "content": tr.get("content", "")
                })
            # 再输出 user 消息（只保留 text 部分）
            if text_parts:
                result.append({"role": "user", "content": "".join(text_parts)})
        else:
            result.append(msg)
    return result


def _extract_text_content(content: Any) -> str:
    """Extract text from content field.

    Handles two formats:
    - Plain string: "Hello"
    - List of content parts: [{"type": "text", "text": "Hello"}, ...]
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for part in content:
            if isinstance(part, dict):
                if part.get("type") == "text":
                    parts.append(part.get("text", ""))
                elif part.get("type") == "image_url":
                    parts.append("[image]")
        return "".join(parts)
    return str(content) if content else ""


class Sample(BaseModel):
    # ... existing fields ...

    @classmethod
    def from_dict(cls, data: dict) -> "Sample":
        """Parse from OpenAI or Anthropic format dict.

        Automatically detects and converts Anthropic format to OpenAI.
        """
        messages = data.get("messages", [])

        # Detect and convert format if needed
        if _detect_format(messages) == "anthropic":
            messages = _anthropic_to_openai(messages)

        # ... rest of existing logic using _extract_text_content helper ...
```

- [ ] **Step 3b: Update existing _extract_text_content to use the module-level function**

The existing `_extract_text_content` in Sample should be updated or the module-level function should replace it.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_models.py::test_sample_detect_anthropic_format tests/test_models.py::test_sample_detect_openai_format -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add claw_data_filter/models/sample.py tests/test_models.py
git commit -m "feat: add Anthropic to OpenAI format conversion at import"
```

---

## Task 3: Update DuckDBStore Schema

**Files:**
- Modify: `claw_data_filter/storage/duckdb_store.py`
- Test: `tests/test_duckdb_store.py`

- [ ] **Step 1: Write failing test for task_type column**

```python
# tests/test_duckdb_store.py
def test_samples_has_task_type_column(temp_db_path):
    """Test samples table has task_type column"""
    store = DuckDBStore(temp_db_path)
    result = store.conn.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'samples' AND column_name = 'task_type'
    """).fetchone()
    assert result is not None, "task_type column should exist"
    store.close()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_duckdb_store.py::test_samples_has_task_type_column -v`
Expected: FAIL - task_type column doesn't exist

- [ ] **Step 3: Update DuckDBStore schema**

```python
# claw_data_filter/storage/duckdb_store.py - init_schema method

def init_schema(self):
    """Create tables and sequences if not exist."""
    # Samples table
    self.conn.execute("""
        CREATE TABLE IF NOT EXISTS samples (
            id INTEGER PRIMARY KEY,
            raw_json JSON,
            user_query TEXT,
            assistant_response TEXT,
            num_turns INTEGER,
            num_tool_calls INTEGER,
            has_error BOOLEAN,
            imported_at TIMESTAMP,
            tool_stats JSON,
            task_type TEXT
        )
    """)

    # Migration: add columns if they don't exist
    for col, col_type in [("tool_stats", "JSON"), ("task_type", "TEXT")]:
        try:
            self.conn.execute(f"ALTER TABLE samples ADD COLUMN {col} {col_type}")
        except:
            pass  # Column may already exist

    # Drop evaluations table completely
    self.conn.execute("DROP TABLE IF EXISTS evaluations")

    # Turn judgments table (simplified schema)
    self.conn.execute("""
        CREATE TABLE IF NOT EXISTS turn_judgments (
            id INTEGER PRIMARY KEY,
            sample_id INTEGER,
            turn_index INTEGER,
            response_helpful TEXT,
            user_satisfied TEXT,
            signal_from_users JSON,
            llm_error BOOLEAN,
            created_at TIMESTAMP
        )
    """)

    # ... rest of sequences and indexes ...
```

- [ ] **Step 4: Update get_stats method**

```python
def get_stats(self) -> dict:
    """Get statistics about samples and turn judgments."""
    sample_count = self.get_sample_count()

    # Aggregate from samples.tool_stats
    stats = self.conn.execute("""
        SELECT
            COUNT(*) as total,
            AVG(json_extract(tool_stats, '$.response_helpful_rate')) as avg_helpful,
            AVG(json_extract(tool_stats, '$.user_satisfied_rate')) as avg_satisfied,
            SUM(CASE WHEN json_extract(tool_stats, '$.has_error') = true THEN 1 ELSE 0 END) as error_count
        FROM samples
        WHERE tool_stats IS NOT NULL
    """).fetchone()

    return {
        "total_samples": sample_count,
        "avg_response_helpful_rate": stats[1] or 0,
        "avg_user_satisfied_rate": stats[2] or 0,
        "error_count": stats[3] or 0,
    }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `pytest tests/test_duckdb_store.py::test_samples_has_task_type_column -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add claw_data_filter/storage/duckdb_store.py tests/test_duckdb_store.py
git commit -m "refactor: update schema - add task_type, drop evaluations, simplify tool_stats"
```

---

## Task 4: Simplify RoundFeedback Processor

**Files:**
- Modify: `claw_data_filter/processors/round_feedback.py`
- Test: `tests/test_round_feedback.py`

- [ ] **Step 1: Write failing test for simplified response parsing**

```python
# tests/test_round_feedback.py
def test_parse_simplified_response():
    """Test parsing simplified response with only 2 judgments"""
    from claw_data_filter.processors.round_feedback import RoundJudgmentProcessor

    # Mock LLM client
    class MockLLM:
        async def chat(self, messages, max_tokens=50):
            return "response_helpful=yes; user_satisfied=no"

    processor = RoundJudgmentProcessor(MockLLM())
    result = processor._parse_response("response_helpful=yes; user_satisfied=no")
    assert result == {"response_helpful": "yes", "user_satisfied": "no"}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_round_feedback.py::test_parse_simplified_response -v`
Expected: FAIL - current parser expects 4 fields

- [ ] **Step 3: Simplify RoundJudgmentProcessor**

Remove `judge_group1`, `_parse_group1_response`, and related logic. Keep only `judge_group2` (renamed to `judge`) and `_parse_response`.

```python
# claw_data_filter/processors/round_feedback.py

class RoundJudgmentProcessor:
    """异步执行单轮2维度判断"""

    def __init__(self, llm_client, max_retries: int = 2):
        self.llm = llm_client
        self.max_retries = max_retries

    async def judge(self, prompt: str) -> dict | None:
        """执行判断"""
        return await self._call_llm_with_retry(prompt, self._parse_response)

    async def _call_llm_with_retry(self, prompt: str, parser) -> dict | None:
        # ... existing retry logic ...

    def _parse_response(self, response: str) -> dict | None:
        """解析响应：response_helpful=yes; user_satisfied=no"""
        response = response.strip()
        result = {}

        helpful_match = re.search(r"response_helpful\s*=\s*(yes|no|uncertain)", response, re.IGNORECASE)
        if helpful_match:
            result["response_helpful"] = helpful_match.group(1).lower()
        else:
            return None

        satisfied_match = re.search(r"user_satisfied\s*=\s*(yes|no|uncertain|neutral)", response, re.IGNORECASE)
        if satisfied_match:
            result["user_satisfied"] = satisfied_match.group(1).lower()
        else:
            return None

        return result

    async def process_turn(self, turn: TurnContext, all_turns: list[TurnContext], builder: TurnContextBuilder) -> RoundJudgment:
        """处理单个turn，返回判断结果"""
        prompt = builder.build_judgment_prompt(turn, all_turns)
        result = await self.judge(prompt)

        llm_error = result is None

        return RoundJudgment(
            sample_id=0,
            turn_index=turn.turn_index,
            response_helpful=result.get("response_helpful") if result else None,
            user_satisfied=result.get("user_satisfied") if result else None,
            signal_from_users=turn.signal_users,
            llm_error=llm_error,
        )
```

- [ ] **Step 4: Simplify TurnContextBuilder**

```python
class TurnContextBuilder:
    # ... existing extract_turns and helper methods ...

    def build_judgment_prompt(self, turn: TurnContext, all_turns: list[TurnContext]) -> str:
        """构建判断prompt（简化版：只判断 response_helpful 和 user_satisfied）"""
        # Build current turn (user + tool_result + assistant)
        current_parts = []
        if turn.user_message:
            current_parts.append(self._format_message("user", turn.user_message))
        if turn.tool_result:
            current_parts.append(f"[tool_result]: {turn.tool_result}")
        if turn.assistant_message:
            current_parts.append(self._format_message("assistant", turn.assistant_message))

        current_section = "\n".join(current_parts)

        # Build signal users
        signal_section = "\n".join([f"[user]: {u}" for u in turn.signal_users]) if turn.signal_users else "(无后续用户消息)"

        prompt = f"""=== 当前轮 ===
{current_section}

=== 后续用户信号（最多3轮）===
{signal_section}

请判断：
1. response_helpful: 这个回答对用户有帮助吗？（yes/no/uncertain）
2. user_satisfied: 用户对助手回复满意吗？（yes/no/uncertain/neutral）

答案格式：response_helpful=yes; user_satisfied=no

注意：
- 用户追问（要求补充/澄清） → user_satisfied=no
- 用户确认/继续/满意 → user_satisfied=yes
- 用户转向新话题 → user_satisfied=neutral
- 无明确反馈 → user_satisfied=uncertain"""

        return prompt
```

- [ ] **Step 5: Simplify ToolStatsAggregator**

```python
class ToolStatsAggregator:
    """从逐轮判断结果汇总统计"""

    @staticmethod
    def aggregate(judgments: list[RoundJudgment]) -> dict:
        """汇总判断结果，生成 tool_stats

        Returns:
            {
                "response_helpful_rate": float,
                "user_satisfied_rate": float,
                "total_turns": int,
                "has_error": bool,
            }
        """
        if not judgments:
            return {
                "response_helpful_rate": 0,
                "user_satisfied_rate": 0,
                "total_turns": 0,
                "has_error": False,
            }

        total = len(judgments)
        helpful_yes = sum(1 for j in judgments if j.response_helpful == "yes")
        satisfied_yes = sum(1 for j in judgments if j.user_satisfied == "yes")

        return {
            "response_helpful_rate": helpful_yes / total,
            "user_satisfied_rate": satisfied_yes / total,
            "total_turns": total,
            "has_error": any(j.llm_error for j in judgments),
        }
```

- [ ] **Step 6: Update RoundFeedbackProcessor.process_sample**

Change to use the new simplified aggregator and update the tool_stats structure.

- [ ] **Step 7: Run test to verify it passes**

Run: `pytest tests/test_round_feedback.py::test_parse_simplified_response -v`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add claw_data_filter/processors/round_feedback.py tests/test_round_feedback.py
git commit -m "refactor: simplify round feedback to 2 judgments (response_helpful, user_satisfied)"
```

---

## Task 5: Update CLI - Remove evaluate, modify filter/stats

**Files:**
- Modify: `claw_data_filter/cli.py`
- Test: `tests/test_integration.py`

- [ ] **Step 1: Write failing test for removed evaluate command**

```python
# tests/test_integration.py
def test_evaluate_command_removed():
    """Test that evaluate command is no longer available"""
    from click.testing import CliRunner
    from claw_data_filter.cli import cli

    runner = CliRunner()
    result = runner.invoke(cli, ['evaluate'])
    # Should fail because command doesn't exist
    assert result.exit_code != 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_integration.py::test_evaluate_command_removed -v`
Expected: FAIL - current code has evaluate command

- [ ] **Step 3: Update CLI**

```python
# claw_data_filter/cli.py

# REMOVE: from claw_data_filter.processors.evaluator import Evaluator

# REMOVE entire evaluate command

# UPDATE stats command
@cli.command()
@click.pass_context
def stats(ctx):
    """Show statistics about imported data and round judgments."""
    config = ctx.obj["config"]
    store = DuckDBStore(config.db_path)

    try:
        stats_data = store.get_stats()
        click.echo("=== Statistics ===")
        click.echo(f"Total samples: {stats_data['total_samples']}")
        if stats_data['total_samples'] > 0:
            click.echo(f"Avg response helpful rate: {stats_data['avg_response_helpful_rate']:.2f}")
            click.echo(f"Avg user satisfied rate: {stats_data['avg_user_satisfied_rate']:.2f}")
            click.echo(f"Error count: {stats_data['error_count']}")
    finally:
        store.close()

# UPDATE filter command to use new tool_stats fields
# ALLOWED_FIELDS will be updated in query.py
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_integration.py::test_evaluate_command_removed -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add claw_data_filter/cli.py tests/test_integration.py
git commit -m "refactor: remove evaluate command, update stats for round feedback"
```

---

## Task 6: Update Filter/Query

**Files:**
- Modify: `claw_data_filter/filters/query.py`

- [ ] **Step 1: Write failing test for new filter fields**

```python
# tests/test_query_filter.py
def test_filter_tool_stats_fields():
    """Test filtering by tool_stats fields"""
    from claw_data_filter.filters.query import FilterQueryBuilder

    builder = FilterQueryBuilder()
    builder.add_condition("response_helpful_rate", ComparisonOp(">="), 0.8)
    sql = builder.build_where_clause()
    assert "response_helpful_rate" in sql
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_query_filter.py::test_filter_tool_stats_fields -v`
Expected: FAIL - current code doesn't support this field

- [ ] **Step 3: Update query.py**

```python
# claw_data_filter/filters/query.py

ALLOWED_FIELDS = frozenset([
    "task_type",
    "response_helpful_rate",
    "user_satisfied_rate",
    "has_error",
    "num_turns",
    "num_tool_calls",
])

# Update get_filtered_samples_query to JOIN with turn_judgments for tool_stats
def get_filtered_samples_query(self, limit: Optional[int] = None) -> str:
    """Build complete SELECT query with filters.

    For tool_stats fields, extracts from JSON.
    """
    where = self.build_where_clause()
    limit_str = f"LIMIT {limit}" if limit else ""

    # Check if any tool_stats fields are used
    tool_stats_fields = ["response_helpful_rate", "user_satisfied_rate"]
    uses_tool_stats = any(
        cond.field in tool_stats_fields
        for cond in self.conditions
    )

    if uses_tool_stats:
        # Need to join samples with aggregated tool_stats
        select_parts = []
        for cond in self.conditions:
            if cond.field == "response_helpful_rate":
                select_parts.append(f"AVG(CASE WHEN tj.response_helpful = 'yes' THEN 1.0 ELSE 0.0 END) as helpful_rate")
            elif cond.field == "user_satisfied_rate":
                select_parts.append(f"AVG(CASE WHEN tj.user_satisfied = 'yes' THEN 1.0 ELSE 0.0 END) as satisfied_rate")

        return f"""
            SELECT s.id, s.raw_json, s.tool_stats
            FROM samples s
            LEFT JOIN turn_judgments tj ON s.id = tj.sample_id
            GROUP BY s.id, s.raw_json, s.tool_stats
            HAVING {where}
            {limit_str}
        """
    else:
        return f"""
            SELECT s.id, s.raw_json, s.tool_stats
            FROM samples s
            WHERE {where}
            {limit_str}
        """
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_query_filter.py::test_filter_tool_stats_fields -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add claw_data_filter/filters/query.py tests/test_query_filter.py
git commit -m "refactor: update filter to query tool_stats from turn_judgments"
```

---

## Task 7: Update Exporters

**Files:**
- Modify: `claw_data_filter/exporters/jsonl_exporter.py`
- Modify: `claw_data_filter/exporters/report_exporter.py`

- [ ] **Step 1: Write failing test for exporter without evaluations**

```python
# tests/test_exporters.py
def test_jsonl_exporter_no_eval_join():
    """Test exporter doesn't require evaluations table"""
    # Should work without evaluations table
```

- [ ] **Step 2: Update jsonl_exporter.py**

Remove the JOIN with evaluations, use samples only:

```python
# claw_data_filter/exporters/jsonl_exporter.py

def export(self, output_path: Path, filter_query: str | None = None, limit: int | None = None) -> int:
    # Remove JOIN with evaluations
    if filter_query:
        # Parse filter_query to determine if tool_stats aggregation needed
        query = f"SELECT raw_json, tool_stats FROM samples WHERE {filter_query}"
    else:
        query = "SELECT raw_json, tool_stats FROM samples"

    if limit:
        query += f" LIMIT {limit}"

    rows = self.store.conn.execute(query).fetchall()
    # ... rest unchanged ...
```

- [ ] **Step 3: Update report_exporter.py**

```python
# claw_data_filter/exporters/report_exporter.py - update stats queries

def export_report(self, output_path: Path) -> dict:
    """Export statistical report."""
    stats = self.store.get_stats()
    # Remove task_type_distribution and progress_dist from evaluations
    # Keep only samples-based stats
    report = {
        "total_samples": stats["total_samples"],
        "avg_response_helpful_rate": stats["avg_response_helpful_rate"],
        "avg_user_satisfied_rate": stats["avg_user_satisfied_rate"],
        "error_count": stats["error_count"],
        "generated_at": datetime.now().isoformat(),
    }
    # ... save report ...
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/test_exporters.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add claw_data_filter/exporters/jsonl_exporter.py claw_data_filter/exporters/report_exporter.py tests/test_exporters.py
git commit -m "refactor: remove evaluations from exporters, use samples + tool_stats only"
```

---

## Task 8: Delete Evaluation Files

**Files:**
- Delete: `claw_data_filter/models/evaluation.py`
- Delete: `claw_data_filter/processors/evaluator.py`
- Delete: `claw_data_filter/prompts/evaluation_prompt.py`
- Delete: `tests/test_evaluator.py`

- [ ] **Step 1: Delete files**

```bash
rm claw_data_filter/models/evaluation.py
rm claw_data_filter/processors/evaluator.py
rm claw_data_filter/prompts/evaluation_prompt.py
rm tests/test_evaluator.py
```

- [ ] **Step 2: Verify tests still pass**

Run: `pytest tests/ -v --ignore=tests/test_evaluator.py`
Expected: All other tests pass

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "refactor: delete evaluation module files"
```

---

## Task 9: Update PressureTest

**Files:**
- Modify: `claw_data_filter/processors/round_feedback.py` (PressureTest class)

- [ ] **Step 1: Update pressure test prompt**

```python
# In PressureTest._send_request, update prompt to use simplified format
async def _send_request(self) -> tuple[bool, float]:
    response = await self.llm.chat(
        [{"role": "user", "content": "判断：response_helpful=yes; user_satisfied=no 对应格式是否正确？"}],
        max_tokens=20,
    )
    latency = time.perf_counter() - start
    return "response_helpful=yes" in response, latency
```

- [ ] **Step 2: Commit**

```bash
git add claw_data_filter/processors/round_feedback.py
git commit -m "refactor: update pressure test to use simplified judgment format"
```

---

## Task 10: Final Integration Test

**Files:**
- Test: `tests/test_integration.py`

- [ ] **Step 1: Run full integration test**

```bash
# Clean up test database
rm -f data/test_integration.duckdb

# Run integration test
pytest tests/test_integration.py -v
```

Expected: All tests pass with simplified architecture

- [ ] **Step 2: Commit**

```bash
git add -A
git commit -m "test: add integration tests for simplified round feedback flow"
```
