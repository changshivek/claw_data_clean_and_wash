# Agent Data Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a CLI tool that imports OpenAI-format agent conversation data into DuckDB, evaluates each interaction using local LLM, and provides filtered export with statistical reports.

**Architecture:** Single Python package with Click CLI. Three-stage pipeline: Import → Evaluate → Filter/Export. DuckDB for storage with parallel worker support.

**Tech Stack:** Python 3.10+, Click, DuckDB, httpx (for LLM API calls), pydantic

---

## File Structure

```
claw_data_filter/           # Package root
├── __init__.py
├── cli.py                  # Click CLI commands
├── config.py               # Configuration management
├── models/
│   ├── __init__.py
│   ├── sample.py            # Sample model (raw data)
│   └── evaluation.py        # Evaluation result model
├── importers/
│   ├── __init__.py
│   └── jsonl_importer.py    # JSONL import logic
├── processors/
│   ├── __init__.py
│   ├── formatter.py         # Strip system prompt, format conversation
│   └── evaluator.py         # LLM evaluation logic
├── storage/
│   ├── __init__.py
│   └── duckdb_store.py       # DuckDB operations
├── filters/
│   ├── __init__.py
│   └── query.py             # Filter query builder
├── exporters/
│   ├── __init__.py
│   ├── jsonl_exporter.py    # JSONL export
│   └── report_exporter.py   # Statistical report generation
├── prompts/
│   ├── __init__.py
│   └── evaluation_prompt.py # Evaluation prompt template
└── llm/
    ├── __init__.py
    └── client.py            # Local LLM API client
tests/
├── __init__.py
├── test_formatter.py
├── test_evaluator.py
├── test_jsonl_importer.py
├── test_duckdb_store.py
└── test_query_filter.py
pyproject.toml
```

---

## Task 1: Project Setup

**Files:**
- Create: `pyproject.toml`
- Create: `claw_data_filter/__init__.py`
- Create: `claw_data_filter/config.py`
- Create: `tests/__init__.py`

- [ ] **Step 1: Create pyproject.toml with dependencies**

```toml
[project]
name = "claw-data-filter"
version = "0.1.0"
description = "LLM-powered agent data filtering tool"
requires-python = ">=3.10"
dependencies = [
    "click>=8.1.0",
    "duckdb>=0.9.0",
    "httpx>=0.25.0",
    "pydantic>=2.0.0",
]

[project.optional-dependencies]
dev = ["pytest>=7.0", "pytest-asyncio>=0.21.0"]

[project.scripts]
claw-filter = "claw_data_filter.cli:main"

[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"
```

- [ ] **Step 2: Create config.py**

```python
"""Configuration management."""
import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel


class Config(BaseModel):
    llm_endpoint: str = "http://localhost:8000/v1"
    llm_api_key: Optional[str] = None
    db_path: Path = Path("./data.duckdb")
    worker_count: int = max(1, os.cpu_count() // 2)
    batch_size: int = 10
    max_retries: int = 3

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            llm_endpoint=os.getenv("LLM_ENDPOINT", "http://localhost:8000/v1"),
            llm_api_key=os.getenv("LLM_API_KEY"),
            db_path=Path(os.getenv("DB_PATH", "./data.duckdb")),
            worker_count=int(os.getenv("WORKER_COUNT", os.cpu_count() // 2)),
            batch_size=int(os.getenv("BATCH_SIZE", 10)),
            max_retries=int(os.getenv("MAX_RETRIES", 3)),
        )
```

- [ ] **Step 3: Run init test**

Run: `python -c "from claw_data_filter.config import Config; c = Config(); print('OK')"`
Expected: Output "OK"

- [ ] **Step 4: Commit**

```bash
git init
git add pyproject.toml claw_data_filter/__init__.py claw_data_filter/config.py tests/__init__.py
git commit -m "feat: project setup with config"
```

---

## Task 2: Data Models

**Files:**
- Create: `claw_data_filter/models/__init__.py`
- Create: `claw_data_filter/models/sample.py`
- Create: `claw_data_filter/models/evaluation.py`

- [ ] **Step 1: Write test for Sample model**

```python
# tests/test_models.py
import json
from claw_data_filter.models.sample import Sample

def test_sample_from_dict():
    raw = {
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "What's the weather?"},
            {"role": "assistant", "content": "Let me check...", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": "{}"}}
            ]},
            {"role": "user", "content": "Thanks"},
            {"role": "assistant", "content": "You're welcome."},
        ]
    }
    sample = Sample.from_dict(raw)
    assert sample.user_query == "What's the weather?"
    assert sample.num_turns == 2
    assert sample.num_tool_calls == 1
    assert sample.has_error is False  # no error in this sample
    print(f"user_query: {sample.user_query}")
    print(f"num_turns: {sample.num_turns}")
    print(f"num_tool_calls: {sample.num_tool_calls}")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_models.py -v`
Expected: FAIL - ModuleNotFoundError

- [ ] **Step 3: Write Sample model**

```python
# claw_data_filter/models/sample.py
from typing import Optional, Any
from datetime import datetime
from pydantic import BaseModel, Field
import json


class Sample(BaseModel):
    id: Optional[int] = None
    raw_json: dict[str, Any]
    user_query: str
    assistant_response: str
    num_turns: int
    num_tool_calls: int
    has_error: bool = False
    imported_at: datetime = Field(default_factory=datetime.now)

    @classmethod
    def from_dict(cls, data: dict) -> "Sample":
        """Parse from OpenAI format dict."""
        messages = data.get("messages", [])

        # Extract user query (last user message)
        user_query = ""
        for msg in reversed(messages):
            if msg.get("role") == "user":
                user_query = msg.get("content", "")
                break

        # Extract formatted assistant response
        assistant_parts = []
        tool_calls = []
        for msg in messages:
            if msg.get("role") == "assistant":
                content = msg.get("content", "")
                if content:
                    assistant_parts.append(content)
                tc = msg.get("tool_calls", [])
                tool_calls.extend(tc)

        assistant_response = "\n".join(assistant_parts)

        # Count turns (user-assistant pairs)
        num_turns = sum(1 for msg in messages if msg.get("role") == "user")

        # Count tool calls
        num_tool_calls = len(tool_calls)

        # Check for errors (would need tool result messages - simplified for now)
        # has_error = any tool call result contains error

        return cls(
            raw_json=data,
            user_query=user_query,
            assistant_response=assistant_response,
            num_turns=num_turns,
            num_tool_calls=num_tool_calls,
            has_error=False,  # Will be refined when tool results are tracked
        )
```

- [ ] **Step 4: Write Evaluation model**

```python
# claw_data_filter/models/evaluation.py
from typing import Optional
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class Evaluation(BaseModel):
    id: Optional[int] = None
    sample_id: int
    task_type: str
    progress_score: int = Field(ge=0, le=5)
    tool_quality_score: float = Field(ge=0.0, le=1.0)
    tool_success_rate: float = Field(ge=0.0, le=1.0)
    overall_score: float = Field(ge=0.0, le=10.0)
    reasoning: str
    evaluated_at: datetime = Field(default_factory=datetime.now)

    @field_validator("progress_score")
    @classmethod
    def validate_progress(cls, v: int) -> int:
        if v not in (0, 1, 2, 4, 5):  # 3 is reserved
            raise ValueError(f"Invalid progress_score: {v}")
        return v
```

- [ ] **Step 5: Run model tests**

Run: `pytest tests/test_models.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add claw_data_filter/models/ tests/test_models.py
git commit -m "feat: add Sample and Evaluation models"
```

---

## Task 3: DuckDB Storage Layer

**Files:**
- Create: `claw_data_filter/storage/__init__.py`
- Create: `claw_data_filter/storage/duckdb_store.py`
- Create: `tests/test_duckdb_store.py`

- [ ] **Step 1: Write failing test for DuckDB store**

```python
# tests/test_duckdb_store.py
import tempfile
from pathlib import Path
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.models.sample import Sample


def test_store_and_retrieve_samples():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        store = DuckDBStore(db_path)

        raw = {
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
            ]
        }
        sample = Sample.from_dict(raw)

        # Insert
        store.insert_sample(sample)

        # Retrieve
        samples = store.get_samples(limit=10)
        assert len(samples) == 1
        assert samples[0].user_query == "Hello"

        # Check count
        count = store.get_sample_count()
        assert count == 1

        store.close()
        print("test_store_and_retrieve_samples passed")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_duckdb_store.py -v`
Expected: FAIL - ModuleNotFoundError

- [ ] **Step 3: Write DuckDB store**

```python
# claw_data_filter/storage/duckdb_store.py
"""DuckDB storage layer for samples and evaluations."""
import json
from pathlib import Path
from typing import Optional
import duckdb
from datetime import datetime

from claw_data_filter.models.sample import Sample
from claw_data_filter.models.evaluation import Evaluation


class DuckDBStore:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = duckdb.connect(str(db_path))
        self._init_schema()

    def _init_schema(self):
        """Create tables if not exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS samples (
                id INTEGER PRIMARY KEY,
                raw_json JSON,
                user_query TEXT,
                assistant_response TEXT,
                num_turns INTEGER,
                num_tool_calls INTEGER,
                has_error BOOLEAN,
                imported_at TIMESTAMP
            )
        """)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS evaluations (
                id INTEGER PRIMARY KEY,
                sample_id INTEGER REFERENCES samples(id),
                task_type TEXT,
                progress_score INTEGER,
                tool_quality_score DOUBLE,
                tool_success_rate DOUBLE,
                overall_score DOUBLE,
                reasoning TEXT,
                evaluated_at TIMESTAMP
            )
        """)
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS sample_id_seq")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS eval_id_seq")

    def insert_sample(self, sample: Sample) -> int:
        """Insert sample, return auto-generated id."""
        result = self.conn.execute(
            "SELECT nextval('sample_id_seq')"
        ).fetchone()
        sample_id = result[0]

        self.conn.execute(
            """
            INSERT INTO samples (id, raw_json, user_query, assistant_response, num_turns, num_tool_calls, has_error, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                sample_id,
                json.dumps(sample.raw_json),
                sample.user_query,
                sample.assistant_response,
                sample.num_turns,
                sample.num_tool_calls,
                sample.has_error,
                datetime.now(),
            ],
        )
        return sample_id

    def get_samples(self, limit: int = 100, offset: int = 0, evaluated_only: bool = False) -> list[Sample]:
        """Get samples with optional evaluation filter."""
        query = "SELECT raw_json FROM samples"
        if evaluated_only:
            query += " WHERE id IN (SELECT sample_id FROM evaluations)"
        query += f" LIMIT {limit} OFFSET {offset}"

        rows = self.conn.execute(query).fetchall()
        return [Sample.from_dict(json.loads(row[0])) for row in rows]

    def get_unevaluated_samples(self, limit: int = 100) -> list[tuple[int, Sample]]:
        """Get samples that haven't been evaluated yet. Returns (id, sample) tuples."""
        rows = self.conn.execute(
            """
            SELECT s.id, s.raw_json
            FROM samples s
            LEFT JOIN evaluations e ON s.id = e.sample_id
            WHERE e.id IS NULL
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [(row[0], Sample.from_dict(json.loads(row[1]))) for row in rows]

    def insert_evaluation(self, evaluation: Evaluation) -> int:
        """Insert evaluation, return auto-generated id."""
        result = self.conn.execute("SELECT nextval('eval_id_seq')").fetchone()
        eval_id = result[0]

        self.conn.execute(
            """
            INSERT INTO evaluations (id, sample_id, task_type, progress_score, tool_quality_score, tool_success_rate, overall_score, reasoning, evaluated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                eval_id,
                evaluation.sample_id,
                evaluation.task_type,
                evaluation.progress_score,
                evaluation.tool_quality_score,
                evaluation.tool_success_rate,
                evaluation.overall_score,
                evaluation.reasoning,
                datetime.now(),
            ],
        )
        return eval_id

    def get_sample_count(self) -> int:
        """Get total sample count."""
        result = self.conn.execute("SELECT COUNT(*) FROM samples").fetchone()
        return result[0] if result else 0

    def get_evaluation_count(self) -> int:
        """Get total evaluation count."""
        result = self.conn.execute("SELECT COUNT(*) FROM evaluations").fetchone()
        return result[0] if result else 0

    def get_stats(self) -> dict:
        """Get statistics about samples and evaluations."""
        sample_count = self.get_sample_count()
        eval_count = self.get_evaluation_count()

        progress_stats = self.conn.execute(
            "SELECT AVG(progress_score), AVG(tool_quality_score), AVG(tool_success_rate), AVG(overall_score) FROM evaluations"
        ).fetchone()

        return {
            "total_samples": sample_count,
            "total_evaluations": eval_count,
            "avg_progress_score": progress_stats[0] if progress_stats[0] is not None else 0,
            "avg_tool_quality": progress_stats[1] if progress_stats[1] is not None else 0,
            "avg_tool_success_rate": progress_stats[2] if progress_stats[2] is not None else 0,
            "avg_overall_score": progress_stats[3] if progress_stats[3] is not None else 0,
        }

    def close(self):
        """Close connection."""
        self.conn.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_duckdb_store.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add claw_data_filter/storage/ tests/test_duckdb_store.py
git commit -m "feat: add DuckDB storage layer"
```

---

## Task 4: JSONL Importer

**Files:**
- Create: `claw_data_filter/importers/__init__.py`
- Create: `claw_data_filter/importers/jsonl_importer.py`
- Create: `tests/test_jsonl_importer.py`

- [ ] **Step 1: Write failing test for JSONL importer**

```python
# tests/test_jsonl_importer.py
import tempfile
import json
from pathlib import Path
from claw_data_filter.importers.jsonl_importer import JSONLImporter


def test_import_single_line():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        input_file = Path(tmpdir) / "input.jsonl"

        # Write test data
        with open(input_file, "w") as f:
            f.write(json.dumps({
                "messages": [
                    {"role": "user", "content": "What's the weather?"},
                    {"role": "assistant", "content": "Let me check"},
                ]
            }) + "\n")

        importer = JSONLImporter(db_path)
        count = importer.import_file(input_file)

        assert count == 1
        assert importer.store.get_sample_count() == 1

        importer.close()
        print("test_import_single_line passed")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_jsonl_importer.py -v`
Expected: FAIL - ModuleNotFoundError

- [ ] **Step 3: Write JSONL importer**

```python
# claw_data_filter/importers/jsonl_importer.py
"""JSONL file importer."""
import json
import logging
from pathlib import Path
from typing import Iterator

from claw_data_filter.models.sample import Sample
from claw_data_filter.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


class JSONLImporter:
    def __init__(self, db_path: Path):
        self.store = DuckDBStore(db_path)

    def import_file(self, input_path: Path, skip_errors: bool = True) -> int:
        """Import JSONL file, return count of imported samples."""
        count = 0
        errors = 0

        with open(input_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    sample = Sample.from_dict(data)
                    self.store.insert_sample(sample)
                    count += 1
                except json.JSONDecodeError as e:
                    errors += 1
                    logger.error(f"Line {line_num}: JSON decode error: {e}")
                    if not skip_errors:
                        raise
                except Exception as e:
                    errors += 1
                    logger.error(f"Line {line_num}: Error: {e}")
                    if not skip_errors:
                        raise

        logger.info(f"Imported {count} samples, {errors} errors")
        return count

    def import_lines(self, lines: Iterator[str], skip_errors: bool = True) -> int:
        """Import from iterator of lines (for streaming)."""
        count = 0
        errors = 0

        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue

            try:
                data = json.loads(line)
                sample = Sample.from_dict(data)
                self.store.insert_sample(sample)
                count += 1
            except Exception as e:
                errors += 1
                logger.error(f"Line {line_num}: Error: {e}")
                if not skip_errors:
                    raise

        return count

    def close(self):
        """Close underlying store."""
        self.store.close()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_jsonl_importer.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add claw_data_filter/importers/ tests/test_jsonl_importer.py
git commit -m "feat: add JSONL importer"
```

---

## Task 5: Formatter (System Prompt Stripper)

**Files:**
- Create: `claw_data_filter/processors/formatter.py`
- Create: `tests/test_formatter.py`

- [ ] **Step 1: Write failing test for formatter**

```python
# tests/test_formatter.py
from claw_data_filter.processors.formatter import ConversationFormatter


def test_format_conversation_strips_system():
    formatter = ConversationFormatter()

    raw = {
        "messages": [
            {"role": "system", "content": "You are a very long system prompt with detailed instructions..."},
            {"role": "user", "content": "What's the weather in SF?"},
            {"role": "assistant", "content": "Let me check the weather API.", "tool_calls": [
                {"id": "call_1", "type": "function", "function": {"name": "get_weather", "arguments": '{"city": "San Francisco"}'}}
            ]},
            {"role": "tool", "content": '{"temp": 72}', "tool_call_id": "call_1"},
            {"role": "assistant", "content": "It's 72°F in San Francisco."},
        ]
    }

    formatted = formatter.format(raw)

    # Should not contain system prompt
    assert "system prompt" not in formatted.lower()
    assert "What's the weather in SF?" in formatted
    assert "get_weather" in formatted
    assert "72°F" in formatted
    print(formatted)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_formatter.py -v`
Expected: FAIL

- [ ] **Step 3: Write formatter**

```python
# claw_data_filter/processors/formatter.py
"""Conversation formatter - strips system prompt, formats for readability."""
import json
from typing import Any


ROLE_LABELS = {
    "user": "User",
    "assistant": "Assistant",
    "tool": "Tool Result",
    "system": "System",
}


class ConversationFormatter:
    def format(self, raw_conversation: dict[str, Any]) -> str:
        """Format conversation for LLM evaluation prompt."""
        messages = raw_conversation.get("messages", [])
        parts = []

        for msg in messages:
            role = msg.get("role", "unknown")

            # Skip system messages
            if role == "system":
                continue

            label = ROLE_LABELS.get(role, role.capitalize())
            content = msg.get("content", "")

            # Handle tool calls
            tool_calls = msg.get("tool_calls", [])
            if tool_calls:
                tc_parts = []
                for tc in tool_calls:
                    func = tc.get("function", {})
                    name = func.get("name", "unknown")
                    args = func.get("arguments", "{}")
                    # Pretty-print JSON arguments
                    try:
                        args_dict = json.loads(args)
                        args_str = json.dumps(args_dict, indent=2)
                    except json.JSONDecodeError:
                        args_str = args
                    tc_parts.append(f"  - {name}({args_str})")
                content = content + "\nTool calls:\n" + "\n".join(tc_parts) if content else "Tool calls:\n" + "\n".join(tc_parts)

            # Handle tool result
            if role == "tool":
                content = f"[Result]: {content}"

            parts.append(f"{label}: {content}")

        return "\n\n".join(parts)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_formatter.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add claw_data_filter/processors/formatter.py tests/test_formatter.py
git commit -m "feat: add conversation formatter"
```

---

## Task 6: LLM Client

**Files:**
- Create: `claw_data_filter/llm/__init__.py`
- Create: `claw_data_filter/llm/client.py`

- [ ] **Step 1: Write LLM client skeleton (test with mocked HTTP)**

```python
# claw_data_filter/llm/client.py
"""Local LLM API client (vLLM/Ollama compatible)."""
import json
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)


class LLMClient:
    def __init__(
        self,
        endpoint: str = "http://localhost:8000/v1",
        api_key: str | None = None,
        model: str | None = None,
        max_retries: int = 3,
    ):
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        self.model = model
        self.max_retries = max_retries
        self.timeout = 120.0  # 2 minutes for evaluation

        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self.client = httpx.Client(timeout=self.timeout, headers=headers)

    def chat(self, messages: list[dict[str, str]], *, temperature: float = 0.1) -> str:
        """Send chat request, return assistant message content."""
        payload: dict[str, Any] = {
            "messages": messages,
            "temperature": temperature,
        }
        if self.model:
            payload["model"] = self.model

        for attempt in range(self.max_retries):
            try:
                response = self.client.post(
                    f"{self.endpoint}/chat/completions",
                    json=payload,
                )
                response.raise_for_status()
                data = response.json()
                return data["choices"][0]["message"]["content"]
            except httpx.HTTPStatusError as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # exponential backoff
                else:
                    raise

        raise RuntimeError("Should not reach here")

    def close(self):
        """Close HTTP client."""
        self.client.close()
```

- [ ] **Step 2: Write basic connection test (skip if no server)**

Run: `python -c "from claw_data_filter.llm.client import LLMClient; print('LLMClient imported OK')"`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add claw_data_filter/llm/client.py
git commit -m "feat: add LLM client for vLLM/Ollama"
```

---

## Task 7: Evaluation Prompt & Evaluator

**Files:**
- Create: `claw_data_filter/prompts/evaluation_prompt.py`
- Modify: `claw_data_filter/processors/evaluator.py`
- Create: `tests/test_evaluator.py`

- [ ] **Step 1: Write evaluation prompt template**

```python
# claw_data_filter/prompts/evaluation_prompt.py
"""Evaluation prompt template."""

EVALUATION_SYSTEM_PROMPT = """You are an expert evaluator of AI agent conversations.
Your task is to evaluate the quality of agent interactions across multiple dimensions.
Be precise and critical in your assessment.

Respond ONLY with valid JSON in this exact format:
{
  "task_type": "one of: information_retrieval, data_processing, coding, reasoning, creative, general",
  "progress_score": 0-5,
  "tool_quality_score": 0.0-1.0,
  "tool_success_rate": 0.0-1.0,
  "overall_score": 0.0-10.0,
  "reasoning": "brief explanation of your assessment"
}"""

EVALUATION_USER_PROMPT_TEMPLATE = """Please evaluate this AI Agent conversation:

{conversation}

---
Evaluate the conversation according to these criteria:

1. **Task Type**: Classify the type of task performed.

2. **Progress Score** (0-5):
   - 0: Wrong direction or endless loop (no useful progress)
   - 1: Reasonable attempt but no significant progress
   - 2: Correct direction, proper tool use, significant progress
   - 4: Successfully completed with trial-and-error in tool usage
   - 5: Successfully completed, all steps correct, no tool errors

3. **Tool Quality Score** (0.0-1.0):
   - 0.0: Repeated tool parameter errors, poor understanding
   - 1.0: Correct tool parameter understanding throughout

4. **Tool Success Rate** (0.0-1.0):
   - Ratio of successful tool calls to total tool calls

5. **Overall Score** (0.0-10.0): Composite assessment

Output JSON only:"""


def build_evaluation_prompt(conversation: str) -> tuple[str, str]:
    """Build (system_prompt, user_prompt) for evaluation."""
    system_prompt = EVALUATION_SYSTEM_PROMPT
    user_prompt = EVALUATION_USER_PROMPT_TEMPLATE.format(conversation=conversation)
    return system_prompt, user_prompt
```

- [ ] **Step 2: Write evaluator processor**

```python
# claw_data_filter/processors/evaluator.py
"""LLM-based evaluation processor."""
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Iterator

from claw_data_filter.config import Config
from claw_data_filter.models.evaluation import Evaluation
from claw_data_filter.models.sample import Sample
from claw_data_filter.processors.formatter import ConversationFormatter
from claw_data_filter.prompts.evaluation_prompt import build_evaluation_prompt
from claw_data_filter.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


class EvaluationError(Exception):
    """Raised when evaluation fails."""
    pass


class Evaluator:
    def __init__(self, store: DuckDBStore, config: Config):
        self.store = store
        self.config = config
        self.formatter = ConversationFormatter()

        # Lazy import to avoid circular deps
        from claw_data_filter.llm.client import LLMClient
        self.llm = LLMClient(
            endpoint=config.llm_endpoint,
            api_key=config.llm_api_key,
            max_retries=config.max_retries,
        )

    def _parse_evaluation_response(self, raw_response: str, sample_id: int) -> Evaluation:
        """Parse LLM JSON response into Evaluation model."""
        # Try to extract JSON from response (may have surrounding text)
        json_match = re.search(r"\{[^{}]*\}", raw_response, re.DOTALL)
        if not json_match:
            # Try simpler approach - find first { and last }
            start = raw_response.find("{")
            end = raw_response.rfind("}") + 1
            if start != -1 and end != 0:
                json_str = raw_response[start:end]
            else:
                raise EvaluationError(f"Could not find JSON in response for sample {sample_id}")
        else:
            json_str = json_match.group()

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            raise EvaluationError(f"Invalid JSON in response for sample {sample_id}: {e}")

        return Evaluation(
            sample_id=sample_id,
            task_type=data.get("task_type", "unknown"),
            progress_score=data.get("progress_score", 0),
            tool_quality_score=data.get("tool_quality_score", 0.0),
            tool_success_rate=data.get("tool_success_rate", 0.0),
            overall_score=data.get("overall_score", 0.0),
            reasoning=data.get("reasoning", ""),
        )

    def evaluate_sample(self, sample_id: int, sample: Sample) -> Evaluation:
        """Evaluate a single sample."""
        formatted = self.formatter.format(sample.raw_json)
        system_prompt, user_prompt = build_evaluation_prompt(formatted)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]

        response = self.llm.chat(messages)
        evaluation = self._parse_evaluation_response(response, sample_id)
        self.store.insert_evaluation(evaluation)

        return evaluation

    def evaluate_batch(self, workers: int = 4) -> tuple[int, int]:
        """Evaluate all unevaluated samples using parallel workers.

        Returns (success_count, failure_count).
        """
        success = 0
        failures = 0

        while True:
            # Fetch batch of unevaluated
            batch = self.store.get_unevaluated_samples(limit=self.config.batch_size)
            if not batch:
                break

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(self.evaluate_sample, sample_id, sample): sample_id
                    for sample_id, sample in batch
                }

                for future in as_completed(futures):
                    sample_id = futures[future]
                    try:
                        future.result()
                        success += 1
                        logger.info(f"Evaluated sample {sample_id}")
                    except Exception as e:
                        failures += 1
                        logger.error(f"Failed to evaluate sample {sample_id}: {e}")

        logger.info(f"Evaluation complete: {success} success, {failures} failures")
        return success, failures

    def close(self):
        """Close resources."""
        self.llm.close()
```

- [ ] **Step 3: Commit prompt module**

```bash
git add claw_data_filter/prompts/evaluation_prompt.py
git commit -m "feat: add evaluation prompt template"
```

---

## Task 8: Filter & Query

**Files:**
- Create: `claw_data_filter/filters/query.py`
- Create: `tests/test_query_filter.py`

- [ ] **Step 1: Write filter query builder**

```python
# claw_data_filter/filters/query.py
"""Filter query builder for selecting samples by evaluation criteria."""
import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ComparisonOp(Enum):
    EQ = "="
    NE = "!="
    GT = ">"
    GTE = ">="
    LT = "<"
    LTE = "<="


@dataclass
class FilterCondition:
    field: str
    op: ComparisonOp
    value: float | int | str

    def to_sql(self) -> str:
        if isinstance(self.value, str):
            return f'{self.field} {self.op.value} \'{self.value}\''
        return f"{self.field} {self.op.value} {self.value}"


class FilterQueryBuilder:
    """Build SQL WHERE clauses from filter conditions."""

    OPERATOR_PATTERN = re.compile(r"^(\w+)\s*(>=|<=|>|<|!=|=)\s*(.+)$")

    def __init__(self):
        self.conditions: list[FilterCondition] = []
        self.task_types: list[str] = []

    def add_condition(self, field: str, op: ComparisonOp, value):
        self.conditions.append(FilterCondition(field, op, value))
        return self

    def add_progress_score_filter(self, expr: str) -> "FilterQueryBuilder":
        """Parse expression like '>=4' and add filter."""
        match = self.OPERATOR_PATTERN.match(expr.strip())
        if not match:
            raise ValueError(f"Invalid filter expression: {expr}")

        field, op_str, value_str = match.groups()
        op = ComparisonOp(op_str)
        value = float(value_str) if "." in value_str else int(value_str)

        return self.add_condition(field, op, value)

    def add_task_type_filter(self, task_types: list[str]) -> "FilterQueryBuilder":
        """Filter by task type(s)."""
        self.task_types.extend(task_types)
        return self

    def build_where_clause(self) -> str:
        """Build WHERE clause SQL fragment."""
        parts = []

        for cond in self.conditions:
            parts.append(cond.to_sql())

        if self.task_types:
            types_str = ", ".join(f"'{t}'" for t in self.task_types)
            parts.append(f"task_type IN ({types_str})")

        return " AND ".join(parts) if parts else "1=1"

    def get_filtered_samples_query(self, limit: Optional[int] = None) -> str:
        """Build complete SELECT query with filters."""
        where = self.build_where_clause()
        limit_str = f"LIMIT {limit}" if limit else ""

        return f"""
            SELECT s.id, s.raw_json, e.*
            FROM samples s
            JOIN evaluations e ON s.id = e.sample_id
            WHERE {where}
            {limit_str}
        """
```

- [ ] **Step 2: Write test for filter query builder**

```python
# tests/test_query_filter.py
from claw_data_filter.filters.query import FilterQueryBuilder


def test_filter_builder_basic():
    builder = FilterQueryBuilder()
    builder.add_condition("progress_score", ">=", 4)
    builder.add_condition("overall_score", ">", 7.0)

    where = builder.build_where_clause()
    assert "progress_score >= 4" in where
    assert "overall_score > 7.0" in where
    print(f"WHERE clause: {where}")


def test_filter_builder_expression_parsing():
    builder = FilterQueryBuilder()
    builder.add_progress_score_filter(">=4")
    builder.add_progress_score_filter("<5")

    where = builder.build_where_clause()
    assert ">=" in where
    assert "<" in where
    print(f"WHERE clause: {where}")
```

- [ ] **Step 3: Run tests**

Run: `pytest tests/test_query_filter.py -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add claw_data_filter/filters/query.py tests/test_query_filter.py
git commit -m "feat: add filter query builder"
```

---

## Task 9: Exporters (JSONL + Report)

**Files:**
- Create: `claw_data_filter/exporters/jsonl_exporter.py`
- Create: `claw_data_filter/exporters/report_exporter.py`

- [ ] **Step 1: Write JSONL exporter**

```python
# claw_data_filter/exporters/jsonl_exporter.py
"""JSONL export functionality."""
import json
import logging
from pathlib import Path
from typing import Iterator

from claw_data_filter.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


class JSONLExporter:
    def __init__(self, store: DuckDBStore):
        self.store = store

    def export(
        self,
        output_path: Path,
        filter_query: str | None = None,
        limit: int | None = None,
    ) -> int:
        """Export filtered samples to JSONL file. Returns count."""
        count = 0

        if filter_query:
            query = f"""
                SELECT s.raw_json
                FROM samples s
                JOIN evaluations e ON s.id = e.sample_id
                WHERE {filter_query}
            """
            if limit:
                query += f" LIMIT {limit}"
            rows = self.store.conn.execute(query).fetchall()
        else:
            rows = self.store.conn.execute(
                f"SELECT raw_json FROM samples LIMIT {limit or 'ALL'}"
            ).fetchall()

        with open(output_path, "w", encoding="utf-8") as f:
            for row in rows:
                raw_json = json.loads(row[0])
                f.write(json.dumps(raw_json, ensure_ascii=False) + "\n")
                count += 1

        logger.info(f"Exported {count} samples to {output_path}")
        return count
```

- [ ] **Step 2: Write report exporter**

```python
# claw_data_filter/exporters/report_exporter.py
"""Statistical report generation."""
import json
import logging
from pathlib import Path
from typing import Any

from claw_data_filter.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


class ReportExporter:
    def __init__(self, store: DuckDBStore):
        self.store = store

    def generate_report(self) -> dict[str, Any]:
        """Generate statistical report from evaluations."""
        stats = self.store.get_stats()

        # Distribution of progress scores
        progress_dist = self.store.conn.execute("""
            SELECT progress_score, COUNT(*)
            FROM evaluations
            GROUP BY progress_score
            ORDER BY progress_score
        """).fetchall()

        # Task type distribution
        task_dist = self.store.conn.execute("""
            SELECT task_type, COUNT(*)
            FROM evaluations
            GROUP BY task_type
        """).fetchall()

        # Score percentiles
        percentiles = self.store.conn.execute("""
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY overall_score) as p25,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY overall_score) as p50,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY overall_score) as p75
            FROM evaluations
        """).fetchone()

        report: dict[str, Any] = {
            "summary": {
                "total_samples": stats["total_samples"],
                "total_evaluations": stats["total_evaluations"],
                "evaluation_rate": (
                    stats["total_evaluations"] / stats["total_samples"]
                    if stats["total_samples"] > 0 else 0
                ),
            },
            "averages": {
                "progress_score": round(stats["avg_progress_score"], 2),
                "tool_quality": round(stats["avg_tool_quality"], 2),
                "tool_success_rate": round(stats["avg_tool_success_rate"], 2),
                "overall_score": round(stats["avg_overall_score"], 2),
            },
            "progress_score_distribution": {
                str(row[0]): row[1] for row in progress_dist
            },
            "task_type_distribution": {
                row[0]: row[1] for row in task_dist
            },
            "overall_score_percentiles": {
                "p25": round(percentiles[0], 2) if percentiles[0] else 0,
                "p50": round(percentiles[1], 2) if percentiles[1] else 0,
                "p75": round(percentiles[2], 2) if percentiles[2] else 0,
            },
        }

        return report

    def export_report(self, output_path: Path) -> None:
        """Generate and save report to JSON file."""
        report = self.generate_report()
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info(f"Report exported to {output_path}")
```

- [ ] **Step 3: Commit**

```bash
git add claw_data_filter/exporters/jsonl_exporter.py claw_data_filter/exporters/report_exporter.py
git commit -m "feat: add JSONL and report exporters"
```

---

## Task 10: CLI Assembly

**Files:**
- Modify: `claw_data_filter/__init__.py`
- Create: `claw_data_filter/cli.py`
- Modify: `pyproject.toml` (add console_scripts)

- [ ] **Step 1: Write Click CLI**

```python
# claw_data_filter/cli.py
"""Click CLI for agent data filter tool."""
import logging
import sys
from pathlib import Path

import click

from claw_data_filter.config import Config
from claw_data_filter.exporters.jsonl_exporter import JSONLExporter
from claw_data_filter.exporters.report_exporter import ReportExporter
from claw_data_filter.filters.query import FilterQueryBuilder
from claw_data_filter.importers.jsonl_importer import JSONLImporter
from claw_data_filter.processors.evaluator import Evaluator
from claw_data_filter.storage.duckdb_store import DuckDBStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--db-path", type=click.Path(), default="./data.duckdb", help="DuckDB database path")
@click.option("--llm-endpoint", type=str, default=None, help="LLM API endpoint")
@click.pass_context
def cli(ctx, db_path, llm_endpoint):
    """Agent Data Filter - LLM-powered agent conversation filtering."""
    config = Config.from_env()
    if db_path:
        config.db_path = Path(db_path)
    if llm_endpoint:
        config.llm_endpoint = llm_endpoint
    ctx.obj["config"] = config


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.pass_context
def import_cmd(ctx, input_file):
    """Import JSONL data file into database."""
    config = ctx.obj["config"]
    click.echo(f"Importing {input_file}...")

    importer = JSONLImporter(config.db_path)
    try:
        count = importer.import_file(Path(input_file))
        click.echo(f"Successfully imported {count} samples.")
    finally:
        importer.close()


@cli.command()
@click.option("--workers", type=int, default=None, help="Number of parallel workers")
@click.option("--batch-size", type=int, default=None, help="Batch size per worker")
@click.pass_context
def evaluate(ctx, workers, batch_size):
    """Evaluate all unevaluated samples using LLM."""
    config = ctx.obj["config"]
    if workers:
        config.worker_count = workers
    if batch_size:
        config.batch_size = batch_size

    click.echo(f"Starting evaluation with {config.worker_count} workers...")

    store = DuckDBStore(config.db_path)
    evaluator = Evaluator(store, config)

    try:
        success, failures = evaluator.evaluate_batch(workers=config.worker_count)
        click.echo(f"Evaluation complete: {success} success, {failures} failures")
    finally:
        evaluator.close()
        store.close()


@cli.command()
@click.option("--progress-score", type=str, help="Filter by progress score (e.g., '>=4')")
@click.option("--overall-score", type=str, help="Filter by overall score (e.g., '>7')")
@click.option("--task-type", type=str, multiple=True, help="Filter by task type")
@click.option("--export", type=click.Path(), required=True, help="Output JSONL file")
@click.option("--report", type=click.Path(), help="Output report JSON file")
@click.option("--limit", type=int, help="Limit number of results")
@click.pass_context
def filter_cmd(ctx, progress_score, overall_score, task_type, export, report, limit):
    """Filter samples and export to JSONL with optional report."""
    config = ctx.obj["config"]

    builder = FilterQueryBuilder()
    if progress_score:
        builder.add_progress_score_filter(progress_score)
    if overall_score:
        builder.add_progress_score_filter(overall_score)
    if task_type:
        builder.add_task_type_filter(list(task_type))

    where_clause = builder.build_where_clause()

    store = DuckDBStore(config.db_path)
    try:
        exporter = JSONLExporter(store)
        count = exporter.export(Path(export), filter_query=where_clause, limit=limit)
        click.echo(f"Exported {count} samples to {export}")

        if report:
            report_exporter = ReportExporter(store)
            report_exporter.export_report(Path(report))
            click.echo(f"Report saved to {report}")
    finally:
        store.close()


@cli.command()
@click.pass_context
def stats(ctx):
    """Show statistics about imported data and evaluations."""
    config = ctx.obj["config"]
    store = DuckDBStore(config.db_path)

    try:
        stats_data = store.get_stats()
        click.echo("=== Statistics ===")
        click.echo(f"Total samples: {stats_data['total_samples']}")
        click.echo(f"Total evaluations: {stats_data['total_evaluations']}")
        if stats_data['total_evaluations'] > 0:
            click.echo(f"Avg progress score: {stats_data['avg_progress_score']:.2f}")
            click.echo(f"Avg tool quality: {stats_data['avg_tool_quality']:.2f}")
            click.echo(f"Avg tool success rate: {stats_data['avg_tool_success_rate']:.2f}")
            click.echo(f"Avg overall score: {stats_data['avg_overall_score']:.2f}")
    finally:
        store.close()


@cli.command()
@click.pass_context
def info(ctx):
    """Show database information."""
    config = ctx.obj["config"]
    store = DuckDBStore(config.db_path)

    try:
        click.echo(f"Database path: {config.db_path}")
        click.echo(f"Sample count: {store.get_sample_count()}")
        click.echo(f"Evaluation count: {store.get_evaluation_count()}")
    finally:
        store.close()


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Update pyproject.toml console_scripts**

```toml
[project.scripts]
claw-filter = "claw_data_filter.cli:main"
```

- [ ] **Step 3: Test CLI loads**

Run: `python -c "from claw_data_filter.cli import cli; print('CLI loaded OK')"`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add claw_data_filter/cli.py pyproject.toml
git commit -m "feat: add Click CLI with all commands"
```

---

## Task 11: Integration Test

**Files:**
- Create: `tests/test_integration.py`

- [ ] **Step 1: Write integration test (requires LLM server)**

```python
# tests/test_integration.py
"""Full pipeline integration test (requires running LLM server)."""
import json
import tempfile
from pathlib import Path
import pytest


@pytest.mark.skipif(
    os.getenv("SKIP_INTEGRATION") == "1",
    reason="Integration test requires running LLM server"
)
def test_full_pipeline():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        input_file = Path(tmpdir) / "input.jsonl"
        output_file = Path(tmpdir) / "output.jsonl"
        report_file = Path(tmpdir) / "report.json"

        # Write test data
        with open(input_file, "w") as f:
            f.write(json.dumps({
                "messages": [
                    {"role": "user", "content": "What's 2+2?"},
                    {"role": "assistant", "content": "2+2 equals 4."},
                ]
            }) + "\n")

        # Import
        from claw_data_filter.importers.jsonl_importer import JSONLImporter
        importer = JSONLImporter(db_path)
        importer.import_file(input_file)
        importer.close()

        # Evaluate (requires LLM server)
        from claw_data_filter.config import Config
        from claw_data_filter.processors.evaluator import Evaluator
        from claw_data_filter.storage.duckdb_store import DuckDBStore

        config = Config()
        config.db_path = db_path
        store = DuckDBStore(db_path)
        evaluator = Evaluator(store, config)
        evaluator.evaluate_batch(workers=1)
        evaluator.close()
        store.close()

        # Verify
        store = DuckDBStore(db_path)
        stats = store.get_stats()
        assert stats["total_evaluations"] == 1
        store.close()

        print("Full pipeline integration test passed")
```

- [ ] **Step 2: Commit**

```bash
git add tests/test_integration.py
git commit -m "test: add integration test"
```

---

## Task 12: Package Installation Test

- [ ] **Step 1: Install package in editable mode**

Run: `pip install -e .`
Expected: Installation succeeds, `claw-filter --help` works

- [ ] **Step 2: Test --help**

Run: `claw-filter --help`
Expected: Shows usage information with all commands

---

## Summary

| Task | Files | Status |
|------|-------|--------|
| 1: Project Setup | pyproject.toml, config.py | ⬜ |
| 2: Data Models | models/sample.py, models/evaluation.py | ⬜ |
| 3: DuckDB Storage | storage/duckdb_store.py | ⬜ |
| 4: JSONL Importer | importers/jsonl_importer.py | ⬜ |
| 5: Formatter | processors/formatter.py | ⬜ |
| 6: LLM Client | llm/client.py | ⬜ |
| 7: Evaluator + Prompts | processors/evaluator.py, prompts/evaluation_prompt.py | ⬜ |
| 8: Filter Query | filters/query.py | ⬜ |
| 9: Exporters | exporters/jsonl_exporter.py, exporters/report_exporter.py | ⬜ |
| 10: CLI | cli.py | ⬜ |
| 11: Integration Test | tests/test_integration.py | ⬜ |
| 12: Package Install | - | ⬜ |
