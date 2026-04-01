# Claw Data Filter

LLM-powered agent conversation data filtering tool. Import, evaluate, filter, and analyze OpenAI-format agent interaction data using local LLM models.

## Features

- **Import**: Load JSONL files with OpenAI-format conversations into DuckDB
- **Evaluate**: Use local LLM (vLLM/Ollama) to assess conversation quality
- **Round Feedback**: Per-turn quality judgments with 4-dimension analysis (need_tool, tool_correct, response_helpful, user_satisfied)
- **Filter**: Query by progress score, tool quality, task type, etc.
- **Export**: Export filtered data as JSONL with statistical reports

## Installation

```bash
pip install -e .
```

Requires Python 3.10+.

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_ENDPOINT` | `http://localhost:8000/v1` | LLM API endpoint |
| `LLM_API_KEY` | - | API key (optional) |
| `DB_PATH` | `./data.duckdb` | Database path |
| `WORKER_COUNT` | CPU cores / 2 | Parallel evaluation workers |
| `BATCH_SIZE` | 10 | Batch size per worker |
| `MAX_RETRIES` | 3 | LLM API retry attempts |
| `MAX_CONCURRENCY` | 10 | Max concurrent LLM calls for round feedback |
| `LLM_TIMEOUT` | 60.0 | LLM API timeout (seconds) |
| `CONTEXT_WINDOW` | 4096 | LLM context window size (tokens) |

## Usage

### 1. Import Data

```bash
claw-filter import data.jsonl
```

### 2. Evaluate (requires LLM server)

```bash
# Run with default settings (4 workers)
claw-filter evaluate

# Custom workers and batch size
claw-filter evaluate --workers 8 --batch-size 20
```

### 3. Round Feedback (per-turn quality judgments)

```bash
# Run pressure test first to verify LLM stability
claw-filter pressure-test

# Process round-level feedback judgments
claw-filter round-feedback --workers 10 --batch-size 5
```

### 4. Filter and Export

```bash
# Export high-quality conversations (progress_score >= 4)
claw-filter filter --progress-score ">=4" --export filtered.jsonl

# Filter by overall score and task type
claw-filter filter --overall-score ">7" --task-type coding --export high-quality.jsonl --report report.json
```

### 5. View Statistics

```bash
claw-filter stats
```

### 6. Database Info

```bash
claw-filter info
```

## Evaluation Dimensions

### Session-Level Evaluation

| Dimension | Score | Description |
|-----------|-------|-------------|
| **Progress** | 0-5 | Task completion progress |
| **Tool Quality** | 0.0-1.0 | Tool parameter understanding |
| **Tool Success** | 0.0-1.0 | Tool call success rate |
| **Overall** | 0.0-10.0 | Composite quality score |

### Turn-Level Feedback (Round Feedback)

Each assistant turn is judged on 4 dimensions:

| Dimension | Values | Description |
|-----------|--------|-------------|
| **need_tool** | yes/no/uncertain | Does the question need tool calling? |
| **tool_correct** | yes/no/uncertain | If tools were used, was the choice correct? |
| **response_helpful** | yes/no/uncertain | Was the response helpful to the user? |
| **user_satisfied** | yes/no/uncertain/neutral | Is the user satisfied based on follow-up? |

Signal attribution: User's subsequent messages (up to 3) are used to determine `user_satisfied`:
- User follows up with clarification → satisfied=no
- User confirms/continues → satisfied=yes
- User switches to unrelated topic → satisfied=neutral
- No clear signal → satisfied=uncertain

### Progress Score Scale

| Score | Description |
|-------|-------------|
| 0 | Wrong direction or endless loop |
| 1 | Reasonable attempt, no significant progress |
| 2 | Correct direction, proper tool use, significant progress |
| 4 | Successfully completed with trial-and-error |
| 5 | Successfully completed, all steps correct |

## Data Format

### Standard OpenAI Format

Input JSONL files should contain OpenAI chat format:

```json
{"messages": [
  {"role": "system", "content": "You are a helpful assistant..."},
  {"role": "user", "content": "User query"},
  {"role": "assistant", "content": "Response", "tool_calls": [...]},
  {"role": "tool", "content": "Tool result", "tool_call_id": "..."}
]}
```

The system prompt is automatically stripped during evaluation to prevent bias.

### UniRouter Format (with request wrapper)

For UniRouter format (`items.jsonl`), the messages are nested:

```json
{
  "log": {...},
  "request": {
    "bodyJson": {
      "messages": [...]
    }
  },
  "response": {...}
}
```

RoundFeedbackProcessor automatically extracts messages from `request.bodyJson.messages`.

## Architecture

```
claw_data_filter/
├── cli.py              # Click CLI commands
├── config.py          # Configuration
├── models/            # Data models (Sample, Evaluation, RoundJudgment)
├── importers/         # JSONL import
├── processors/        # Formatter, Evaluator, RoundFeedback
│   └── round_feedback.py   # TurnContextBuilder, RoundJudgmentProcessor,
│                           # ToolStatsAggregator, PressureTest
├── storage/           # DuckDB operations (samples, evaluations, turn_judgments)
├── filters/           # Query builder
├── exporters/         # JSONL & report export
├── prompts/           # LLM evaluation prompts
└── llm/               # LLM clients (sync & async)
```

## Development

```bash
# Install in dev mode
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Skip integration tests (require LLM server)
SKIP_INTEGRATION=1 pytest tests/ -v
```

## License

MIT
