# Claw Data Filter

LLM-powered agent conversation data filtering tool. Import, filter, and analyze OpenAI-format (and Anthropic-format) agent interaction data using local LLM models.

## Features

- **Import**: Load JSONL files with OpenAI-format or Anthropic-format conversations into DuckDB
- **Round Feedback**: Per-turn quality judgments with 2-dimension analysis (response_helpful, user_satisfied)
- **Filter**: Query by response_helpful_rate, user_satisfied_rate, task_type, etc.
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
| `WORKER_COUNT` | CPU cores / 2 | Parallel workers for round feedback |
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

### 2. Round Feedback (per-turn quality judgments)

```bash
# Run pressure test first to verify LLM stability
claw-filter pressure-test

# Process round-level feedback judgments
claw-filter round-feedback --workers 10 --batch-size 5
```

### 3. Filter and Export

```bash
# Export high-quality conversations (response_helpful_rate >= 0.7)
claw-filter filter --response-helpful-rate ">=0.7" --export filtered.jsonl

# Filter by user satisfied rate and task type
claw-filter filter --user-satisfied-rate ">=0.7" --task-type coding --export high-quality.jsonl --report report.json
```

### 4. View Statistics

```bash
claw-filter stats
```

### 5. Database Info

```bash
claw-filter info
```

## Round Feedback Dimensions

Each assistant turn is judged on 2 dimensions:

| Dimension | Values | Description |
|-----------|--------|-------------|
| **response_helpful** | yes/no/uncertain | Was the response helpful to the user? |
| **user_satisfied** | yes/no/uncertain/neutral | Is the user satisfied based on follow-up? |

### Signal Attribution for user_satisfied

User's subsequent messages (up to 3) are used to determine `user_satisfied`:
- User follows up with clarification → satisfied=no
- User confirms/continues → satisfied=yes
- User switches to unrelated topic → satisfied=neutral
- No clear signal → satisfied=uncertain

### Aggregated Scores

Per-sample aggregated scores are stored in `tool_stats` JSON:

| Field | Type | Description |
|-------|------|-------------|
| **response_helpful_rate** | float | Ratio of "yes" responses among non-uncertain |
| **user_satisfied_rate** | float | Ratio of "yes" responses among non-uncertain/neutral |
| **total_turns** | int | Total assistant turns |
| **has_error** | bool | Any LLM errors occurred |

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
├── models/            # Data models (Sample, RoundJudgment)
├── importers/         # JSONL import
├── processors/        # RoundFeedback processor
│   └── round_feedback.py   # TurnContextBuilder, RoundJudgmentProcessor,
│                           # ToolStatsAggregator, PressureTest
├── storage/           # DuckDB operations (samples, turn_judgments)
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
