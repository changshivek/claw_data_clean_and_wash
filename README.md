# Claw Data Filter

LLM-powered agent conversation data filtering tool. Import, evaluate, and filter OpenAI-format agent interaction data using local LLM models.

## Features

- **Import**: Load JSONL files with OpenAI-format conversations into DuckDB
- **Evaluate**: Use local LLM (vLLM/Ollama) to assess conversation quality
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

### 3. Filter and Export

```bash
# Export high-quality conversations (progress_score >= 4)
claw-filter filter --progress-score ">=4" --export filtered.jsonl

# Filter by overall score and task type
claw-filter filter --overall-score ">7" --task-type coding --export high-quality.jsonl --report report.json
```

### 4. View Statistics

```bash
claw-filter stats
```

### 5. Database Info

```bash
claw-filter info
```

## Evaluation Dimensions

| Dimension | Score | Description |
|-----------|-------|-------------|
| **Progress** | 0-5 | Task completion progress |
| **Tool Quality** | 0.0-1.0 | Tool parameter understanding |
| **Tool Success** | 0.0-1.0 | Tool call success rate |
| **Overall** | 0.0-10.0 | Composite quality score |

### Progress Score Scale

| Score | Description |
|-------|-------------|
| 0 | Wrong direction or endless loop |
| 1 | Reasonable attempt, no significant progress |
| 2 | Correct direction, proper tool use, significant progress |
| 4 | Successfully completed with trial-and-error |
| 5 | Successfully completed, all steps correct |

## Data Format

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

## Architecture

```
claw_data_filter/
├── cli.py              # Click CLI commands
├── config.py          # Configuration
├── models/            # Data models (Sample, Evaluation)
├── importers/         # JSONL import
├── processors/        # Formatter, Evaluator
├── storage/           # DuckDB operations
├── filters/           # Query builder
├── exporters/         # JSONL & report export
├── prompts/           # LLM evaluation prompts
└── llm/               # LLM client
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
