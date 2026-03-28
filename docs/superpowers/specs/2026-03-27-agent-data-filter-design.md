# Agent Data Filter - Design Specification

## Overview

A CLI tool for filtering and evaluating agent interaction data using local LLM models. The tool processes OpenAI-format conversation data, evaluates interaction quality across multiple dimensions, and provides filtered export capabilities.

## Technology Stack

- **Interface**: CLI (Click-based)
- **LLM**: Local models via vLLM/Ollama
- **Database**: DuckDB (embedded, parallel-friendly)
- **Export**: JSONL + JSON statistical report

## Data Format

Input: JSONL with OpenAI chat completion format
```json
{"messages": [{"role": "system", "content": "..."}, {"role": "user", "content": "..."}, {"role": "assistant", "content": "...", "tool_calls": [...]}]}
```

## Database Schema

### Table: `samples`
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| raw_json | JSON | Original complete record |
| user_query | TEXT | Extracted user query |
| assistant_response | TEXT | Formatted assistant response |
| num_turns | INTEGER | Number of interaction turns |
| num_tool_calls | INTEGER | Total tool calls count |
| has_error | BOOLEAN | Whether any tool call errored |
| imported_at | TIMESTAMP | Import timestamp |

### Table: `evaluations`
| Column | Type | Description |
|--------|------|-------------|
| id | INTEGER | Primary key |
| sample_id | INTEGER | Foreign key to samples |
| task_type | TEXT | Classified task type |
| progress_score | INTEGER | 0-5 scale |
| tool_quality_score | FLOAT | 0-1 scale |
| tool_success_rate | FLOAT | 0.0-1.0 scale |
| overall_score | FLOAT | 0.0-10.0 scale |
| reasoning | TEXT | Evaluation reasoning |
| evaluated_at | TIMESTAMP | Evaluation timestamp |

## Pipeline Stages

### Stage 1: Import
1. Read JSONL file line by line
2. Parse messages, extract user query and assistant response
3. Remove system prompt from output
4. Extract metadata: turn count, tool call count, error presence
5. Insert into DuckDB `samples` table

### Stage 2: Evaluation
1. Batch read samples from DuckDB (filter: unevaluated)
2. Format each sample into evaluation prompt
3. Dispatch to local LLM via API
4. Parse structured JSON response
5. Write results to `evaluations` table
6. Support parallel workers and resume on interruption

### Stage 3: Filter & Export
1. Query samples with JOIN to evaluations
2. Apply filter conditions (score thresholds, task type, etc.)
3. Export filtered samples as JSONL
4. Generate statistical summary report

## Evaluation Dimensions

| Dimension | Score | Description |
|-----------|-------|-------------|
| Progress | 0-5 | Task progress level (see below) |
| Tool Quality | 0-1 | Tool parameter understanding quality |
| Tool Success | 0.0-1.0 | Tool call success rate |
| Overall | 0.0-10.0 | Composite score |

### Progress Score Scale
- **0**: Wrong direction or endless loop
- **1**: Reasonable attempt, no significant progress
- **2**: Correct direction, correct tool calls, significant progress
- **3**: Reserved
- **4**: Successfully completed with trial-and-error
- **5**: Successfully completed, no trial-and-error

## CLI Commands

```bash
# Import data
claw-filter import <input.jsonl>

# Evaluate (parallel)
claw-filter evaluate --workers <n> --batch-size <n>

# Filter and export
claw-filter filter --progress-score ">=4" --export <output.jsonl> --report <report.json>

# Show statistics
claw-filter stats

# Show database info
claw-filter info
```

## Project Structure
```
claw-data-filter/
├── cli.py
├── config.py
├── models/
│   ├── __init__.py
│   ├── sample.py
│   └── evaluation.py
├── importers/
│   ├── __init__.py
│   └── jsonl_importer.py
├── processors/
│   ├── __init__.py
│   ├── formatter.py
│   └── evaluator.py
├── storage/
│   ├── __init__.py
│   └── duckdb_store.py
├── filters/
│   ├── __init__.py
│   └── query.py
├── exporters/
│   ├── __init__.py
│   ├── jsonl_exporter.py
│   └── report_exporter.py
├── prompts/
│   ├── __init__.py
│   └── evaluation_prompt.py
├── llm/
│   ├── __init__.py
│   └── client.py
└── main.py
```

## Error Handling
- Malformed JSONL lines: log error, skip line, continue
- LLM API errors: retry with exponential backoff, mark as failed after N attempts
- Database errors: transaction rollback, clear error message
- Evaluation parse errors: log sample ID and raw response, mark as failed

## Configuration
- LLM endpoint URL (default: http://localhost:8000/v1)
- Database path (default: ./data.duckdb)
- Worker count (default: CPU cores / 2)
- Batch size (default: 10)
- API key (optional, for authenticated endpoints)
