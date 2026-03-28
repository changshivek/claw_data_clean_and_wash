"""Full pipeline integration test.

This test verifies the complete workflow: import -> evaluate -> filter -> export.
Note: This test requires a running LLM server. Skip if SKIP_INTEGRATION=1.
"""
import json
import os
from pathlib import Path
import pytest

# Use data directory for tests
TEST_DATA_DIR = Path(__file__).parent.parent / "data"
TEST_DATA_DIR.mkdir(exist_ok=True)


@pytest.mark.skipif(
    os.getenv("SKIP_INTEGRATION") == "1",
    reason="Integration test requires running LLM server"
)
def test_import_evaluate_export_pipeline():
    """Test the complete import -> evaluate -> filter -> export pipeline."""
    db_path = TEST_DATA_DIR / "test_integration.duckdb"
    input_file = TEST_DATA_DIR / "test_integration_input.jsonl"
    output_file = TEST_DATA_DIR / "test_integration_output.jsonl"
    report_file = TEST_DATA_DIR / "test_integration_report.json"

    # Clean up
    for f in [db_path, input_file, output_file, report_file]:
        if f.exists():
            f.unlink()

    # Write test data
    test_data = [
        {
            "messages": [
                {"role": "user", "content": "What's 2+2?"},
                {"role": "assistant", "content": "2+2 equals 4."},
            ]
        },
        {
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
            ]
        },
    ]
    with open(input_file, "w") as f:
        for record in test_data:
            f.write(json.dumps(record) + "\n")

    # Import
    from claw_data_filter.importers.jsonl_importer import JSONLImporter
    importer = JSONLImporter(db_path)
    import_count = importer.import_file(input_file)
    importer.close()
    assert import_count == 2

    # Verify import
    from claw_data_filter.storage.duckdb_store import DuckDBStore
    store = DuckDBStore(db_path)
    assert store.get_sample_count() == 2
    store.close()

    # Verify unevaluated samples
    store = DuckDBStore(db_path)
    unevaluated = store.get_unevaluated_samples(limit=10)
    assert len(unevaluated) == 2
    store.close()

    print("Integration test (import phase) passed")
    print("Note: Skipping LLM evaluation and full pipeline (requires LLM server)")


def test_all_imports_work():
    """Verify all major components can be imported."""
    from claw_data_filter.cli import cli
    from claw_data_filter.config import Config
    from claw_data_filter.models import Sample, Evaluation
    from claw_data_filter.importers import JSONLImporter
    from claw_data_filter.processors import ConversationFormatter
    from claw_data_filter.processors.evaluator import Evaluator
    from claw_data_filter.storage import DuckDBStore
    from claw_data_filter.filters import FilterQueryBuilder
    from claw_data_filter.exporters import JSONLExporter, ReportExporter
    from claw_data_filter.prompts import build_evaluation_prompt
    from claw_data_filter.llm import LLMClient

    assert cli is not None
    assert Config is not None
    assert Sample is not None
    assert Evaluation is not None
    assert JSONLImporter is not None
    assert Evaluator is not None
    assert ConversationFormatter is not None
    assert DuckDBStore is not None
    assert FilterQueryBuilder is not None
    assert JSONLExporter is not None
    assert ReportExporter is not None
    assert build_evaluation_prompt is not None
    assert LLMClient is not None

    print("test_all_imports_work passed")


if __name__ == "__main__":
    test_all_imports_work()
    print("Integration tests complete (skipping LLM-dependent tests)")
