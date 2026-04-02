"""Tests for exporters."""
import json
from pathlib import Path
from claw_data_filter.exporters.jsonl_exporter import JSONLExporter
from claw_data_filter.exporters.report_exporter import ReportExporter
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.models.sample import Sample

# Use data directory for tests
TEST_DATA_DIR = Path(__file__).parent.parent / "data"
TEST_DATA_DIR.mkdir(exist_ok=True)


def test_jsonl_export():
    """Test exporting samples to JSONL."""
    db_path = TEST_DATA_DIR / "test_export.duckdb"
    output_path = TEST_DATA_DIR / "test_output.jsonl"

    # Clean up
    if db_path.exists():
        db_path.unlink()
    if output_path.exists():
        output_path.unlink()

    store = DuckDBStore(db_path)

    # Insert sample
    raw = {"messages": [{"role": "user", "content": "Test"}]}
    sample = Sample.from_dict(raw)
    store.insert_sample(sample)

    # Export
    exporter = JSONLExporter(store)
    count = exporter.export(output_path)

    assert count == 1
    with open(output_path) as f:
        lines = f.readlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert "messages" in data

    store.close()
    print("test_jsonl_export passed")


def test_jsonl_export_with_filter():
    """Test exporting with filter query."""
    db_path = TEST_DATA_DIR / "test_export_filter.duckdb"
    output_path = TEST_DATA_DIR / "test_output_filter.jsonl"

    if db_path.exists():
        db_path.unlink()
    if output_path.exists():
        output_path.unlink()

    store = DuckDBStore(db_path)

    # Insert 3 samples with tool_stats
    for i in range(3):
        raw = {"messages": [{"role": "user", "content": f"Test {i}"}]}
        sample = Sample.from_dict(raw)
        sample_id = store.insert_sample(sample)
        # Different response_helpful_rate for each
        tool_stats = {"response_helpful_rate": 0.5 + i * 0.2, "user_satisfied_rate": 0.8, "total_turns": 1, "has_error": False}
        store.update_sample_tool_stats(sample_id, tool_stats)

    # Export with filter
    exporter = JSONLExporter(store)
    count = exporter.export(output_path, filter_query="json_extract(samples.tool_stats, '$.response_helpful_rate') >= 0.7")

    assert count == 2  # rates 0.9 and 0.7

    store.close()
    print("test_jsonl_export_with_filter passed")


def test_report_generation():
    """Test generating statistical report."""
    db_path = TEST_DATA_DIR / "test_report_gen.duckdb"

    if db_path.exists():
        db_path.unlink()

    store = DuckDBStore(db_path)

    # Insert sample with tool_stats
    raw = {"messages": [{"role": "user", "content": "Test"}]}
    sample = Sample.from_dict(raw)
    sample_id = store.insert_sample(sample)
    tool_stats = {"response_helpful_rate": 0.9, "user_satisfied_rate": 0.85, "total_turns": 2, "has_error": False}
    store.update_sample_tool_stats(sample_id, tool_stats)

    # Generate report
    exporter = ReportExporter(store)
    report = exporter.generate_report()

    assert "summary" in report
    assert report["summary"]["total_samples"] == 1

    store.close()
    print("test_report_generation passed")


def test_report_export():
    """Test exporting report to file."""
    db_path = TEST_DATA_DIR / "test_report_export.duckdb"
    report_path = TEST_DATA_DIR / "test_report.json"

    if db_path.exists():
        db_path.unlink()
    if report_path.exists():
        report_path.unlink()

    store = DuckDBStore(db_path)

    # Insert sample
    raw = {"messages": [{"role": "user", "content": "Test"}]}
    sample = Sample.from_dict(raw)
    store.insert_sample(sample)

    # Export report
    exporter = ReportExporter(store)
    exporter.export_report(report_path)

    assert report_path.exists()
    with open(report_path) as f:
        report = json.load(f)
        assert "total_samples" in report
        assert "avg_response_helpful_rate" in report

    store.close()
    print("test_report_export passed")


def test_jsonl_exporter_no_filter():
    """Test exporter works without evaluations table"""
    db_path = TEST_DATA_DIR / "test_no_eval.duckdb"
    output_path = TEST_DATA_DIR / "test_no_eval.jsonl"

    if db_path.exists():
        db_path.unlink()
    if output_path.exists():
        output_path.unlink()

    store = DuckDBStore(db_path)

    # Insert ONLY samples - no evaluations
    for i in range(3):
        raw = {"messages": [{"role": "user", "content": f"Test {i}"}]}
        sample = Sample.from_dict(raw)
        store.insert_sample(sample)

    # Export without filter should work
    exporter = JSONLExporter(store)
    count = exporter.export(output_path)

    assert count == 3
    with open(output_path) as f:
        lines = f.readlines()
        assert len(lines) == 3

    # Export with id-based filter should also work
    filter_path = TEST_DATA_DIR / "test_no_eval_filtered.jsonl"
    count_filtered = exporter.export(filter_path, filter_query="id > 0")
    assert count_filtered == 3

    store.close()
    print("test_jsonl_exporter_no_filter passed")


if __name__ == "__main__":
    test_jsonl_export()
    test_jsonl_export_with_filter()
    test_report_generation()
    test_report_export()
    test_jsonl_exporter_no_filter()
    print("All exporter tests passed!")
