"""Tests for exporters."""
import json
from pathlib import Path
from claw_data_filter.exporters.jsonl_exporter import JSONLExporter
from claw_data_filter.exporters.report_exporter import ReportExporter
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.models.sample import Sample
from claw_data_filter.models.evaluation import Evaluation

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

    # Insert sample and evaluation
    raw = {"messages": [{"role": "user", "content": "Test"}]}
    sample = Sample.from_dict(raw)
    sample_id = store.insert_sample(sample)

    evaluation = Evaluation(
        sample_id=sample_id,
        task_type="coding",
        progress_score=4,
        tool_quality_score=1.0,
        tool_success_rate=1.0,
        overall_score=8.0,
        reasoning="Good"
    )
    store.insert_evaluation(evaluation)

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

    # Insert 3 samples with different scores
    for i, score in enumerate([2, 4, 5]):
        raw = {"messages": [{"role": "user", "content": f"Test {i}"}]}
        sample = Sample.from_dict(raw)
        sample_id = store.insert_sample(sample)
        evaluation = Evaluation(
            sample_id=sample_id,
            task_type="coding",
            progress_score=score,
            tool_quality_score=1.0,
            tool_success_rate=1.0,
            overall_score=8.0,
            reasoning="Good"
        )
        store.insert_evaluation(evaluation)

    # Export with filter
    exporter = JSONLExporter(store)
    count = exporter.export(output_path, filter_query="progress_score >= 4")

    assert count == 2  # Only scores 4 and 5

    store.close()
    print("test_jsonl_export_with_filter passed")


def test_report_generation():
    """Test generating statistical report."""
    db_path = TEST_DATA_DIR / "test_report_gen.duckdb"

    if db_path.exists():
        db_path.unlink()

    store = DuckDBStore(db_path)

    # Insert sample and evaluation
    raw = {"messages": [{"role": "user", "content": "Test"}]}
    sample = Sample.from_dict(raw)
    sample_id = store.insert_sample(sample)

    evaluation = Evaluation(
        sample_id=sample_id,
        task_type="coding",
        progress_score=5,
        tool_quality_score=0.9,
        tool_success_rate=1.0,
        overall_score=9.5,
        reasoning="Excellent"
    )
    store.insert_evaluation(evaluation)

    # Generate report
    exporter = ReportExporter(store)
    report = exporter.generate_report()

    assert "summary" in report
    assert report["summary"]["total_samples"] == 1
    assert report["summary"]["total_evaluations"] == 1
    assert report["averages"]["progress_score"] == 5.0
    assert report["averages"]["tool_quality"] == 0.9

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

    # Insert sample and evaluation
    raw = {"messages": [{"role": "user", "content": "Test"}]}
    sample = Sample.from_dict(raw)
    sample_id = store.insert_sample(sample)

    evaluation = Evaluation(
        sample_id=sample_id,
        task_type="general",
        progress_score=4,
        tool_quality_score=1.0,
        tool_success_rate=1.0,
        overall_score=8.0,
        reasoning="Good"
    )
    store.insert_evaluation(evaluation)

    # Export report
    exporter = ReportExporter(store)
    exporter.export_report(report_path)

    assert report_path.exists()
    with open(report_path) as f:
        report = json.load(f)
        assert "summary" in report
        assert "averages" in report

    store.close()
    print("test_report_export passed")


if __name__ == "__main__":
    test_jsonl_export()
    test_jsonl_export_with_filter()
    test_report_generation()
    test_report_export()
    print("All exporter tests passed!")
