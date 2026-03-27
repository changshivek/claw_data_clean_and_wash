"""Tests for DuckDB storage."""
import tempfile
from pathlib import Path
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.models.sample import Sample
from claw_data_filter.models.evaluation import Evaluation


def test_store_and_retrieve_samples():
    """Test storing and retrieving samples."""
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
        sample_id = store.insert_sample(sample)
        assert sample_id == 1

        # Retrieve
        samples = store.get_samples(limit=10)
        assert len(samples) == 1
        assert samples[0].user_query == "Hello"

        # Check count
        count = store.get_sample_count()
        assert count == 1

        store.close()
        print("test_store_and_retrieve_samples passed")


def test_insert_evaluation():
    """Test inserting evaluation."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        store = DuckDBStore(db_path)

        # Insert sample first
        raw = {"messages": [{"role": "user", "content": "Test"}]}
        sample = Sample.from_dict(raw)
        sample_id = store.insert_sample(sample)

        # Insert evaluation
        evaluation = Evaluation(
            sample_id=sample_id,
            task_type="general",
            progress_score=4,
            tool_quality_score=1.0,
            tool_success_rate=1.0,
            overall_score=8.0,
            reasoning="Good work"
        )
        eval_id = store.insert_evaluation(evaluation)
        assert eval_id == 1

        # Check counts
        assert store.get_sample_count() == 1
        assert store.get_evaluation_count() == 1

        store.close()
        print("test_insert_evaluation passed")


def test_get_unevaluated_samples():
    """Test getting unevaluated samples."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        store = DuckDBStore(db_path)

        # Insert two samples
        for i in range(2):
            raw = {"messages": [{"role": "user", "content": f"Query {i}"}]}
            sample = Sample.from_dict(raw)
            store.insert_sample(sample)

        # Only evaluate one
        evaluation = Evaluation(
            sample_id=1,
            task_type="general",
            progress_score=4,
            tool_quality_score=1.0,
            tool_success_rate=1.0,
            overall_score=8.0,
            reasoning="Good"
        )
        store.insert_evaluation(evaluation)

        # Get unevaluated
        unevaluated = store.get_unevaluated_samples(limit=10)
        assert len(unevaluated) == 1
        assert unevaluated[0][0] == 2  # sample_id 2

        store.close()
        print("test_get_unevaluated_samples passed")


def test_get_stats():
    """Test getting statistics."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
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

        stats = store.get_stats()
        assert stats["total_samples"] == 1
        assert stats["total_evaluations"] == 1
        assert stats["avg_progress_score"] == 5.0

        store.close()
        print("test_get_stats passed")


if __name__ == "__main__":
    test_store_and_retrieve_samples()
    test_insert_evaluation()
    test_get_unevaluated_samples()
    test_get_stats()
    print("All DuckDB store tests passed!")