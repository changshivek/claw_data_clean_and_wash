"""Tests for DuckDB storage."""
import tempfile
from pathlib import Path
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.models.sample import Sample


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


def test_turn_judgments_table_created():
    """Test that turn_judgments table is created on init"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)
        # Check table exists (DuckDB uses SHOW TABLES)
        tables = store.conn.execute("SHOW TABLES").fetchall()
        table_names = [r[0] for r in tables]
        assert "turn_judgments" in table_names
        store.close()


def test_insert_and_fetch_turn_judgment():
    """Test inserting and fetching turn judgments"""
    from claw_data_filter.models.round_judgment import RoundJudgment

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)

        judgment = RoundJudgment(
            sample_id=1,
            turn_index=0,
            response_helpful="yes",
            user_satisfied="yes",
            signal_from_users=["谢谢"],
        )
        j_id = store.insert_turn_judgment(judgment)
        assert j_id > 0

        fetched = store.get_turn_judgments(1)
        assert len(fetched) == 1
        assert fetched[0].response_helpful == "yes"
        store.close()


def test_tool_stats_column_exists():
    """Test that tool_stats column exists in samples table"""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)
        # Check tool_stats column exists in samples (DuckDB uses PRAGMA)
        columns = store.conn.execute("PRAGMA table_info('samples')").fetchall()
        col_names = [c[1] for c in columns]  # PRAGMA returns: cid, name, type, notnull, dflt_value, pk
        assert "tool_stats" in col_names
        store.close()


def test_update_sample_tool_stats():
    """Test updating tool_stats for a sample"""
    from claw_data_filter.models.sample import Sample

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)

        # Insert a sample first
        sample = Sample(
            raw_json={"messages": []},
            user_query="test",
            assistant_response="test",
        )
        sample_id = store.insert_sample(sample)

        # Update tool stats
        tool_stats = {"response_helpful_rate": 0.8, "user_satisfied_rate": 0.9, "total_turns": 3, "has_error": False}
        store.update_sample_tool_stats(sample_id, tool_stats)

        # Verify
        result = store.conn.execute("SELECT tool_stats FROM samples WHERE id = ?", [sample_id]).fetchone()
        import json
        assert result is not None
        assert json.loads(result[0])["response_helpful_rate"] == 0.8
        store.close()


def test_get_unprocessed_samples():
    """Test getting samples that haven't been processed"""
    from claw_data_filter.models.sample import Sample

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)

        # Insert a sample
        sample = Sample(
            raw_json={"request": {"bodyJson": {"messages": [{"role": "user", "content": "test"}]}}},
            user_query="test",
            assistant_response="test",
        )
        sample_id = store.insert_sample(sample)

        # Should be unprocessed
        unprocessed = store.get_unprocessed_samples(limit=10)
        assert len(unprocessed) == 1
        assert unprocessed[0][0] == sample_id

        store.close()


def test_samples_has_task_type_column():
    """Test samples table has task_type column"""
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)
        result = store.conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'samples' AND column_name = 'task_type'
        """).fetchone()
        assert result is not None, "task_type column should exist"
        store.close()


def test_evaluations_table_dropped():
    """Test evaluations table no longer exists"""
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)
        result = store.conn.execute("""
            SELECT table_name FROM information_schema.tables WHERE table_name = 'evaluations'
        """).fetchone()
        assert result is None, "evaluations table should be dropped"
        store.close()


def test_get_stats_returns_new_fields():
    """Test get_stats returns response_helpful_rate, user_satisfied_rate, error_count"""
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)
        stats = store.get_stats()
        assert "avg_response_helpful_rate" in stats
        assert "avg_user_satisfied_rate" in stats
        assert "error_count" in stats
        store.close()


if __name__ == "__main__":
    test_store_and_retrieve_samples()
    test_insert_and_fetch_turn_judgment()
    test_tool_stats_column_exists()
    test_update_sample_tool_stats()
    test_get_unprocessed_samples()
    test_samples_has_task_type_column()
    test_evaluations_table_dropped()
    test_get_stats_returns_new_fields()
    print("All DuckDB store tests passed!")