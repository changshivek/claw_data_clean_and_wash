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


def test_insert_sample_deduplicates_by_sample_uid():
    """Test duplicate raw payloads reuse the same internal sample id."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "dedupe.duckdb"
        store = DuckDBStore(db_path)

        raw = {
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi there!"},
            ]
        }
        sample = Sample.from_dict(raw)

        first_id = store.insert_sample(sample)
        second_id = store.insert_sample(Sample.from_dict(raw))

        assert first_id == second_id
        assert store.get_sample_count() == 1
        row = store.conn.execute("SELECT sample_uid FROM samples WHERE id = ?", [first_id]).fetchone()
        assert row is not None and row[0]

        store.close()


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


def test_claim_unprocessed_samples_marks_processing():
    """Test claiming samples moves them to processing state."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "claim.db"
        store = DuckDBStore(db_path)
        sample = Sample.from_dict({
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi"},
            ]
        })
        sample_id = store.insert_sample(sample)

        claimed = store.claim_unprocessed_samples(limit=10)

        assert len(claimed) == 1
        assert claimed[0][0] == sample_id
        row = store.conn.execute("SELECT processing_status FROM samples WHERE id = ?", [sample_id]).fetchone()
        assert row[0] == "processing"
        store.close()


def test_partially_processed_sample_remains_unprocessed():
    """Test samples with missing judgments are still returned for processing."""
    from claw_data_filter.models.round_judgment import RoundJudgment

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_partial.db"
        store = DuckDBStore(db_path)

        sample = Sample.from_dict({
            "messages": [
                {"role": "user", "content": "Question"},
                {"role": "assistant", "content": "First"},
                {"role": "user", "content": "Follow up"},
                {"role": "assistant", "content": "Second"},
            ]
        })
        sample_id = store.insert_sample(sample)
        store.insert_turn_judgment(
            RoundJudgment(sample_id=sample_id, turn_index=0, response_helpful="yes", user_satisfied="yes")
        )

        unprocessed = store.get_unprocessed_samples(limit=10)
        assert len(unprocessed) == 1
        assert unprocessed[0][0] == sample_id

        store.close()


def test_replace_round_feedback_results_replaces_old_judgments():
    """Test replacing round feedback results clears stale partial judgments."""
    from claw_data_filter.models.round_judgment import RoundJudgment

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test_replace.db"
        store = DuckDBStore(db_path)

        sample = Sample.from_dict({
            "messages": [
                {"role": "user", "content": "Question"},
                {"role": "assistant", "content": "First"},
                {"role": "assistant", "content": "Second"},
            ]
        })
        sample_id = store.insert_sample(sample)
        store.insert_turn_judgment(
            RoundJudgment(sample_id=sample_id, turn_index=0, response_helpful="no", user_satisfied="no")
        )

        judgments = [
            RoundJudgment(sample_id=sample_id, turn_index=0, response_helpful="yes", user_satisfied="yes"),
            RoundJudgment(sample_id=sample_id, turn_index=1, response_helpful="yes", user_satisfied="uncertain"),
        ]
        tool_stats = {
            "response_helpful_rate": 1.0,
            "user_satisfied_rate": 0.5,
            "total_turns": 2,
            "has_error": False,
        }

        store.replace_round_feedback_results(sample_id, 2, judgments, tool_stats)

        rows = store.get_turn_judgments(sample_id)
        assert len(rows) == 2
        assert [row.turn_index for row in rows] == [0, 1]
        sample_row = store.conn.execute(
            "SELECT processing_status FROM samples WHERE id = ?",
            [sample_id],
        ).fetchone()
        assert sample_row[0] == "completed"

        store.close()


def test_mark_sample_processing_failed_sets_failed_status():
    """Test failed status is persisted for later retry."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "failed.db"
        store = DuckDBStore(db_path)
        sample_id = store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi"},
            ]
        }))

        store.mark_sample_processing_failed(sample_id, "boom")

        row = store.conn.execute(
            "SELECT processing_status, tool_stats FROM samples WHERE id = ?",
            [sample_id],
        ).fetchone()
        assert row[0] == "failed"
        assert "boom" in row[1]
        store.close()


def test_filter_samples_returns_sample_dicts():
    """Test filter_samples returns parsed records and count."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "filter.db"
        store = DuckDBStore(db_path)
        sample_id = store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi"},
            ]
        }))
        store.update_sample_tool_stats(
            sample_id,
            {"response_helpful_rate": 0.9, "user_satisfied_rate": 0.8, "total_turns": 1, "has_error": False},
        )

        samples, total = store.filter_samples(helpful_rate_val=0.8, limit=10, offset=0)

        assert total == 1
        assert len(samples) == 1
        assert samples[0]["id"] == sample_id
        assert samples[0]["helpful_rate"] == 0.9
        store.close()


def test_samples_schema_removed_unused_columns_and_added_uid():
    """Test samples schema removes dead columns and keeps stable import uid."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)
        columns = store.conn.execute("PRAGMA table_info('samples')").fetchall()
        column_names = {column[1] for column in columns}
        assert "sample_uid" in column_names
        assert "task_type" not in column_names
        assert "has_error" not in column_names
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


def test_init_schema_backfills_num_turns_from_expected_judgment_count():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "backfill.db"
        conn = __import__("duckdb").connect(str(db_path))
        conn.execute(
            """
            CREATE TABLE samples (
                id INTEGER PRIMARY KEY,
                sample_uid TEXT,
                raw_json JSON,
                user_query TEXT,
                assistant_response TEXT,
                num_turns INTEGER,
                expected_judgment_count INTEGER,
                num_tool_calls INTEGER,
                imported_at TIMESTAMP,
                tool_stats JSON,
                processing_status TEXT,
                processing_updated_at TIMESTAMP
            )
            """
        )
        conn.execute(
            "INSERT INTO samples (id, sample_uid, raw_json, user_query, assistant_response, num_turns, expected_judgment_count) VALUES (1, 'u', '{\"messages\":[]}', '', '', 15, 1)"
        )
        conn.execute(
            """
            CREATE TABLE turn_judgments (
                id INTEGER PRIMARY KEY,
                sample_id INTEGER,
                turn_index INTEGER,
                response_helpful TEXT,
                user_satisfied TEXT,
                signal_from_users JSON,
                llm_error BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )
        conn.close()

        store = DuckDBStore(db_path)
        row = store.conn.execute("SELECT num_turns, expected_judgment_count FROM samples WHERE id = 1").fetchone()
        assert row == (1, 1)
        store.close()


if __name__ == "__main__":
    test_store_and_retrieve_samples()
    test_insert_sample_deduplicates_by_sample_uid()
    test_insert_and_fetch_turn_judgment()
    test_tool_stats_column_exists()
    test_update_sample_tool_stats()
    test_get_unprocessed_samples()
    test_claim_unprocessed_samples_marks_processing()
    test_partially_processed_sample_remains_unprocessed()
    test_replace_round_feedback_results_replaces_old_judgments()
    test_mark_sample_processing_failed_sets_failed_status()
    test_filter_samples_returns_sample_dicts()
    test_samples_schema_removed_unused_columns_and_added_uid()
    test_evaluations_table_dropped()
    test_get_stats_returns_new_fields()
    print("All DuckDB store tests passed!")