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


def test_store_reopens_with_stale_sample_sequence_and_recovers():
    """Test reopening a DB repairs sample_id_seq when it lags max(id)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "stale_sequence.duckdb"
        store = DuckDBStore(db_path)
        first_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "Hello"},
                        {"role": "assistant", "content": "Hi there!"},
                    ]
                }
            )
        )
        assert first_id == 1
        store.close()

        reopened = DuckDBStore(db_path)
        reopened.conn.execute("DROP SEQUENCE sample_id_seq")
        reopened.conn.execute("CREATE SEQUENCE sample_id_seq START 1")
        reopened.close()

        repaired = DuckDBStore(db_path)
        second_id = repaired.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "Another"},
                        {"role": "assistant", "content": "Reply"},
                    ]
                }
            )
        )

        assert second_id == 2
        assert repaired.get_sample_count() == 2
        repaired.close()


def test_dual_judgment_tables_created():
    """Test that dual judgment tables are created on init."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)
        tables = store.conn.execute("SHOW TABLES").fetchall()
        table_names = [r[0] for r in tables]
        assert "assistant_response_judgments" in table_names
        assert "user_episode_judgments" in table_names
        assert "turn_judgments" not in table_names
        store.close()


def test_insert_and_fetch_dual_judgments():
    """Test inserting and fetching dual-level judgments."""
    from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind, UserEpisodeJudgment
    from claw_data_filter.models.sample import Sample

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)
        sample_id = store.insert_sample(
            Sample.from_dict(
                {
                    "messages": [
                        {"role": "user", "content": "hello"},
                        {"role": "assistant", "content": "hi"},
                    ]
                }
            )
        )
        sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]

        response_judgment = AssistantResponseJudgment(
            sample_uid=sample_uid,
            response_index=0,
            episode_index=0,
            assistant_message_index=1,
            feedback_kind=FeedbackKind.USER,
            feedback_message_start_index=2,
            feedback_message_end_index=2,
            feedback_payload=["谢谢"],
            response_progress="yes",
        )
        episode_judgment = UserEpisodeJudgment(
            sample_uid=sample_uid,
            episode_index=0,
            start_user_message_index=0,
            end_before_user_message_index=1,
            signal_from_users=["谢谢"],
            user_satisfied="yes",
        )
        response_uid = store.insert_assistant_response_judgment(response_judgment)
        episode_uid = store.insert_user_episode_judgment(episode_judgment)
        assert response_uid == response_judgment.judgment_uid
        assert episode_uid == episode_judgment.judgment_uid

        fetched_response = store.get_assistant_response_judgments(sample_uid)
        fetched_episode = store.get_user_episode_judgments(sample_uid)
        assert len(fetched_response) == 1
        assert len(fetched_episode) == 1
        assert fetched_response[0].response_progress == "yes"
        assert fetched_episode[0].user_satisfied == "yes"
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
        sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]

        # Update tool stats
        tool_stats = {"response_progress_rate": 0.8, "user_satisfied_rate": 0.9, "total_turns": 3, "has_error": False}
        store.update_sample_tool_stats(sample_uid, tool_stats)

        # Verify
        result = store.conn.execute("SELECT tool_stats FROM samples WHERE id = ?", [sample_id]).fetchone()
        import json
        assert result is not None
        assert json.loads(result[0])["response_progress_rate"] == 0.8
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
        store.conn.execute(
            "UPDATE samples SET session_merge_status = 'keep', session_merge_keep = TRUE WHERE id = ?",
            [sample_id],
        )

        # Should be unprocessed
        unprocessed = store.get_unprocessed_samples(limit=10)
        assert len(unprocessed) == 1
        assert unprocessed[0][0] == store.get_sample_by_id(sample_id)["sample_uid"]

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
        store.conn.execute(
            "UPDATE samples SET session_merge_status = 'keep', session_merge_keep = TRUE WHERE id = ?",
            [sample_id],
        )

        claimed = store.claim_unprocessed_samples(limit=10)

        assert len(claimed) == 1
        assert claimed[0][0] == store.get_sample_by_id(sample_id)["sample_uid"]
        row = store.conn.execute("SELECT processing_status FROM samples WHERE id = ?", [sample_id]).fetchone()
        assert row[0] == "processing"
        store.close()


def test_claim_unprocessed_samples_skips_unmarked_rows():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "claim_unmarked.db"
        store = DuckDBStore(db_path)
        sample_id = store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi"},
            ]
        }))

        claimed = store.claim_unprocessed_samples(limit=10)

        assert claimed == []
        row = store.conn.execute(
            "SELECT processing_status, session_merge_status, session_merge_keep FROM samples WHERE id = ?",
            [sample_id],
        ).fetchone()
        assert row == ("pending", None, None)
        store.close()


def test_claim_unprocessed_samples_skips_session_merged_rows():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "claim_session_merge.db"
        store = DuckDBStore(db_path)
        sample_id = store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi"},
            ]
        }))
        sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]
        store.conn.execute(
            "UPDATE samples SET session_merge_keep = false, session_merge_status = 'merged', session_merge_representative_uid = ? WHERE id = ?",
            [sample_uid, sample_id],
        )

        claimed = store.claim_unprocessed_samples(limit=10)

        assert claimed == []
        row = store.conn.execute("SELECT processing_status FROM samples WHERE id = ?", [sample_id]).fetchone()
        assert row[0] == "pending"
        store.close()


def test_partially_processed_sample_remains_unprocessed():
    """Test samples with missing judgments are still returned for processing."""
    from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind

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
        store.conn.execute(
            "UPDATE samples SET session_merge_status = 'keep', session_merge_keep = TRUE WHERE id = ?",
            [sample_id],
        )
        sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]
        store.insert_assistant_response_judgment(
            AssistantResponseJudgment(
                sample_uid=sample_uid,
                response_index=0,
                episode_index=0,
                assistant_message_index=1,
                feedback_kind=FeedbackKind.NONE,
                response_progress="yes",
            )
        )

        unprocessed = store.get_unprocessed_samples(limit=10)
        assert len(unprocessed) == 1
        assert unprocessed[0][0] == store.get_sample_by_id(sample_id)["sample_uid"]

        store.close()


def test_replace_round_feedback_results_replaces_stale_dual_judgments():
    """Test replacing round feedback results clears stale dual judgments."""
    from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind, UserEpisodeJudgment

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
        sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]
        store.insert_assistant_response_judgment(
            AssistantResponseJudgment(
                sample_uid=sample_uid,
                response_index=0,
                episode_index=0,
                assistant_message_index=1,
                feedback_kind=FeedbackKind.NONE,
                response_progress="no",
            )
        )
        store.insert_user_episode_judgment(
            UserEpisodeJudgment(
                sample_uid=sample_uid,
                episode_index=0,
                start_user_message_index=0,
                end_before_user_message_index=2,
                signal_from_users=[],
                user_satisfied="no",
            )
        )

        response_judgments = [
            AssistantResponseJudgment(
                sample_uid=sample_uid,
                response_index=0,
                episode_index=0,
                assistant_message_index=1,
                feedback_kind=FeedbackKind.NONE,
                response_progress="yes",
            ),
            AssistantResponseJudgment(
                sample_uid=sample_uid,
                response_index=1,
                episode_index=0,
                assistant_message_index=2,
                feedback_kind=FeedbackKind.NONE,
                response_progress="yes",
            ),
        ]
        episode_judgments = [
            UserEpisodeJudgment(
                sample_uid=sample_uid,
                episode_index=0,
                start_user_message_index=0,
                end_before_user_message_index=2,
                signal_from_users=[],
                user_satisfied="uncertain",
            )
        ]
        tool_stats = {
            "response_progress_rate": 1.0,
            "user_satisfied_rate": 0.5,
            "response_regress_rate": 0.0,
            "user_negative_feedback_rate": 0.0,
            "assistant_response_count": 2,
            "user_episode_count": 1,
            "has_error": False,
        }

        store.replace_round_feedback_results(sample_uid, 2, 1, response_judgments, episode_judgments, tool_stats)

        stored_response = store.get_assistant_response_judgments(sample_uid)
        stored_episode = store.get_user_episode_judgments(sample_uid)
        assert len(stored_response) == 2
        assert len(stored_episode) == 1
        assert [row.response_index for row in stored_response] == [0, 1]
        assert stored_episode[0].user_satisfied == "uncertain"
        sample_row = store.conn.execute(
            "SELECT processing_status, expected_response_judgment_count, expected_episode_judgment_count FROM samples WHERE id = ?",
            [sample_id],
        ).fetchone()
        assert sample_row == ("completed", 2, 1)

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
        sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]

        store.mark_sample_processing_failed(sample_uid, "boom")

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
        sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]
        store.update_sample_tool_stats(
            sample_uid,
            {"response_progress_rate": 0.9, "user_satisfied_rate": 0.8, "total_turns": 1, "has_error": False},
        )

        samples, total = store.filter_samples(progress_rate_val=0.8, limit=10, offset=0)

        assert total == 1
        assert len(samples) == 1
        assert samples[0]["id"] == sample_id
        assert samples[0]["progress_rate"] == 0.9
        store.close()


    def test_insert_sample_persists_empty_response_marker():
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "empty_response_insert.duckdb"
            store = DuckDBStore(db_path)

            sample_id = store.insert_sample(
                Sample.from_dict(
                    {
                        "request": {
                            "bodyJson": {
                                "messages": [
                                    {"role": "user", "content": "只有用户"},
                                ]
                            }
                        }
                    }
                )
            )

            row = store.conn.execute("SELECT empty_response FROM samples WHERE id = ?", [sample_id]).fetchone()
            assert row == (True,)
            store.close()


def test_filter_samples_supports_session_merge_scope_and_status():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "filter_session_merge.db"
        store = DuckDBStore(db_path)
        first_id = store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi"},
            ]
        }))
        second_id = store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "Hello"},
                {"role": "assistant", "content": "Hi again"},
            ]
        }))
        store.conn.execute(
            "UPDATE samples SET session_merge_status = 'keep', session_merge_keep = TRUE WHERE id = ?",
            [first_id],
        )
        store.conn.execute(
            "UPDATE samples SET session_merge_status = 'merged', session_merge_keep = FALSE, session_merge_reason = 'exact_duplicate_sequence' WHERE id = ?",
            [second_id],
        )

        keep_rows, keep_total = store.filter_samples(session_merge_keep=True, limit=10, offset=0)
        merged_rows, merged_total = store.filter_samples(session_merge_keep=False, limit=10, offset=0)
        merged_status_rows, merged_status_total = store.filter_samples(session_merge_status="merged", limit=10, offset=0)

        assert keep_total == 1
        assert keep_rows[0]["id"] == first_id
        assert merged_total == 1
        assert merged_rows[0]["id"] == second_id
        assert merged_status_total == 1
        assert merged_status_rows[0]["session_merge_reason"] == "exact_duplicate_sequence"
        store.close()


    def test_filter_samples_supports_empty_response_flag():
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "filter_empty_response.db"
            store = DuckDBStore(db_path)
            empty_id = store.insert_sample(Sample.from_dict({
                "messages": [
                    {"role": "user", "content": "hello"},
                ]
            }))
            normal_id = store.insert_sample(Sample.from_dict({
                "messages": [
                    {"role": "user", "content": "hello"},
                    {"role": "assistant", "content": "hi"},
                ]
            }))

            empty_rows, empty_total = store.filter_samples(empty_response=True, limit=10, offset=0)
            normal_rows, normal_total = store.filter_samples(empty_response=False, limit=10, offset=0)

            assert empty_total == 1
            assert empty_rows[0]["id"] == empty_id
            assert normal_total == 1
            assert normal_rows[0]["id"] == normal_id
            store.close()


def test_get_session_merge_counts_returns_summary():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "merge_counts.db"
        store = DuckDBStore(db_path)
        keep_id = store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "keep"},
                {"role": "assistant", "content": "ok"},
            ]
        }))
        merged_id = store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "merged"},
                {"role": "assistant", "content": "ok"},
            ]
        }))
        skipped_id = store.insert_sample(Sample.from_dict({
            "messages": [
                {"role": "user", "content": "skipped"},
                {"role": "assistant", "content": "ok"},
            ]
        }))
        store.conn.execute("UPDATE samples SET session_merge_status = 'keep', session_merge_keep = TRUE WHERE id = ?", [keep_id])
        store.conn.execute("UPDATE samples SET session_merge_status = 'merged', session_merge_keep = FALSE WHERE id = ?", [merged_id])
        store.conn.execute("UPDATE samples SET session_merge_status = 'skipped', session_merge_keep = TRUE WHERE id = ?", [skipped_id])

        counts = store.get_session_merge_counts()

        assert counts == {"total": 3, "keep": 2, "merged": 1, "skipped": 1, "unmarked": 0, "empty_response": 0}
        store.close()


def test_samples_schema_removed_unused_columns_and_added_uid():
    """Test samples schema removes dead columns and keeps stable import uid."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)
        columns = store.conn.execute("PRAGMA table_info('samples')").fetchall()
        column_names = {column[1] for column in columns}
        assert "sample_uid" in column_names
        assert "empty_response" in column_names
        assert "response_progress_rate" in column_names
        assert "response_regress_rate" in column_names
        assert "user_satisfied_rate" in column_names
        assert "user_negative_feedback_rate" in column_names
        assert "session_merge_status" in column_names
        assert "session_merge_keep" in column_names
        assert "session_merge_group_id" in column_names
        assert "session_merge_group_size" in column_names
        assert "session_merge_representative_uid" in column_names
        assert "session_merge_reason" in column_names
        assert "session_merge_updated_at" in column_names
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
    """Test get_stats returns response_progress_rate, user_satisfied_rate, error_count"""
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        store = DuckDBStore(db_path)
        stats = store.get_stats()
        assert "avg_response_progress_rate" in stats
        assert "avg_response_regress_rate" in stats
        assert "avg_user_satisfied_rate" in stats
        assert "avg_user_negative_feedback_rate" in stats
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
            CREATE TABLE assistant_response_judgments (
                judgment_uid TEXT PRIMARY KEY,
                sample_uid TEXT,
                response_index INTEGER,
                episode_index INTEGER,
                assistant_message_index INTEGER,
                feedback_kind TEXT,
                feedback_message_start_index INTEGER,
                feedback_message_end_index INTEGER,
                feedback_payload JSON,
                response_progress TEXT,
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


def test_init_schema_recomputes_tool_stats_from_dual_judgments():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "tool_stats_backfill.db"
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
            """
            CREATE TABLE assistant_response_judgments (
                judgment_uid TEXT PRIMARY KEY,
                sample_uid TEXT,
                response_index INTEGER,
                episode_index INTEGER,
                assistant_message_index INTEGER,
                feedback_kind TEXT,
                feedback_message_start_index INTEGER,
                feedback_message_end_index INTEGER,
                feedback_payload JSON,
                response_progress TEXT,
                llm_error BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE user_episode_judgments (
                judgment_uid TEXT PRIMARY KEY,
                sample_uid TEXT,
                episode_index INTEGER,
                start_user_message_index INTEGER,
                end_before_user_message_index INTEGER,
                signal_from_users JSON,
                user_satisfied TEXT,
                llm_error BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )
        conn.execute(
            "INSERT INTO samples (id, sample_uid, raw_json, user_query, assistant_response, num_turns, expected_judgment_count, tool_stats) VALUES (1, 'u', '{\"messages\":[]}', '', '', 3, 3, '{\"response_progress_rate\": 0.33, \"user_satisfied_rate\": 0.25, \"total_turns\": 4, \"has_error\": false}')"
        )
        conn.execute(
            "INSERT INTO assistant_response_judgments VALUES ('resp:u:0', 'u', 0, 0, 1, 'none', NULL, NULL, '[]', 'yes', false, CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "INSERT INTO assistant_response_judgments VALUES ('resp:u:1', 'u', 1, 0, 2, 'none', NULL, NULL, '[]', 'no', false, CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "INSERT INTO assistant_response_judgments VALUES ('resp:u:2', 'u', 2, 1, 4, 'none', NULL, NULL, '[]', 'yes', false, CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "INSERT INTO user_episode_judgments VALUES ('episode:u:0', 'u', 0, 0, 2, '[]', 'neutral', false, CURRENT_TIMESTAMP)"
        )
        conn.execute(
            "INSERT INTO user_episode_judgments VALUES ('episode:u:1', 'u', 1, 3, 4, '[]', 'no', false, CURRENT_TIMESTAMP)"
        )
        conn.close()

        store = DuckDBStore(db_path)
        row = store.conn.execute("SELECT tool_stats FROM samples WHERE id = 1").fetchone()
        stats = __import__("json").loads(row[0])
        assert stats["response_progress_rate"] == 2 / 3
        assert stats["response_regress_rate"] == 1 / 3
        assert stats["user_satisfied_rate"] == 0.0
        assert stats["user_negative_feedback_rate"] == 0.5
        assert stats["response_progress_scored_steps"] == 3
        assert stats["user_feedback_scored_episodes"] == 2
        rate_row = store.conn.execute(
            "SELECT response_progress_rate, response_regress_rate, user_satisfied_rate, user_negative_feedback_rate FROM samples WHERE id = 1"
        ).fetchone()
        assert rate_row == (2 / 3, 1 / 3, 0.0, 0.5)
        store.close()


if __name__ == "__main__":
    test_store_and_retrieve_samples()
    test_insert_sample_deduplicates_by_sample_uid()
    test_insert_and_fetch_dual_judgments()
    test_tool_stats_column_exists()
    test_update_sample_tool_stats()
    test_get_unprocessed_samples()
    test_claim_unprocessed_samples_marks_processing()
    test_partially_processed_sample_remains_unprocessed()
    test_replace_round_feedback_results_replaces_stale_dual_judgments()
    test_mark_sample_processing_failed_sets_failed_status()
    test_filter_samples_returns_sample_dicts()
    test_samples_schema_removed_unused_columns_and_added_uid()
    test_evaluations_table_dropped()
    test_get_stats_returns_new_fields()
    print("All DuckDB store tests passed!")