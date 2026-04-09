"""Tests for exporters."""
import json
from pathlib import Path
from claw_data_filter.exporters.report_exporter import ReportExporter
from claw_data_filter.exporters.unified_exporter import (
    OPENAI_ROUND_FEEDBACK,
    RAW_JSONL,
    ExportFilterSpec,
    ExportRequest,
    UnifiedExporter,
)
from claw_data_filter.storage.duckdb_store import DuckDBStore
from claw_data_filter.models.sample import Sample
from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind, UserEpisodeJudgment

# Use data directory for tests
TEST_DATA_DIR = Path(__file__).parent.parent / "data"
TEST_DATA_DIR.mkdir(exist_ok=True)


def test_raw_jsonl_export():
    """Test exporting raw samples to JSONL."""
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
    exporter = UnifiedExporter(store)
    count = exporter.export(ExportRequest(output_path=output_path, export_format=RAW_JSONL))

    assert count == 1
    with open(output_path) as f:
        lines = f.readlines()
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert "messages" in data

    store.close()
    print("test_raw_jsonl_export passed")


def test_raw_jsonl_export_with_filter():
    """Test exporting with structured filter spec."""
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
    exporter = UnifiedExporter(store)
    count = exporter.export(
        ExportRequest(
            output_path=output_path,
            export_format=RAW_JSONL,
            filter_spec=ExportFilterSpec(helpful_op=">=", helpful_val=0.7),
        )
    )

    assert count == 2  # rates 0.9 and 0.7

    store.close()
    print("test_raw_jsonl_export_with_filter passed")


def test_openai_round_feedback_export_includes_turn_ranges_and_judgments():
    """Test exporting OpenAI-compatible payloads with sidecar round feedback."""
    db_path = TEST_DATA_DIR / "test_export_feedback.duckdb"
    output_path = TEST_DATA_DIR / "test_output_feedback.jsonl"

    if db_path.exists():
        db_path.unlink()
    if output_path.exists():
        output_path.unlink()

    store = DuckDBStore(db_path)
    sample_id = store.insert_sample(
        Sample.from_dict(
            {
                "messages": [
                    {"role": "user", "content": "hello"},
                    {"role": "assistant", "content": "hi"},
                    {"role": "user", "content": "next"},
                    {"role": "assistant", "content": "done"},
                ],
                "metadata": {"source": "unit-test"},
            }
        )
    )
    sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]
    from claw_data_filter.models.round_judgment import AssistantResponseJudgment, FeedbackKind, UserEpisodeJudgment

    store.insert_assistant_response_judgment(
        AssistantResponseJudgment(
            sample_uid=sample_uid,
            response_index=0,
            episode_index=0,
            assistant_message_index=1,
            feedback_kind=FeedbackKind.USER,
            feedback_message_start_index=2,
            feedback_message_end_index=2,
            feedback_payload=["next"],
            response_helpful="yes",
            llm_error=False,
        )
    )
    store.insert_user_episode_judgment(
        UserEpisodeJudgment(
            sample_uid=sample_uid,
            episode_index=0,
            start_user_message_index=0,
            end_before_user_message_index=1,
            signal_from_users=["next"],
            user_satisfied="neutral",
            llm_error=False,
        )
    )
    exporter = UnifiedExporter(store)
    count = exporter.export(ExportRequest(output_path=output_path, export_format=OPENAI_ROUND_FEEDBACK))

    assert count == 1
    payload = json.loads(output_path.read_text(encoding="utf-8").splitlines()[0])
    assert payload["schema"] == "openai_round_feedback_v2"
    assert payload["conversation"]["messages"][0]["role"] == "user"
    assert payload["round_feedback"]["response_helpful_steps"][0] == {
        "response_index": 0,
        "episode_index": 0,
        "assistant_message_index": 1,
        "feedback_kind": "user",
        "feedback_message_start_index": 2,
        "feedback_message_end_index": 2,
        "feedback_payload": ["next"],
        "response_helpful": "yes",
        "llm_error": False,
    }
    assert payload["round_feedback"]["user_satisfied_episodes"][0] == {
        "episode_index": 0,
        "message_start_index": 0,
        "message_end_index": 1,
        "user_satisfied": "neutral",
        "signal_from_users": ["next"],
        "llm_error": False,
    }
    assert payload["round_feedback"]["user_satisfied_episodes"][1]["message_start_index"] == 2
    assert payload["source_metadata"]["metadata"] == {"source": "unit-test"}

    store.close()


def test_openai_round_feedback_export_converts_anthropic_system_and_tools():
    """Test Anthropic request-level system and tools are preserved in exported OpenAI-compatible payloads."""
    db_path = TEST_DATA_DIR / "test_export_anthropic_request.duckdb"
    output_path = TEST_DATA_DIR / "test_output_anthropic_request.jsonl"

    if db_path.exists():
        db_path.unlink()
    if output_path.exists():
        output_path.unlink()

    store = DuckDBStore(db_path)
    sample_id = store.insert_sample(
        Sample.from_dict(
            {
                "request": {
                    "bodyJson": {
                        "system": [
                            {"type": "text", "text": "You are a coding assistant."},
                            {"type": "text", "text": "Answer in Chinese."},
                        ],
                        "tools": [
                            {
                                "name": "read_file",
                                "description": "Read a file",
                                "input_schema": {
                                    "type": "object",
                                    "properties": {"path": {"type": "string"}},
                                    "required": ["path"],
                                },
                            }
                        ],
                        "messages": [
                            {"role": "user", "content": [{"type": "text", "text": "hello"}]},
                            {
                                "role": "assistant",
                                "content": [
                                    {"type": "text", "text": "我先读文件。"},
                                    {
                                        "type": "tool_use",
                                        "id": "toolu_1",
                                        "name": "read_file",
                                        "input": {"path": "/tmp/demo.txt"},
                                    },
                                ],
                            },
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "tool_result",
                                        "tool_use_id": "toolu_1",
                                        "content": "demo",
                                    }
                                ],
                            },
                        ],
                    }
                }
            }
        )
    )
    sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]
    store.insert_assistant_response_judgment(
        AssistantResponseJudgment(
            sample_uid=sample_uid,
            response_index=0,
            episode_index=0,
            assistant_message_index=1,
            feedback_kind=FeedbackKind.TOOL_RESULT,
            feedback_message_start_index=2,
            feedback_message_end_index=2,
            feedback_payload=["demo"],
            response_helpful="yes",
            llm_error=False,
        )
    )
    store.insert_user_episode_judgment(
        UserEpisodeJudgment(
            sample_uid=sample_uid,
            episode_index=0,
            start_user_message_index=0,
            end_before_user_message_index=1,
            signal_from_users=[],
            user_satisfied="yes",
            llm_error=False,
        )
    )

    exporter = UnifiedExporter(store)
    count = exporter.export(ExportRequest(output_path=output_path, export_format=OPENAI_ROUND_FEEDBACK))

    assert count == 1
    payload = json.loads(output_path.read_text(encoding="utf-8").splitlines()[0])
    assert payload["conversation"]["messages"][0] == {
        "role": "system",
        "content": "You are a coding assistant.\n\nAnswer in Chinese.",
    }
    assert payload["conversation"]["tools"] == [
        {
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read a file",
                "parameters": {
                    "type": "object",
                    "properties": {"path": {"type": "string"}},
                    "required": ["path"],
                },
            },
        }
    ]
    assert payload["conversation"]["messages"][2]["tool_calls"][0]["function"]["name"] == "read_file"
    assert payload["conversation"]["messages"][3] == {
        "role": "tool",
        "tool_call_id": "toolu_1",
        "content": "demo",
    }

    store.close()


def test_openai_round_feedback_export_preserves_openai_tools():
    """Test OpenAI request-level tools are preserved as-is in exported payloads."""
    db_path = TEST_DATA_DIR / "test_export_openai_tools.duckdb"
    output_path = TEST_DATA_DIR / "test_output_openai_tools.jsonl"

    if db_path.exists():
        db_path.unlink()
    if output_path.exists():
        output_path.unlink()

    raw = {
        "request": {
            "bodyJson": {
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": "hello"},
                    {"role": "assistant", "content": "hi"},
                ],
                "tools": [
                    {
                        "type": "function",
                        "function": {
                            "name": "lookup",
                            "description": "Lookup a value",
                            "parameters": {
                                "type": "object",
                                "properties": {"key": {"type": "string"}},
                                "required": ["key"],
                            },
                        },
                    }
                ],
            }
        }
    }

    store = DuckDBStore(db_path)
    store.insert_sample(Sample.from_dict(raw))

    exporter = UnifiedExporter(store)
    count = exporter.export(ExportRequest(output_path=output_path, export_format=OPENAI_ROUND_FEEDBACK))

    assert count == 1
    payload = json.loads(output_path.read_text(encoding="utf-8").splitlines()[0])
    assert payload["conversation"]["messages"][0] == {
        "role": "system",
        "content": "You are a helpful assistant.",
    }
    assert payload["conversation"]["tools"] == raw["request"]["bodyJson"]["tools"]

    store.close()


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
    tool_stats = {
        "response_helpful_rate": 0.9,
        "response_unhelpful_rate": 0.1,
        "user_satisfied_rate": 0.85,
        "user_negative_feedback_rate": 0.15,
        "assistant_response_count": 4,
        "user_episode_count": 2,
        "response_helpful_scored_steps": 4,
        "user_feedback_scored_episodes": 2,
        "has_error": False,
    }
    store.update_sample_tool_stats(sample_id, tool_stats)

    # Generate report
    exporter = ReportExporter(store)
    report = exporter.generate_report()

    assert "summary" in report
    assert report["summary"]["total_samples"] == 1
    assert report["summary"]["processed_samples"] == 1
    assert "avg_response_unhelpful_rate" in report["summary"]
    assert "avg_user_negative_feedback_rate" in report["summary"]
    assert report["judgment_totals"] == {
        "assistant_response_count": 4,
        "user_episode_count": 2,
        "response_helpful_scored_steps": 4,
        "user_feedback_scored_episodes": 2,
    }
    assert "num_turns" in report["semantics"]
    assert "assistant response judgments" in report["semantics"]["response_helpful_rate"]

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
        assert "summary" in report
        assert "judgment_totals" in report
        assert "semantics" in report
        assert "generated_at" in report
        assert "avg_response_helpful_rate" in report["summary"]
        assert "avg_response_unhelpful_rate" in report["summary"]
        assert "avg_user_negative_feedback_rate" in report["summary"]

    store.close()
    print("test_report_export passed")


def test_unified_exporter_no_filter():
    """Test exporter works without requiring judgments."""
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
    exporter = UnifiedExporter(store)
    count = exporter.export(ExportRequest(output_path=output_path, export_format=RAW_JSONL))

    assert count == 3
    with open(output_path) as f:
        lines = f.readlines()
        assert len(lines) == 3

    # Export with id-based filter should also work
    filter_path = TEST_DATA_DIR / "test_no_eval_filtered.jsonl"
    count_filtered = exporter.export(
        ExportRequest(output_path=filter_path, export_format=RAW_JSONL, filter_spec=ExportFilterSpec(selected_ids=[1, 2, 3]))
    )
    assert count_filtered == 3

    store.close()
    print("test_unified_exporter_no_filter passed")


def test_unified_export_preview_supports_estimation():
    """Test preview returns count and estimated bytes."""
    db_path = TEST_DATA_DIR / "test_export_params.duckdb"
    output_path = TEST_DATA_DIR / "test_output_params.jsonl"

    if db_path.exists():
        db_path.unlink()
    if output_path.exists():
        output_path.unlink()

    store = DuckDBStore(db_path)

    for i in range(2):
        raw = {"messages": [{"role": "user", "content": f"Test {i}"}]}
        sample = Sample.from_dict(raw)
        sample_id = store.insert_sample(sample)
        tool_stats = {
            "response_helpful_rate": 0.6 + i * 0.3,
            "user_satisfied_rate": 0.8,
            "total_turns": 1,
            "has_error": False,
        }
        store.update_sample_tool_stats(sample_id, tool_stats)

    exporter = UnifiedExporter(store)
    preview = exporter.preview(ExportFilterSpec(helpful_op=">=", helpful_val=0.8))

    assert preview["count"] == 1
    assert preview["estimated_bytes"] > 0
    store.close()


if __name__ == "__main__":
    test_raw_jsonl_export()
    test_raw_jsonl_export_with_filter()
    test_openai_round_feedback_export_includes_turn_ranges_and_judgments()
    test_openai_round_feedback_export_converts_anthropic_system_and_tools()
    test_openai_round_feedback_export_preserves_openai_tools()
    test_report_generation()
    test_report_export()
    test_unified_exporter_no_filter()
    test_unified_export_preview_supports_estimation()
    print("All exporter tests passed!")
