"""Tests for JSONL importer."""
import json
import os
from pathlib import Path
import claw_data_filter.importers.jsonl_importer as importer_module
from claw_data_filter.importers.jsonl_importer import JSONLImporter
from claw_data_filter.models.sample import Sample, extract_import_fields_from_payload

# Use data directory for tests
TEST_DATA_DIR = Path(__file__).parent.parent / "data"
TEST_DATA_DIR.mkdir(exist_ok=True)


def test_import_single_line():
    """Test importing a single line."""
    db_path = TEST_DATA_DIR / "test_import_single.duckdb"
    input_file = TEST_DATA_DIR / "test_input_single.jsonl"

    # Clean up any existing test files
    if db_path.exists():
        db_path.unlink()
    if input_file.exists():
        input_file.unlink()

    # Write test data
    with open(input_file, "w") as f:
        f.write(json.dumps({
            "messages": [
                {"role": "user", "content": "What's the weather?"},
                {"role": "assistant", "content": "Let me check"},
            ]
        }) + "\n")

    importer = JSONLImporter(db_path)
    count = importer.import_file(input_file)

    assert count == 1
    assert importer.store.get_sample_count() == 1

    importer.close()
    print("test_import_single_line passed")


def test_import_multiple_lines():
    """Test importing multiple lines."""
    db_path = TEST_DATA_DIR / "test_import_multi.duckdb"
    input_file = TEST_DATA_DIR / "test_input_multi.jsonl"

    if db_path.exists():
        db_path.unlink()
    if input_file.exists():
        input_file.unlink()

    # Write multiple test records
    records = [
        {"messages": [{"role": "user", "content": f"Query {i}"}, {"role": "assistant", "content": f"Answer {i}"}]}
        for i in range(5)
    ]
    with open(input_file, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    importer = JSONLImporter(db_path)
    count = importer.import_file(input_file)

    assert count == 5
    assert importer.store.get_sample_count() == 5

    importer.close()
    print("test_import_multiple_lines passed")


def test_import_skips_malformed_lines():
    """Test that malformed JSON lines are skipped with skip_errors=True."""
    db_path = TEST_DATA_DIR / "test_import_skip.duckdb"
    input_file = TEST_DATA_DIR / "test_input_skip.jsonl"

    if db_path.exists():
        db_path.unlink()
    if input_file.exists():
        input_file.unlink()

    # Write mixed valid and invalid lines
    with open(input_file, "w") as f:
        f.write(json.dumps({"messages": [{"role": "user", "content": "Valid 1"}]}) + "\n")
        f.write("this is not json\n")
        f.write(json.dumps({"messages": [{"role": "user", "content": "Valid 2"}]}) + "\n")
        f.write("also not json\n")
        f.write(json.dumps({"messages": [{"role": "user", "content": "Valid 3"}]}) + "\n")

    importer = JSONLImporter(db_path)
    count = importer.import_file(input_file, skip_errors=True)

    assert count == 3
    assert importer.store.get_sample_count() == 3

    importer.close()
    print("test_import_skips_malformed_lines passed")


def test_import_closes_on_error():
    """Test that errors raise when skip_errors=False."""
    db_path = TEST_DATA_DIR / "test_import_error.duckdb"
    input_file = TEST_DATA_DIR / "test_input_error.jsonl"

    if db_path.exists():
        db_path.unlink()
    if input_file.exists():
        input_file.unlink()

    with open(input_file, "w") as f:
        f.write(json.dumps({"messages": [{"role": "user", "content": "Valid"}]}) + "\n")
        f.write("invalid json\n")

    importer = JSONLImporter(db_path)
    try:
        importer.import_file(input_file, skip_errors=False)
        assert False, "Should have raised"
    except json.JSONDecodeError:
        pass  # Expected
    finally:
        importer.close()
    print("test_import_closes_on_error passed")


def test_import_unirouter_payload_populates_derived_fields():
    """Test UniRouter payload import populates user and assistant derived fields."""
    db_path = TEST_DATA_DIR / "test_import_unirouter.duckdb"
    input_file = TEST_DATA_DIR / "test_input_unirouter.jsonl"

    if db_path.exists():
        db_path.unlink()
    if input_file.exists():
        input_file.unlink()

    with open(input_file, "w") as f:
        f.write(json.dumps({
            "request": {
                "bodyJson": {
                    "messages": [
                        {"role": "user", "content": "Hello"},
                        {"role": "assistant", "content": "Hi there"},
                    ]
                }
            }
        }) + "\n")

    importer = JSONLImporter(db_path)
    count = importer.import_file(input_file)

    assert count == 1
    row = importer.store.conn.execute(
        "SELECT user_query, assistant_response, expected_judgment_count FROM samples LIMIT 1"
    ).fetchone()
    assert row == ("Hello", "Hi there", 2)

    importer.close()
    print("test_import_unirouter_payload_populates_derived_fields passed")


def test_import_force_serial_env_overrides_parallel_workers(monkeypatch):
    db_path = TEST_DATA_DIR / "test_import_force_serial.duckdb"
    input_file = TEST_DATA_DIR / "test_input_force_serial.jsonl"

    if db_path.exists():
        db_path.unlink()
    if input_file.exists():
        input_file.unlink()

    with open(input_file, "w") as f:
        for i in range(3):
            f.write(json.dumps({"messages": [{"role": "user", "content": f"Query {i}"}]}) + "\n")

    monkeypatch.setenv("CLAW_IMPORT_FORCE_SERIAL", "1")
    importer = JSONLImporter(db_path)
    try:
        summary = importer.import_lines_with_summary(iter(input_file.read_text(encoding="utf-8").splitlines(True)), workers=4)
        assert summary.imported_count == 3
        assert importer.store.get_sample_count() == 3
    finally:
        importer.close()

    print("test_import_force_serial_env_overrides_parallel_workers passed")


def test_import_parallel_respects_max_pending_chunks(monkeypatch):
    db_path = TEST_DATA_DIR / "test_import_pending.duckdb"
    input_file = TEST_DATA_DIR / "test_input_pending.jsonl"

    if db_path.exists():
        db_path.unlink()
    if input_file.exists():
        input_file.unlink()

    with open(input_file, "w") as f:
        for i in range(6):
            f.write(json.dumps({"messages": [{"role": "user", "content": f"Query {i}"}]}) + "\n")

    class FakeFuture:
        def __init__(self, fn, args, tracker):
            self._fn = fn
            self._args = args
            self._tracker = tracker

        def result(self):
            self._tracker["outstanding"] -= 1
            return self._fn(*self._args)

    class FakeExecutor:
        def __init__(self, *args, **kwargs):
            self.tracker = kwargs.pop("tracker")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def submit(self, fn, *args):
            self.tracker["outstanding"] += 1
            self.tracker["peak"] = max(self.tracker["peak"], self.tracker["outstanding"])
            return FakeFuture(fn, args, self.tracker)

    tracker = {"outstanding": 0, "peak": 0}
    monkeypatch.setattr(
        importer_module,
        "ProcessPoolExecutor",
        lambda *args, **kwargs: FakeExecutor(*args, tracker=tracker, **kwargs),
    )

    importer = JSONLImporter(db_path)
    try:
        summary = importer.import_lines_with_summary(
            iter(input_file.read_text(encoding="utf-8").splitlines(True)),
            workers=4,
            chunk_size=1,
            max_pending_chunks=1,
        )
        assert summary.imported_count == 6
        assert tracker["peak"] == 1
    finally:
        importer.close()


def test_import_reconnects_store_every_n_chunks(monkeypatch):
    db_path = TEST_DATA_DIR / "test_import_reconnect.duckdb"
    input_file = TEST_DATA_DIR / "test_input_reconnect.jsonl"

    if db_path.exists():
        db_path.unlink()
    if input_file.exists():
        input_file.unlink()

    with open(input_file, "w") as f:
        for i in range(5):
            f.write(json.dumps({"messages": [{"role": "user", "content": f"Query {i}"}]}) + "\n")

    importer = JSONLImporter(db_path)
    reconnect_calls = {"count": 0}
    original = importer._reconnect_store

    def tracked_reconnect():
        reconnect_calls["count"] += 1
        original()

    monkeypatch.setattr(importer, "_reconnect_store", tracked_reconnect)
    try:
        summary = importer.import_file(input_file, workers=1, chunk_size=1, reconnect_every_chunks=2)
        assert summary == 5
        assert reconnect_calls["count"] == 2
    finally:
        importer.close()


def test_extract_import_fields_matches_sample_from_dict():
    payload = {
        "request": {
            "bodyJson": {
                "messages": [
                    {"role": "system", "content": "You are helpful."},
                    {"role": "user", "content": [{"type": "text", "text": "Hello"}]},
                    {
                        "role": "assistant",
                        "content": "Calling tool",
                        "tool_calls": [{"id": "call_1", "type": "function", "function": {"name": "search"}}],
                    },
                    {"role": "tool", "content": "error: upstream timeout"},
                    {"role": "assistant", "content": [{"type": "text", "text": "Final answer"}]},
                ]
            }
        }
    }

    fields = extract_import_fields_from_payload(payload)
    sample = Sample.from_dict(payload)

    assert fields["sample_uid"] == sample.sample_uid
    assert fields["raw_json"] == sample.raw_json
    assert fields["user_query"] == sample.user_query
    assert fields["assistant_response"] == sample.assistant_response
    assert fields["num_turns"] == sample.num_turns
    assert fields["expected_judgment_count"] == sample.expected_judgment_count
    assert fields["expected_response_judgment_count"] == sample.expected_response_judgment_count
    assert fields["expected_episode_judgment_count"] == sample.expected_episode_judgment_count
    assert fields["num_tool_calls"] == sample.num_tool_calls
    assert fields["empty_response"] == sample.empty_response
    assert fields["has_error"] == sample.has_error


if __name__ == "__main__":
    test_import_single_line()
    test_import_multiple_lines()
    test_import_skips_malformed_lines()
    test_import_closes_on_error()
    test_import_unirouter_payload_populates_derived_fields()
    print("All JSONL importer tests passed!")
