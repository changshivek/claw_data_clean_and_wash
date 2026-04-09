"""Tests for JSONL importer."""
import json
import os
from pathlib import Path
from claw_data_filter.importers.jsonl_importer import JSONLImporter

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


if __name__ == "__main__":
    test_import_single_line()
    test_import_multiple_lines()
    test_import_skips_malformed_lines()
    test_import_closes_on_error()
    test_import_unirouter_payload_populates_derived_fields()
    print("All JSONL importer tests passed!")
