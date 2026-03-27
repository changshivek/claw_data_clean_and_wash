"""Tests for JSONL importer."""
import tempfile
import json
from pathlib import Path
from claw_data_filter.importers.jsonl_importer import JSONLImporter


def test_import_single_line():
    """Test importing a single line."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        input_file = Path(tmpdir) / "input.jsonl"

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
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        input_file = Path(tmpdir) / "input.jsonl"

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
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        input_file = Path(tmpdir) / "input.jsonl"

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
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        input_file = Path(tmpdir) / "input.jsonl"

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


if __name__ == "__main__":
    test_import_single_line()
    test_import_multiple_lines()
    test_import_skips_malformed_lines()
    test_import_closes_on_error()
    print("All JSONL importer tests passed!")