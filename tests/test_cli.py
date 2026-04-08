from click.testing import CliRunner
from pathlib import Path
import tempfile

import duckdb

from claw_data_filter.cli import cli
from claw_data_filter.importers.jsonl_importer import JSONLImporter


def test_filter_command_accepts_has_error_only(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path("data").mkdir()

    db_path = Path("data/cli_filter.duckdb")
    input_file = Path("data/input.jsonl")
    output_file = Path("data/output.jsonl")

    input_file.write_text(
        '{"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]}\n',
        encoding="utf-8",
    )

    importer = JSONLImporter(db_path)
    try:
        sample_count = importer.import_file(input_file)
    finally:
        importer.close()

    assert sample_count == 1

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--db-path",
            str(db_path),
            "filter",
            "--has-error",
            "false",
            "--export",
            str(output_file),
        ],
        obj={},
    )

    assert result.exit_code == 0
    assert output_file.exists()


def test_filter_command_accepts_session_merge_filters(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path("data").mkdir()

    db_path = Path("data/cli_filter_merge.duckdb")
    input_file = Path("data/input_merge.jsonl")
    output_file = Path("data/output_merge.jsonl")

    input_file.write_text(
        '{"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]}\n',
        encoding="utf-8",
    )

    importer = JSONLImporter(db_path)
    try:
        sample_id = importer.import_file(input_file)
    finally:
        importer.close()

    conn = duckdb.connect(str(db_path))
    conn.execute("UPDATE samples SET session_merge_status = 'keep', session_merge_keep = TRUE")
    conn.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--db-path",
            str(db_path),
            "filter",
            "--session-merge-keep",
            "true",
            "--session-merge-status",
            "keep",
            "--export",
            str(output_file),
        ],
        obj={},
    )

    assert sample_id == 1
    assert result.exit_code == 0
    assert output_file.exists()


def test_filter_command_accepts_empty_response_filter(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path("data").mkdir()

    db_path = Path("data/cli_filter_empty.duckdb")
    input_file = Path("data/input_empty.jsonl")
    output_file = Path("data/output_empty.jsonl")

    input_file.write_text(
        '{"messages":[{"role":"user","content":"hello"}]}\n',
        encoding="utf-8",
    )

    importer = JSONLImporter(db_path)
    try:
        importer.import_file(input_file)
    finally:
        importer.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--db-path",
            str(db_path),
            "filter",
            "--empty-response",
            "true",
            "--export",
            str(output_file),
        ],
        obj={},
    )

    assert result.exit_code == 0
    assert output_file.exists()


def test_session_merge_command_supports_dry_run(monkeypatch):
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.chdir(tmpdir)
        Path(tmpdir, "data").mkdir()
        db_path = Path(tmpdir) / "cli_session_merge.duckdb"
        importer = JSONLImporter(db_path)
        input_file = Path(tmpdir) / "data" / "input.jsonl"
        input_file.write_text(
            '{"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]}\n',
            encoding="utf-8",
        )
        try:
            importer.import_file(input_file)
        finally:
            importer.close()

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "--db-path",
                str(db_path),
                "session-merge",
                "--dry-run",
                "--workers",
                "1",
                "--batch-size",
                "10",
                "--min-prefix-turns",
                "2",
            ],
            obj={},
        )

        assert result.exit_code == 0
        assert "Session Merge Summary" in result.output


def test_filter_command_accepts_negative_feedback_rate(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path("data").mkdir()

    db_path = Path("data/cli_filter_negative.duckdb")
    input_file = Path("data/input_negative.jsonl")
    output_file = Path("data/output_negative.jsonl")

    input_file.write_text(
        '{"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]}\n',
        encoding="utf-8",
    )

    importer = JSONLImporter(db_path)
    try:
        importer.import_file(input_file)
    finally:
        importer.close()

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--db-path",
            str(db_path),
            "filter",
            "--user-negative-feedback-rate",
            ">=0.0",
            "--export",
            str(output_file),
        ],
        obj={},
    )

    assert result.exit_code == 0
    assert output_file.exists()