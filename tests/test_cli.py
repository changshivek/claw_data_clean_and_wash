import logging
from click.testing import CliRunner
from pathlib import Path
import tempfile

import duckdb

from claw_data_filter.cli import _configure_logging, cli
from claw_data_filter.exporters.unified_exporter import OPENAI_ROUND_FEEDBACK
from claw_data_filter.importers.jsonl_importer import JSONLImporter
from claw_data_filter.models.sample import Sample
from claw_data_filter.storage.duckdb_store import DuckDBStore


def test_configure_logging_suppresses_http_client_info_logs():
    logging.getLogger("httpx").setLevel(logging.INFO)
    logging.getLogger("httpcore").setLevel(logging.INFO)

    _configure_logging()

    assert logging.getLogger("httpx").level == logging.WARNING
    assert logging.getLogger("httpcore").level == logging.WARNING


def test_round_feedback_sample_command_runs_in_isolated_db(tmp_path, monkeypatch):
    source_db = tmp_path / "source.duckdb"
    isolated_db = tmp_path / "isolated.duckdb"
    store = DuckDBStore(source_db)

    raw_json = {
        "request": {
            "bodyJson": {
                "messages": [
                    {"role": "user", "content": "Hi"},
                    {"role": "assistant", "content": "Hello"},
                    {"role": "tool", "content": "tool ok"},
                    {"role": "assistant", "content": "Anything else?"},
                ]
            }
        }
    }

    try:
        sample_id = store.insert_sample(Sample.from_dict(raw_json))
        sample_uid = store.get_sample_by_id(sample_id)["sample_uid"]
    finally:
        store.close()

    class FakeLLM:
        def __init__(self, *args, **kwargs):
            pass

        async def chat(self, messages, max_tokens=1024, temperature=0.1):
            if "response_progress" in messages[0]["content"]:
                return "response_progress=yes"
            return "user_satisfied=yes"

        async def close(self):
            return None

    monkeypatch.setattr("claw_data_filter.llm.async_client.AsyncLLMClient", FakeLLM)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--db-path",
            str(source_db),
            "round-feedback-sample",
            "--sample-uid",
            sample_uid,
            "--isolated-db-path",
            str(isolated_db),
            "--workers",
            "1",
        ],
        obj={},
    )

    assert result.exit_code == 0
    assert "Sample summary:" in result.output
    assert "Isolated sample complete:" in result.output

    isolated_store = DuckDBStore(isolated_db)
    try:
        isolated_sample = isolated_store.get_sample_by_uid(sample_uid)
        assert isolated_sample is not None
        assert isolated_sample["processing_status"] == "completed"
        assert isolated_sample["progress_rate"] == 1.0
        assert isolated_sample["satisfied_rate"] == 1.0
    finally:
        isolated_store.close()


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


def test_import_command_supports_parallel_options(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path("data").mkdir()

    db_path = Path("data/cli_parallel_import.duckdb")
    input_file = Path("data/parallel_input.jsonl")
    input_file.write_text(
        """
{"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"}]}
{"messages":[{"role":"user","content":"again"},{"role":"assistant","content":"hello again"}]}
        """.strip()
        + "\n",
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--db-path",
            str(db_path),
            "import",
            "--workers",
            "2",
            "--chunk-size",
            "1",
            str(input_file),
        ],
        obj={},
    )

    assert result.exit_code == 0
    assert "Successfully imported 2 samples." in result.output


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


def test_filter_command_accepts_openai_round_feedback_export(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path("data").mkdir()

    db_path = Path("data/cli_filter_feedback.duckdb")
    input_file = Path("data/input_feedback.jsonl")
    output_file = Path("data/output_feedback.jsonl")

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
            "--export-format",
            OPENAI_ROUND_FEEDBACK,
            "--export",
            str(output_file),
        ],
        obj={},
    )

    assert result.exit_code == 0
    payload = output_file.read_text(encoding="utf-8").splitlines()[0]
    assert '"schema": "openai_round_feedback_v2"' in payload