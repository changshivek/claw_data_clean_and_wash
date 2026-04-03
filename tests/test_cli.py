from click.testing import CliRunner
from pathlib import Path

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
    assert "Exported" in result.output