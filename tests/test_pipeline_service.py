"""Tests for the incremental pipeline service."""

from __future__ import annotations

import gzip
import io
import json
import tarfile
from pathlib import Path

from click.testing import CliRunner

from claw_data_filter.cli import cli
from claw_data_filter.pipeline.config import PipelineConfig
from claw_data_filter.pipeline.service import PipelineService


def _write_unisound_config(config_path: Path) -> None:
    config_path.write_text(
        json.dumps(
            {
                "domain": "Agent",
                "task_describe": "pipeline_test",
                "data_source": "pipeline_test_source",
                "default_answer_key": "Assistant",
                "id_strategy": "source_metadata_then_sample_uid",
                "preserve_extensions": True,
                "preserve_round_feedback": True,
                "preserve_conversation": True,
                "task_describe_en_suffix": False,
                "turn_feedback_field": "round_feedback",
                "think_split_strategy": "tag",
                "english_detection_mode": "simple",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _build_config(
    *,
    source_dir: Path,
    unpack_dir: Path,
    work_dir: Path,
    db_path: Path,
    export_dir: Path,
    log_dir: Path,
    unisound_config_path: Path,
) -> PipelineConfig:
    return PipelineConfig.model_validate(
        {
            "paths": {
                "source_dir": source_dir,
                "unpack_dir": unpack_dir,
                "work_dir": work_dir,
                "db_path": db_path,
                "export_dir": export_dir,
                "log_dir": log_dir,
            },
            "import": {
                "workers": 1,
                "chunk_size": 16,
                "skip_errors": False,
            },
            "session_merge": {
                "enabled": False,
            },
            "round_feedback": {
                "enabled": False,
                "workers": 1,
                "batch_size": 4,
            },
            "export": {
                "response_progress_rate": None,
                "user_satisfied_rate": None,
                "user_negative_feedback_rate": None,
                "empty_response": False,
                "session_merge_keep": None,
                "session_merge_status": None,
                "has_error": None,
                "keep_intermediate": True,
                "unisound_config_path": unisound_config_path,
            },
        }
    )


def _items_payload() -> bytes:
    lines = [
        json.dumps(
            {
                "messages": [
                    {"role": "system", "content": "You are a helper."},
                    {"role": "user", "content": "hello"},
                    {"role": "assistant", "content": "world"},
                ],
                "metadata": {"id": "sample-1"},
            },
            ensure_ascii=False,
        ),
        json.dumps(
            {
                "messages": [
                    {"role": "user", "content": "next"},
                    {"role": "assistant", "content": "done"},
                ],
                "metadata": {"id": "sample-2"},
            },
            ensure_ascii=False,
        ),
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _write_nested_archive(archive_path: Path) -> None:
    items_bytes = _items_payload()
    gzip_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gzip_buffer, mode="wb") as gz_handle:
        gz_handle.write(items_bytes)

    inner_tar_buffer = io.BytesIO()
    with tarfile.open(fileobj=inner_tar_buffer, mode="w") as inner_tar:
        info = tarfile.TarInfo(name="nested/items.jsonl.gz")
        payload = gzip_buffer.getvalue()
        info.size = len(payload)
        inner_tar.addfile(info, io.BytesIO(payload))

    with tarfile.open(archive_path, mode="w") as outer_tar:
        inner_payload = inner_tar_buffer.getvalue()
        info = tarfile.TarInfo(name="bundle/inner.tar")
        info.size = len(inner_payload)
        outer_tar.addfile(info, io.BytesIO(inner_payload))


def _write_toml_config(config_path: Path, paths: dict[str, Path], unisound_config_path: Path) -> None:
    config_path.write_text(
        "\n".join(
            [
                "[paths]",
                f'source_dir = "{paths["source_dir"]}"',
                f'unpack_dir = "{paths["unpack_dir"]}"',
                f'work_dir = "{paths["work_dir"]}"',
                f'db_path = "{paths["db_path"]}"',
                f'export_dir = "{paths["export_dir"]}"',
                f'log_dir = "{paths["log_dir"]}"',
                "",
                "[import]",
                "workers = 1",
                "chunk_size = 16",
                "skip_errors = false",
                "",
                "[session_merge]",
                "enabled = false",
                "",
                "[round_feedback]",
                "enabled = false",
                "workers = 1",
                "batch_size = 4",
                "",
                "[export]",
                "response_progress_rate = \"\"",
                "user_satisfied_rate = \"\"",
                "user_negative_feedback_rate = \"\"",
                "keep_intermediate = true",
                f'unisound_config_path = "{unisound_config_path}"',
            ]
        ),
        encoding="utf-8",
    )


def test_pipeline_service_runs_once_and_exports_incremental_files(tmp_path: Path):
    source_dir = tmp_path / "source"
    unpack_dir = tmp_path / "unpack"
    work_dir = tmp_path / "work"
    db_path = tmp_path / "db" / "pipeline.duckdb"
    export_dir = tmp_path / "exports"
    log_dir = tmp_path / "logs"
    unisound_config_path = tmp_path / "unisound_config.json"

    for path in (source_dir, unpack_dir, work_dir, export_dir, log_dir, db_path.parent):
        path.mkdir(parents=True, exist_ok=True)
    _write_unisound_config(unisound_config_path)

    archive_path = source_dir / "incremental_bundle.tar"
    _write_nested_archive(archive_path)

    service = PipelineService(
        _build_config(
            source_dir=source_dir,
            unpack_dir=unpack_dir,
            work_dir=work_dir,
            db_path=db_path,
            export_dir=export_dir,
            log_dir=log_dir,
            unisound_config_path=unisound_config_path,
        )
    )
    try:
        summary = service.run_once()
        assert summary["status"] == "completed"
        assert summary["processed_files"] == 1
        assert summary["imported_samples"] == 2
        assert summary["exported_samples"] == 2
        assert summary["exported_files"] == 1
        assert summary["unisound_files"] == 1

        exported_feedback = sorted(export_dir.glob("*.openai_round_feedback.jsonl"))
        exported_unisound = sorted(export_dir.glob("*.unisound.jsonl"))
        assert len(exported_feedback) == 1
        assert len(exported_unisound) == 1
        assert len(exported_feedback[0].read_text(encoding="utf-8").splitlines()) == 2
        assert len(exported_unisound[0].read_text(encoding="utf-8").splitlines()) == 2

        run_samples_row = service.store.conn.execute("SELECT COUNT(*) FROM pipeline_run_samples").fetchone()
        assert run_samples_row is not None
        run_samples = run_samples_row[0]
        assert run_samples == 2
        source_status = service.store.conn.execute(
            "SELECT status, imported_samples FROM pipeline_source_files WHERE source_path = ?",
            [str(archive_path)],
        ).fetchone()
        assert source_status == ("completed", 2)
    finally:
        service.close()


def test_pipeline_service_skips_unchanged_archives_on_second_run(tmp_path: Path):
    source_dir = tmp_path / "source"
    unpack_dir = tmp_path / "unpack"
    work_dir = tmp_path / "work"
    db_path = tmp_path / "db" / "pipeline.duckdb"
    export_dir = tmp_path / "exports"
    log_dir = tmp_path / "logs"
    unisound_config_path = tmp_path / "unisound_config.json"

    for path in (source_dir, unpack_dir, work_dir, export_dir, log_dir, db_path.parent):
        path.mkdir(parents=True, exist_ok=True)
    _write_unisound_config(unisound_config_path)

    archive_path = source_dir / "incremental_bundle.tar"
    _write_nested_archive(archive_path)

    config = _build_config(
        source_dir=source_dir,
        unpack_dir=unpack_dir,
        work_dir=work_dir,
        db_path=db_path,
        export_dir=export_dir,
        log_dir=log_dir,
        unisound_config_path=unisound_config_path,
    )

    service = PipelineService(config)
    try:
        first_summary = service.run_once()
        second_summary = service.run_once()
        assert first_summary["imported_samples"] == 2
        assert second_summary["imported_samples"] == 0
        assert second_summary["processed_files"] == 0
        assert second_summary["exported_samples"] == 0
    finally:
        service.close()


def test_pipeline_run_cli_loads_toml_and_executes(tmp_path: Path):
    source_dir = tmp_path / "source"
    unpack_dir = tmp_path / "unpack"
    work_dir = tmp_path / "work"
    db_path = tmp_path / "db" / "pipeline.duckdb"
    export_dir = tmp_path / "exports"
    log_dir = tmp_path / "logs"
    unisound_config_path = tmp_path / "unisound_config.json"
    config_path = tmp_path / "pipeline.toml"

    for path in (source_dir, unpack_dir, work_dir, export_dir, log_dir, db_path.parent):
        path.mkdir(parents=True, exist_ok=True)
    _write_unisound_config(unisound_config_path)
    _write_nested_archive(source_dir / "cli_bundle.tar")
    _write_toml_config(
        config_path,
        {
            "source_dir": source_dir,
            "unpack_dir": unpack_dir,
            "work_dir": work_dir,
            "db_path": db_path,
            "export_dir": export_dir,
            "log_dir": log_dir,
        },
        unisound_config_path,
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["pipeline-run", "--config", str(config_path)], obj={})

    assert result.exit_code == 0
    assert "Pipeline Run Summary" in result.output
    assert "imported_samples: 2" in result.output