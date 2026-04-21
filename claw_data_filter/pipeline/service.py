"""Incremental tar-to-export pipeline orchestration."""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import logging
import os
import re
import shutil
import tarfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

from claw_data_filter.config import Config
from claw_data_filter.exporters.unified_exporter import ExportFilterSpec, ExportRequest, UnifiedExporter
from claw_data_filter.importers.jsonl_importer import JSONLImporter
from claw_data_filter.llm.async_client import AsyncLLMClient
from claw_data_filter.models.sample import Sample
from claw_data_filter.pipeline.config import PipelineConfig
from claw_data_filter.processors.round_feedback import RoundFeedbackProcessor
from claw_data_filter.session_merge import run_session_merge
from claw_data_filter.storage.duckdb_store import DuckDBStore

from scripts.unisound_export import build_report as build_unisound_report
from scripts.unisound_export import convert_file as convert_unisound_file
from scripts.unisound_export import load_config as load_unisound_config

logger = logging.getLogger(__name__)

RATE_PATTERN = re.compile(r"^(>=|<=|>|<|!=|=)\s*([\d.]+)$")
ARCHIVE_SUFFIXES = (
    ".tar",
    ".tar.gz",
    ".tgz",
    ".tar.bz2",
    ".tbz2",
    ".tar.xz",
    ".txz",
    ".gz",
)


@dataclass(slots=True)
class SourceFilePlan:
    source_path: Path
    source_name: str
    size_bytes: int
    mtime_ns: int
    fingerprint: str


@dataclass(slots=True)
class ProcessedFileResult:
    source_path: Path
    source_name: str
    extracted_dir: Path
    items_paths: list[Path]
    imported_sample_uids: list[str]
    imported_count: int


class PipelineService:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.store = DuckDBStore(config.paths.db_path)
        self.exporter = UnifiedExporter(self.store)
        self.repo_root = Path(__file__).resolve().parents[2]
        self._ensure_directories()
        self._ensure_pipeline_schema()

    def close(self) -> None:
        self.store.close()

    def run_once(self) -> dict[str, Any]:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + f"_{uuid.uuid4().hex[:8]}"
        log_path = self.config.paths.log_dir / f"pipeline_{run_id}.log"
        imported_total = 0
        exported_total = 0
        exported_files = 0
        unisound_files = 0
        processed_results: list[ProcessedFileResult] = []
        self._insert_pipeline_run(run_id, log_path)

        with self._pipeline_log_handler(log_path):
            logger.info("Incremental pipeline run started: run_id=%s", run_id)
            try:
                plans = self._discover_source_files(run_id)
                logger.info("Discovered candidate archives: run_id=%s count=%s", run_id, len(plans))
                for plan in plans:
                    result = self._process_source_file(run_id, plan)
                    if result is not None:
                        processed_results.append(result)
                        imported_total += result.imported_count

                all_imported_sample_uids = [
                    sample_uid
                    for result in processed_results
                    for sample_uid in result.imported_sample_uids
                ]

                if all_imported_sample_uids:
                    if self.config.session_merge.enabled:
                        self._run_session_merge()
                    if self.config.round_feedback.enabled:
                        self._run_round_feedback_for_samples(all_imported_sample_uids)

                for result in processed_results:
                    export_summary = self._export_file_result(run_id, result)
                    exported_total += export_summary["qualified_samples"]
                    exported_files += 1 if export_summary["qualified_samples"] > 0 else 0
                    unisound_files += 1 if export_summary["qualified_samples"] > 0 else 0

                summary = {
                    "run_id": run_id,
                    "status": "completed",
                    "processed_files": len(processed_results),
                    "imported_samples": imported_total,
                    "exported_samples": exported_total,
                    "exported_files": exported_files,
                    "unisound_files": unisound_files,
                    "log_path": str(log_path),
                }
                self._finish_pipeline_run(run_id, "completed", summary)
                logger.info("Incremental pipeline run completed: %s", json.dumps(summary, ensure_ascii=False))
                return summary
            except Exception as exc:
                logger.exception("Incremental pipeline run failed: run_id=%s", run_id)
                failure_summary = {
                    "run_id": run_id,
                    "status": "failed",
                    "processed_files": len(processed_results),
                    "imported_samples": imported_total,
                    "exported_samples": exported_total,
                    "exported_files": exported_files,
                    "unisound_files": unisound_files,
                    "log_path": str(log_path),
                    "error": str(exc),
                }
                self._finish_pipeline_run(run_id, "failed", failure_summary, error_message=str(exc))
                raise

    def _ensure_directories(self) -> None:
        for path in (
            self.config.paths.unpack_dir,
            self.config.paths.work_dir,
            self.config.paths.export_dir,
            self.config.paths.log_dir,
            self.config.paths.db_path.parent,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def _ensure_pipeline_schema(self) -> None:
        self.store.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                status TEXT,
                source_dir TEXT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                processed_files INTEGER,
                imported_samples INTEGER,
                exported_samples INTEGER,
                exported_files INTEGER,
                unisound_files INTEGER,
                log_path TEXT,
                error_message TEXT
            )
            """
        )
        self.store.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_source_files (
                source_path TEXT PRIMARY KEY,
                source_name TEXT,
                size_bytes BIGINT,
                mtime_ns BIGINT,
                fingerprint TEXT,
                status TEXT,
                last_seen_at TIMESTAMP,
                last_run_id TEXT,
                processed_at TIMESTAMP,
                extracted_dir TEXT,
                items_paths JSON,
                imported_samples INTEGER,
                error_message TEXT
            )
            """
        )
        self.store.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_run_files (
                run_file_id TEXT PRIMARY KEY,
                run_id TEXT,
                source_path TEXT,
                source_name TEXT,
                status TEXT,
                extracted_dir TEXT,
                items_paths JSON,
                imported_samples INTEGER,
                qualified_samples INTEGER,
                export_path TEXT,
                export_report_path TEXT,
                unisound_path TEXT,
                unisound_report_path TEXT,
                error_message TEXT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP
            )
            """
        )
        self.store.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_run_samples (
                run_id TEXT,
                source_path TEXT,
                sample_uid TEXT,
                PRIMARY KEY (run_id, source_path, sample_uid)
            )
            """
        )

    @contextmanager
    def _pipeline_log_handler(self, log_path: Path) -> Iterator[None]:
        handler = logging.FileHandler(log_path, encoding="utf-8")
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)
        try:
            yield
        finally:
            root_logger.removeHandler(handler)
            handler.close()

    def _discover_source_files(self, run_id: str) -> list[SourceFilePlan]:
        plans: list[SourceFilePlan] = []
        for path in sorted(self.config.paths.source_dir.rglob("*")):
            if not path.is_file() or not self._is_supported_archive(path):
                continue
            stat = path.stat()
            fingerprint = self._fingerprint_file(path, stat.st_size, stat.st_mtime_ns)
            plans.append(
                SourceFilePlan(
                    source_path=path,
                    source_name=path.name,
                    size_bytes=stat.st_size,
                    mtime_ns=stat.st_mtime_ns,
                    fingerprint=fingerprint,
                )
            )

        pending: list[SourceFilePlan] = []
        now = datetime.now()
        for plan in plans:
            existing = self.store.conn.execute(
                "SELECT fingerprint, status FROM pipeline_source_files WHERE source_path = ?",
                [str(plan.source_path)],
            ).fetchone()
            if existing and existing[0] == plan.fingerprint and existing[1] == "completed":
                self.store.conn.execute(
                    "UPDATE pipeline_source_files SET last_seen_at = ?, last_run_id = ? WHERE source_path = ?",
                    [now, run_id, str(plan.source_path)],
                )
                continue
            pending.append(plan)
            self._upsert_source_file(
                plan=plan,
                status="pending",
                run_id=run_id,
                last_seen_at=now,
            )
        return pending

    def _process_source_file(self, run_id: str, plan: SourceFilePlan) -> ProcessedFileResult | None:
        run_file_id = f"{run_id}:{hashlib.sha1(str(plan.source_path).encode('utf-8')).hexdigest()[:16]}"
        started_at = datetime.now()
        self._upsert_run_file(
            run_file_id=run_file_id,
            run_id=run_id,
            source_path=plan.source_path,
            source_name=plan.source_name,
            status="processing",
            started_at=started_at,
        )
        self._upsert_source_file(plan=plan, status="processing", run_id=run_id, last_seen_at=started_at)

        try:
            extracted_dir = self.config.paths.unpack_dir / self._safe_file_stem(plan.source_name)
            if extracted_dir.exists():
                shutil.rmtree(extracted_dir)
            extracted_dir.mkdir(parents=True, exist_ok=True)

            items_paths = self._extract_items_jsonl(plan.source_path, extracted_dir)
            if not items_paths:
                raise RuntimeError(f"No items.jsonl found after extraction: {plan.source_path}")

            imported_sample_uids: list[str] = []
            imported_count = 0
            for items_path in items_paths:
                parsed_sample_uids = self._collect_sample_uids(items_path)
                new_sample_uids = self._new_sample_uids(parsed_sample_uids)
                if not new_sample_uids:
                    continue

                importer = JSONLImporter(self.config.paths.db_path)
                try:
                    with items_path.open("r", encoding="utf-8") as handle:
                        imported_count += importer.import_lines(
                            handle,
                            skip_errors=self.config.import_settings.skip_errors,
                            workers=self.config.import_settings.workers,
                            chunk_size=self.config.import_settings.chunk_size,
                        )
                finally:
                    importer.close()

                imported_sample_uids.extend(new_sample_uids)

            imported_sample_uids = self._dedupe_keep_order(imported_sample_uids)
            if imported_sample_uids:
                self.store.conn.executemany(
                    "INSERT OR IGNORE INTO pipeline_run_samples (run_id, source_path, sample_uid) VALUES (?, ?, ?)",
                    [(run_id, str(plan.source_path), sample_uid) for sample_uid in imported_sample_uids],
                )

            finished_at = datetime.now()
            self._upsert_run_file(
                run_file_id=run_file_id,
                run_id=run_id,
                source_path=plan.source_path,
                source_name=plan.source_name,
                status="completed",
                extracted_dir=extracted_dir,
                items_paths=items_paths,
                imported_samples=len(imported_sample_uids),
                started_at=started_at,
                finished_at=finished_at,
            )
            self._upsert_source_file(
                plan=plan,
                status="completed",
                run_id=run_id,
                last_seen_at=finished_at,
                processed_at=finished_at,
                extracted_dir=extracted_dir,
                items_paths=items_paths,
                imported_samples=len(imported_sample_uids),
            )
            logger.info(
                "Processed source archive: source=%s items_files=%s imported_samples=%s",
                plan.source_path,
                len(items_paths),
                len(imported_sample_uids),
            )
            return ProcessedFileResult(
                source_path=plan.source_path,
                source_name=plan.source_name,
                extracted_dir=extracted_dir,
                items_paths=items_paths,
                imported_sample_uids=imported_sample_uids,
                imported_count=len(imported_sample_uids),
            )
        except Exception as exc:
            finished_at = datetime.now()
            self._upsert_run_file(
                run_file_id=run_file_id,
                run_id=run_id,
                source_path=plan.source_path,
                source_name=plan.source_name,
                status="failed",
                error_message=str(exc),
                started_at=started_at,
                finished_at=finished_at,
            )
            self._upsert_source_file(
                plan=plan,
                status="failed",
                run_id=run_id,
                last_seen_at=finished_at,
                error_message=str(exc),
            )
            raise

    def _run_session_merge(self) -> None:
        logger.info("Running session merge for database: %s", self.config.paths.db_path)
        run_session_merge(
            self.config.paths.db_path,
            dry_run=False,
            batch_size=self.config.session_merge.batch_size,
            workers=self.config.session_merge.workers,
            min_prefix_turns=self.config.session_merge.min_prefix_turns,
        )

    def _run_round_feedback_for_samples(self, sample_uids: list[str]) -> None:
        candidate_rows = self._load_round_feedback_rows(sample_uids)
        if not candidate_rows:
            logger.info("No imported samples eligible for round feedback in current run")
            return

        runtime_config = Config(
            llm_endpoint=self.config.llm.endpoint,
            llm_api_key=self.config.llm.api_key,
            llm_model_id=self.config.llm.model_id,
            db_path=self.config.paths.db_path,
            batch_size=self.config.round_feedback.batch_size,
            max_retries=self.config.llm.max_retries,
            max_concurrency=self.config.round_feedback.workers,
            llm_timeout=self.config.llm.timeout,
            llm_retry_base_delay=self.config.llm.retry_base_delay,
            llm_retry_max_delay=self.config.llm.retry_max_delay,
        )

        async def _run() -> None:
            llm = AsyncLLMClient(
                endpoint=runtime_config.llm_endpoint,
                api_key=runtime_config.llm_api_key,
                model=runtime_config.llm_model_id,
                timeout=runtime_config.llm_timeout,
            )
            processor = RoundFeedbackProcessor(
                self.store,
                llm,
                runtime_config.max_concurrency,
                llm_max_retries=runtime_config.max_retries,
                llm_retry_base_delay=runtime_config.llm_retry_base_delay,
                llm_retry_max_delay=runtime_config.llm_retry_max_delay,
            )
            try:
                batch_size = max(1, self.config.round_feedback.batch_size)
                for start in range(0, len(candidate_rows), batch_size):
                    batch = candidate_rows[start:start + batch_size]
                    success, failures = await processor.process_batch(batch)
                    logger.info(
                        "Round feedback batch finished for current run: success=%s failures=%s batch_size=%s",
                        success,
                        failures,
                        len(batch),
                    )
            finally:
                await llm.close()

        logger.info("Running round feedback for imported samples: count=%s", len(candidate_rows))
        asyncio.run(_run())

    def _export_file_result(self, run_id: str, result: ProcessedFileResult) -> dict[str, Any]:
        if not result.imported_sample_uids:
            return {"qualified_samples": 0}

        filter_spec = self._build_export_filter_spec(result.imported_sample_uids)
        qualified_rows = self.exporter.preview(filter_spec)
        if qualified_rows["count"] == 0:
            logger.info("No qualified samples for source file: %s", result.source_name)
            return {"qualified_samples": 0}

        stem = self._safe_file_stem(result.source_name)
        export_path = self.config.paths.export_dir / f"{stem}.{run_id}.openai_round_feedback.jsonl"
        export_report_path = self.config.paths.export_dir / f"{stem}.{run_id}.openai_round_feedback.report.json"
        unisound_path = self.config.paths.export_dir / f"{stem}.{run_id}.unisound.jsonl"
        unisound_report_path = self.config.paths.export_dir / f"{stem}.{run_id}.unisound.report.json"

        count = self.exporter.export(
            ExportRequest(
                output_path=export_path,
                export_format="openai_round_feedback",
                filter_spec=filter_spec,
                allowed_output_dirs=[self.config.paths.export_dir],
            )
        )
        export_report_path.write_text(
            json.dumps(
                {
                    "run_id": run_id,
                    "source_path": str(result.source_path),
                    "qualified_samples": count,
                    "export_path": str(export_path),
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        unisound_config = load_unisound_config(self.config.export.unisound_config_path)
        unisound_summary = convert_unisound_file(export_path, unisound_path, unisound_config)
        build_unisound_report(unisound_summary, unisound_report_path)

        run_file_id = f"{run_id}:{hashlib.sha1(str(result.source_path).encode('utf-8')).hexdigest()[:16]}"
        self._upsert_run_file(
            run_file_id=run_file_id,
            run_id=run_id,
            source_path=result.source_path,
            source_name=result.source_name,
            status="completed",
            extracted_dir=result.extracted_dir,
            items_paths=result.items_paths,
            imported_samples=result.imported_count,
            qualified_samples=count,
            export_path=export_path,
            export_report_path=export_report_path,
            unisound_path=unisound_path,
            unisound_report_path=unisound_report_path,
        )
        logger.info(
            "Exported incremental dataset: source=%s qualified_samples=%s export=%s unisound=%s",
            result.source_name,
            count,
            export_path,
            unisound_path,
        )
        return {"qualified_samples": count}

    def _build_export_filter_spec(self, sample_uids: list[str]) -> ExportFilterSpec:
        filter_spec = ExportFilterSpec(
            empty_response=self.config.export.empty_response,
            session_merge_keep=self.config.export.session_merge_keep,
            session_merge_status=self.config.export.session_merge_status,
            has_error=self.config.export.has_error,
            selected_sample_uids=sample_uids,
        )
        self._apply_rate_expression(self.config.export.response_progress_rate, "progress", filter_spec)
        self._apply_rate_expression(self.config.export.user_satisfied_rate, "satisfied", filter_spec)
        self._apply_rate_expression(self.config.export.user_negative_feedback_rate, "negative_feedback", filter_spec)
        return filter_spec

    def _apply_rate_expression(self, expression: str | None, prefix: str, filter_spec: ExportFilterSpec) -> None:
        if not expression:
            return
        match = RATE_PATTERN.match(expression.strip())
        if not match:
            raise ValueError(f"Invalid rate expression: {expression}")
        op, value = match.groups()
        setattr(filter_spec, f"{prefix}_op", op)
        setattr(filter_spec, f"{prefix}_val", float(value))

    def _load_round_feedback_rows(self, sample_uids: list[str]) -> list[tuple[str, dict[str, Any]]]:
        eligible_uids = []
        for chunk in self._chunked(sample_uids, 500):
            placeholders = ", ".join(["?"] * len(chunk))
            if self.config.session_merge.enabled:
                query = (
                    f"SELECT sample_uid, raw_json FROM samples WHERE sample_uid IN ({placeholders}) "
                    "AND COALESCE(session_merge_keep, TRUE) = TRUE ORDER BY id"
                )
            else:
                query = f"SELECT sample_uid, raw_json FROM samples WHERE sample_uid IN ({placeholders}) ORDER BY id"
            rows = self.store.conn.execute(query, chunk).fetchall()
            eligible_uids.extend((row[0], json.loads(row[1]) if isinstance(row[1], str) else row[1]) for row in rows)
        return eligible_uids

    def _extract_items_jsonl(self, archive_path: Path, extracted_dir: Path) -> list[Path]:
        pending_archives = [archive_path]
        extracted_archives: set[str] = set()
        items_paths: list[Path] = []

        while pending_archives:
            current = pending_archives.pop(0)
            current_key = str(current.resolve()) if current.exists() else str(current)
            if current_key in extracted_archives:
                continue
            extracted_archives.add(current_key)

            target_dir = extracted_dir / f"extract_{len(extracted_archives):03d}_{self._safe_file_stem(current.name)}"
            target_dir.mkdir(parents=True, exist_ok=True)
            self._extract_archive(current, target_dir)

            for path in sorted(target_dir.rglob("*")):
                if not path.is_file():
                    continue
                if path.name == "items.jsonl":
                    items_paths.append(path)
                elif path.name == "items.jsonl.gz":
                    decompressed = path.with_suffix("")
                    self._gunzip_file(path, decompressed)
                    items_paths.append(decompressed)
                elif self._is_supported_archive(path):
                    pending_archives.append(path)

        return self._dedupe_keep_order(items_paths)

    def _extract_archive(self, archive_path: Path, target_dir: Path) -> None:
        if tarfile.is_tarfile(archive_path):
            with tarfile.open(archive_path, "r:*") as tar:
                self._safe_extract_tar(tar, target_dir)
            return
        if archive_path.suffix == ".gz":
            output_path = target_dir / archive_path.stem
            self._gunzip_file(archive_path, output_path)
            return
        raise ValueError(f"Unsupported archive format: {archive_path}")

    def _safe_extract_tar(self, tar: tarfile.TarFile, target_dir: Path) -> None:
        target_root = target_dir.resolve()
        for member in tar.getmembers():
            member_path = (target_dir / member.name).resolve()
            if os.path.commonpath([str(target_root), str(member_path)]) != str(target_root):
                raise ValueError(f"Unsafe archive member path detected: {member.name}")
        if hasattr(tarfile, "data_filter"):
            tar.extractall(target_dir, filter="data")
        else:  # pragma: no cover - Python < 3.12
            tar.extractall(target_dir)

    def _gunzip_file(self, source_path: Path, target_path: Path) -> None:
        with gzip.open(source_path, "rb") as source, target_path.open("wb") as target:
            shutil.copyfileobj(source, target)

    def _collect_sample_uids(self, items_path: Path) -> list[str]:
        sample_uids: list[str] = []
        with items_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if not stripped:
                    continue
                payload = json.loads(stripped)
                sample_uids.append(Sample.from_dict(payload).sample_uid)
        return self._dedupe_keep_order(sample_uids)

    def _new_sample_uids(self, sample_uids: list[str]) -> list[str]:
        if not sample_uids:
            return []
        existing: set[str] = set()
        for chunk in self._chunked(sample_uids, 500):
            placeholders = ", ".join(["?"] * len(chunk))
            rows = self.store.conn.execute(
                f"SELECT sample_uid FROM samples WHERE sample_uid IN ({placeholders})",
                chunk,
            ).fetchall()
            existing.update(row[0] for row in rows)
        return [sample_uid for sample_uid in sample_uids if sample_uid not in existing]

    def _insert_pipeline_run(self, run_id: str, log_path: Path) -> None:
        self.store.conn.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, status, source_dir, started_at, log_path,
                processed_files, imported_samples, exported_samples, exported_files, unisound_files
            ) VALUES (?, ?, ?, ?, ?, 0, 0, 0, 0, 0)
            """,
            [run_id, "running", str(self.config.paths.source_dir), datetime.now(), str(log_path)],
        )

    def _finish_pipeline_run(
        self,
        run_id: str,
        status: str,
        summary: dict[str, Any],
        error_message: str | None = None,
    ) -> None:
        self.store.conn.execute(
            """
            UPDATE pipeline_runs
            SET status = ?,
                finished_at = ?,
                processed_files = ?,
                imported_samples = ?,
                exported_samples = ?,
                exported_files = ?,
                unisound_files = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            [
                status,
                datetime.now(),
                int(summary.get("processed_files", 0)),
                int(summary.get("imported_samples", 0)),
                int(summary.get("exported_samples", 0)),
                int(summary.get("exported_files", 0)),
                int(summary.get("unisound_files", 0)),
                error_message,
                run_id,
            ],
        )

    def _upsert_source_file(
        self,
        plan: SourceFilePlan,
        status: str,
        run_id: str,
        last_seen_at: datetime,
        processed_at: datetime | None = None,
        extracted_dir: Path | None = None,
        items_paths: list[Path] | None = None,
        imported_samples: int | None = None,
        error_message: str | None = None,
    ) -> None:
        self.store.conn.execute("DELETE FROM pipeline_source_files WHERE source_path = ?", [str(plan.source_path)])
        self.store.conn.execute(
            """
            INSERT INTO pipeline_source_files (
                source_path, source_name, size_bytes, mtime_ns, fingerprint, status,
                last_seen_at, last_run_id, processed_at, extracted_dir, items_paths,
                imported_samples, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                str(plan.source_path),
                plan.source_name,
                plan.size_bytes,
                plan.mtime_ns,
                plan.fingerprint,
                status,
                last_seen_at,
                run_id,
                processed_at,
                None if extracted_dir is None else str(extracted_dir),
                None if items_paths is None else json.dumps([str(path) for path in items_paths], ensure_ascii=False),
                imported_samples,
                error_message,
            ],
        )

    def _upsert_run_file(
        self,
        run_file_id: str,
        run_id: str,
        source_path: Path,
        source_name: str,
        status: str,
        extracted_dir: Path | None = None,
        items_paths: list[Path] | None = None,
        imported_samples: int | None = None,
        qualified_samples: int | None = None,
        export_path: Path | None = None,
        export_report_path: Path | None = None,
        unisound_path: Path | None = None,
        unisound_report_path: Path | None = None,
        error_message: str | None = None,
        started_at: datetime | None = None,
        finished_at: datetime | None = None,
    ) -> None:
        existing_started_at = self.store.conn.execute(
            "SELECT started_at FROM pipeline_run_files WHERE run_file_id = ?",
            [run_file_id],
        ).fetchone()
        self.store.conn.execute("DELETE FROM pipeline_run_files WHERE run_file_id = ?", [run_file_id])
        self.store.conn.execute(
            """
            INSERT INTO pipeline_run_files (
                run_file_id, run_id, source_path, source_name, status, extracted_dir, items_paths,
                imported_samples, qualified_samples, export_path, export_report_path,
                unisound_path, unisound_report_path, error_message, started_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_file_id,
                run_id,
                str(source_path),
                source_name,
                status,
                None if extracted_dir is None else str(extracted_dir),
                None if items_paths is None else json.dumps([str(path) for path in items_paths], ensure_ascii=False),
                imported_samples,
                qualified_samples,
                None if export_path is None else str(export_path),
                None if export_report_path is None else str(export_report_path),
                None if unisound_path is None else str(unisound_path),
                None if unisound_report_path is None else str(unisound_report_path),
                error_message,
                started_at or (existing_started_at[0] if existing_started_at else datetime.now()),
                finished_at,
            ],
        )

    def _fingerprint_file(self, path: Path, size_bytes: int, mtime_ns: int) -> str:
        raw = f"{path.resolve()}::{size_bytes}::{mtime_ns}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _is_supported_archive(self, path: Path) -> bool:
        name = path.name.lower()
        return any(name.endswith(suffix) for suffix in ARCHIVE_SUFFIXES)

    def _safe_file_stem(self, name: str) -> str:
        stem = name
        for suffix in sorted(ARCHIVE_SUFFIXES, key=len, reverse=True):
            if stem.lower().endswith(suffix):
                stem = stem[: -len(suffix)]
                break
        stem = stem.replace(".", "_")
        stem = re.sub(r"[^A-Za-z0-9_-]+", "_", stem)
        stem = stem.strip("_")
        return stem or "archive"

    def _chunked(self, values: list[str], chunk_size: int) -> Iterable[list[str]]:
        for start in range(0, len(values), chunk_size):
            yield values[start:start + chunk_size]

    def _dedupe_keep_order(self, values: Iterable[Any]) -> list[Any]:
        seen: set[Any] = set()
        ordered: list[Any] = []
        for value in values:
            if value in seen:
                continue
            seen.add(value)
            ordered.append(value)
        return ordered