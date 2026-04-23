"""JSONL file importer."""
import json
import logging
import multiprocessing
import os
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterator, Sequence

from claw_data_filter.models.sample import extract_import_fields_from_payload
from claw_data_filter.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)

# Allowed directories for I/O
ALLOWED_IO_DIRS = ["data", "."]
DEFAULT_IMPORT_WORKERS = 1
DEFAULT_IMPORT_CHUNK_SIZE = 64
DEFAULT_IMPORT_RECONNECT_EVERY_CHUNKS = 0
IMPORT_PROGRESS_LOG_INTERVAL = 10
IMPORT_FORCE_SERIAL_ENV = "CLAW_IMPORT_FORCE_SERIAL"


@dataclass(slots=True)
class ImportSummary:
    imported_count: int
    imported_sample_uids: list[str]
    error_count: int
    processed_lines: int


def _build_insert_row_from_payload(data: dict) -> tuple:
    sample = extract_import_fields_from_payload(data)
    return (
        sample["sample_uid"],
        json.dumps(sample["raw_json"], ensure_ascii=False),
        sample["user_query"],
        sample["assistant_response"],
        sample["empty_response"],
        sample["num_turns"],
        sample["expected_judgment_count"],
        sample["expected_response_judgment_count"],
        sample["expected_episode_judgment_count"],
        sample["num_tool_calls"],
        datetime.now(),
        "pending",
        datetime.now(),
    )


def _parse_jsonl_chunk(lines: Sequence[str], skip_errors: bool = True) -> tuple[list[tuple], int, int]:
    rows: list[tuple] = []
    errors = 0
    non_empty_lines = 0

    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        non_empty_lines += 1
        try:
            data = json.loads(stripped)
            rows.append(_build_insert_row_from_payload(data))
        except json.JSONDecodeError as exc:
            errors += 1
            logger.error(f"Chunk JSON decode error: {exc}")
            if not skip_errors:
                raise
        except Exception as exc:
            errors += 1
            logger.error(f"Chunk processing error: {exc}")
            if not skip_errors:
                raise

    return rows, errors, non_empty_lines


def _iter_line_chunks(lines: Iterator[str], chunk_size: int) -> Iterator[list[str]]:
    chunk: list[str] = []
    for line in lines:
        chunk.append(line)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def _validate_input_path(path: Path) -> None:
    """Validate input path is within allowed directories.

    Args:
        path: Path to validate

    Raises:
        ValueError: If path is outside allowed directories
    """
    path = path.resolve()
    allowed = [Path.cwd() / d for d in ALLOWED_IO_DIRS]
    for allowed_dir in allowed:
        try:
            path.relative_to(allowed_dir.resolve())
            return
        except ValueError:
            continue
    raise ValueError(f"Input path must be within allowed directories: {ALLOWED_IO_DIRS}")


class JSONLImporter:
    """Import JSONL files into DuckDB storage."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.store = DuckDBStore(self.db_path)

    def import_file(
        self,
        input_path: Path,
        skip_errors: bool = True,
        workers: int = DEFAULT_IMPORT_WORKERS,
        chunk_size: int = DEFAULT_IMPORT_CHUNK_SIZE,
        max_pending_chunks: int | None = None,
        reconnect_every_chunks: int = DEFAULT_IMPORT_RECONNECT_EVERY_CHUNKS,
    ) -> int:
        """Import JSONL file, return count of imported samples.

        Args:
            input_path: Path to JSONL file
            skip_errors: If True, skip malformed lines; if False, raise on error

        Returns:
            Number of successfully imported samples
        """
        _validate_input_path(input_path)
        try:
            input_size = input_path.stat().st_size
        except OSError:
            input_size = None
        logger.info(
            "Starting JSONL import: path=%s workers=%s chunk_size=%s size_bytes=%s",
            input_path,
            workers,
            chunk_size,
            input_size if input_size is not None else "unknown",
        )
        with open(input_path, "r", encoding="utf-8") as f:
            return self.import_lines(
                f,
                skip_errors=skip_errors,
                workers=workers,
                chunk_size=chunk_size,
                max_pending_chunks=max_pending_chunks,
                reconnect_every_chunks=reconnect_every_chunks,
            )

    def import_lines(
        self,
        lines: Iterator[str],
        skip_errors: bool = True,
        workers: int = DEFAULT_IMPORT_WORKERS,
        chunk_size: int = DEFAULT_IMPORT_CHUNK_SIZE,
        max_pending_chunks: int | None = None,
        reconnect_every_chunks: int = DEFAULT_IMPORT_RECONNECT_EVERY_CHUNKS,
    ) -> int:
        """Import from iterator of lines (for streaming).

        Args:
            lines: Iterator of JSONL lines
            skip_errors: If True, skip malformed lines; if False, raise on error

        Returns:
            Number of successfully imported samples
        """
        summary = self.import_lines_with_summary(
            lines,
            skip_errors=skip_errors,
            workers=workers,
            chunk_size=chunk_size,
            max_pending_chunks=max_pending_chunks,
            reconnect_every_chunks=reconnect_every_chunks,
        )
        return summary.imported_count

    def import_lines_with_summary(
        self,
        lines: Iterator[str],
        skip_errors: bool = True,
        workers: int = DEFAULT_IMPORT_WORKERS,
        chunk_size: int = DEFAULT_IMPORT_CHUNK_SIZE,
        max_pending_chunks: int | None = None,
        reconnect_every_chunks: int = DEFAULT_IMPORT_RECONNECT_EVERY_CHUNKS,
    ) -> ImportSummary:
        """Import from iterator of lines and return detailed import results."""
        normalized_workers = max(1, int(workers or 1))
        normalized_chunk_size = max(1, int(chunk_size or DEFAULT_IMPORT_CHUNK_SIZE))
        normalized_max_pending = max(1, int(max_pending_chunks or normalized_workers))
        normalized_reconnect_every = max(0, int(reconnect_every_chunks or DEFAULT_IMPORT_RECONNECT_EVERY_CHUNKS))
        force_serial = os.getenv(IMPORT_FORCE_SERIAL_ENV, "").strip().lower() in {"1", "true", "yes", "on"}

        if force_serial and normalized_workers > 1:
            logger.warning(
                "Import workers forced to serial mode by %s: requested_workers=%s effective_workers=1",
                IMPORT_FORCE_SERIAL_ENV,
                normalized_workers,
            )
            normalized_workers = 1

        logger.info(
            "Import configuration resolved: workers=%s chunk_size=%s max_pending=%s reconnect_every_chunks=%s mode=%s",
            normalized_workers,
            normalized_chunk_size,
            normalized_max_pending,
            normalized_reconnect_every,
            "parallel" if normalized_workers > 1 else "serial",
        )

        if normalized_workers == 1:
            count, imported_sample_uids, errors, processed_lines = self._import_serial_batched(
                lines,
                skip_errors,
                normalized_chunk_size,
                normalized_reconnect_every,
            )
        else:
            count, imported_sample_uids, errors, processed_lines = self._import_parallel_batched(
                lines,
                skip_errors,
                normalized_workers,
                normalized_chunk_size,
                normalized_max_pending,
                normalized_reconnect_every,
            )

        logger.info(f"Imported {count} samples, {errors} errors")
        return ImportSummary(
            imported_count=count,
            imported_sample_uids=imported_sample_uids,
            error_count=errors,
            processed_lines=processed_lines,
        )

    def _import_serial_batched(
        self,
        lines: Iterator[str],
        skip_errors: bool,
        chunk_size: int,
        reconnect_every_chunks: int,
    ) -> tuple[int, list[str], int, int]:
        count = 0
        errors = 0
        processed_lines = 0
        imported_sample_uids: list[str] = []
        for chunk_index, chunk in enumerate(_iter_line_chunks(lines, chunk_size), start=1):
            rows, chunk_errors, line_count = _parse_jsonl_chunk(chunk, skip_errors=skip_errors)
            errors += chunk_errors
            processed_lines += line_count
            chunk_count, chunk_sample_uids = self.store.insert_sample_batch_detailed(rows)
            count += chunk_count
            imported_sample_uids.extend(chunk_sample_uids)
            self._log_progress(chunk_index, processed_lines, count, errors)
            self._maybe_reconnect_store(chunk_index, reconnect_every_chunks)
        return count, imported_sample_uids, errors, processed_lines

    def _import_parallel_batched(
        self,
        lines: Iterator[str],
        skip_errors: bool,
        workers: int,
        chunk_size: int,
        max_pending_chunks: int,
        reconnect_every_chunks: int,
    ) -> tuple[int, list[str], int, int]:
        count = 0
        errors = 0
        processed_lines = 0
        chunk_index = 0
        pending = deque()
        imported_sample_uids: list[str] = []

        with ProcessPoolExecutor(
            max_workers=workers,
            mp_context=multiprocessing.get_context("spawn"),
        ) as executor:
            for chunk in _iter_line_chunks(lines, chunk_size):
                pending.append(executor.submit(_parse_jsonl_chunk, chunk, skip_errors))
                if len(pending) >= max_pending_chunks:
                    rows, chunk_errors, line_count = pending.popleft().result()
                    errors += chunk_errors
                    processed_lines += line_count
                    chunk_count, chunk_sample_uids = self.store.insert_sample_batch_detailed(rows)
                    count += chunk_count
                    imported_sample_uids.extend(chunk_sample_uids)
                    chunk_index += 1
                    self._log_progress(chunk_index, processed_lines, count, errors)
                    self._maybe_reconnect_store(chunk_index, reconnect_every_chunks)

            while pending:
                rows, chunk_errors, line_count = pending.popleft().result()
                errors += chunk_errors
                processed_lines += line_count
                chunk_count, chunk_sample_uids = self.store.insert_sample_batch_detailed(rows)
                count += chunk_count
                imported_sample_uids.extend(chunk_sample_uids)
                chunk_index += 1
                self._log_progress(chunk_index, processed_lines, count, errors)
                self._maybe_reconnect_store(chunk_index, reconnect_every_chunks)

            return count, imported_sample_uids, errors, processed_lines

    def _maybe_reconnect_store(self, chunk_index: int, reconnect_every_chunks: int) -> None:
        if reconnect_every_chunks <= 0 or chunk_index % reconnect_every_chunks != 0:
            return
        logger.info(
            "Reconnecting DuckDB store after import chunk boundary: chunk_index=%s reconnect_every_chunks=%s",
            chunk_index,
            reconnect_every_chunks,
        )
        self._reconnect_store()

    def _reconnect_store(self) -> None:
        self.store.close()
        self.store = DuckDBStore(self.db_path)

    def _log_progress(self, chunk_index: int, processed_lines: int, imported_count: int, errors: int) -> None:
        if chunk_index == 1 or chunk_index % IMPORT_PROGRESS_LOG_INTERVAL == 0:
            logger.info(
                "Import progress: chunks=%s processed_lines=%s imported=%s errors=%s",
                chunk_index,
                processed_lines,
                imported_count,
                errors,
            )

    def close(self):
        """Close underlying store."""
        self.store.close()