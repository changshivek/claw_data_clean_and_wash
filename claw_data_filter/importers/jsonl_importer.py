"""JSONL file importer."""
import json
import logging
from pathlib import Path
from typing import Iterator

from claw_data_filter.models.sample import Sample
from claw_data_filter.storage.duckdb_store import DuckDBStore

logger = logging.getLogger(__name__)


class JSONLImporter:
    """Import JSONL files into DuckDB storage."""

    def __init__(self, db_path: Path):
        self.store = DuckDBStore(db_path)

    def import_file(self, input_path: Path, skip_errors: bool = True) -> int:
        """Import JSONL file, return count of imported samples.

        Args:
            input_path: Path to JSONL file
            skip_errors: If True, skip malformed lines; if False, raise on error

        Returns:
            Number of successfully imported samples
        """
        count = 0
        errors = 0

        with open(input_path, "r", encoding="utf-8") as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    data = json.loads(line)
                    sample = Sample.from_dict(data)
                    self.store.insert_sample(sample)
                    count += 1
                except json.JSONDecodeError as e:
                    errors += 1
                    logger.error(f"Line {line_num}: JSON decode error: {e}")
                    if not skip_errors:
                        raise
                except Exception as e:
                    errors += 1
                    logger.error(f"Line {line_num}: Error processing line: {e}")
                    if not skip_errors:
                        raise

        logger.info(f"Imported {count} samples, {errors} errors")
        return count

    def import_lines(self, lines: Iterator[str], skip_errors: bool = True) -> int:
        """Import from iterator of lines (for streaming).

        Args:
            lines: Iterator of JSONL lines
            skip_errors: If True, skip malformed lines; if False, raise on error

        Returns:
            Number of successfully imported samples
        """
        count = 0
        errors = 0

        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:
                continue

            try:
                data = json.loads(line)
                sample = Sample.from_dict(data)
                self.store.insert_sample(sample)
                count += 1
            except Exception as e:
                errors += 1
                logger.error(f"Line {line_num}: Error: {e}")
                if not skip_errors:
                    raise

        logger.info(f"Imported {count} samples, {errors} errors")
        return count

    def close(self):
        """Close underlying store."""
        self.store.close()