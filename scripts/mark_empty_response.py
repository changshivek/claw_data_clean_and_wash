"""One-off script to backfill empty_response markers for an existing DuckDB database."""

import argparse
from pathlib import Path

from claw_data_filter.empty_response import backfill_empty_response


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill empty_response markers in DuckDB")
    parser.add_argument("--db-path", required=True, help="DuckDB database path")
    parser.add_argument("--dry-run", action="store_true", help="Only print summary without writing updates")
    args = parser.parse_args()

    summary = backfill_empty_response(Path(args.db_path), dry_run=args.dry_run)
    for key in sorted(summary):
        print(f"{key}: {summary[key]}")


if __name__ == "__main__":
    main()