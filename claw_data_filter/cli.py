"""Click CLI for agent data filter tool."""
import asyncio
import logging
import os
import re
import sys
from pathlib import Path

import click

from claw_data_filter.config import Config
from claw_data_filter.exporters.report_exporter import ReportExporter
from claw_data_filter.exporters.unified_exporter import (
    OPENAI_ROUND_FEEDBACK,
    RAW_JSONL,
    ExportFilterSpec,
    ExportRequest,
    UnifiedExporter,
)
from claw_data_filter.importers.jsonl_importer import JSONLImporter
from claw_data_filter.storage.duckdb_store import DuckDBStore


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    # Keep third-party HTTP client noise out of long-running rebuild logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


_configure_logging()
logger = logging.getLogger(__name__)

def _shared_cpu_budget(max_cap: int | None = None) -> int:
    cpu_count = max(1, os.cpu_count() or 1)
    budget = max(1, int(cpu_count * 0.7))
    if max_cap is not None:
        budget = min(budget, max_cap)
    return budget


DEFAULT_IMPORT_WORKERS = _shared_cpu_budget(max_cap=8)
DEFAULT_IMPORT_CHUNK_SIZE = 64
DEFAULT_SESSION_MERGE_WORKERS = _shared_cpu_budget(max_cap=16)


@click.group()
@click.option("--db-path", type=click.Path(), default="./data.duckdb", help="DuckDB database path")
@click.option("--llm-endpoint", type=str, default=None, help="LLM API endpoint")
@click.option("--llm-model-id", type=str, default=None, help="LLM model id")
@click.pass_context
def cli(ctx, db_path, llm_endpoint, llm_model_id):
    """Agent Data Filter - LLM-powered agent conversation filtering."""
    config = Config.from_env()
    if db_path:
        config.db_path = Path(db_path)
    if llm_endpoint:
        config.llm_endpoint = llm_endpoint
    if llm_model_id:
        config.llm_model_id = llm_model_id
    ctx.obj["config"] = config


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.option("--workers", type=int, default=DEFAULT_IMPORT_WORKERS, show_default=True, help="Number of parallel parser workers for import")
@click.option("--chunk-size", type=int, default=DEFAULT_IMPORT_CHUNK_SIZE, show_default=True, help="Number of JSONL rows parsed and inserted per batch")
@click.pass_context
def import_cmd(ctx, input_file, workers, chunk_size):
    """Import JSONL data file into database."""
    config = ctx.obj["config"]
    click.echo(f"Importing {input_file}...")
    logger.info(
        "CLI import command starting: db_path=%s input_file=%s workers=%s chunk_size=%s",
        config.db_path,
        input_file,
        workers,
        chunk_size,
    )

    importer = JSONLImporter(config.db_path)
    try:
        count = importer.import_file(Path(input_file), workers=workers, chunk_size=chunk_size)
        click.echo(f"Successfully imported {count} samples.")
    finally:
        importer.close()


@cli.command()
@click.option("--response-progress-rate", type=str, help="Filter by response progress rate (e.g., '>=0.7')")
@click.option("--user-satisfied-rate", type=str, help="Filter by user satisfied rate (e.g., '>=0.7')")
@click.option("--user-negative-feedback-rate", type=str, help="Filter by user negative feedback rate (e.g., '>=0.3')")
@click.option("--empty-response", type=bool, help="Filter by empty response marker (true/false)")
@click.option("--session-merge-keep", type=bool, help="Filter by session merge keep marker (true/false)")
@click.option("--session-merge-status", type=click.Choice(["keep", "merged", "skipped", "unmarked"]), help="Filter by session merge status")
@click.option("--has-error", type=bool, help="Filter by has error (true/false)")
@click.option(
    "--export-format",
    type=click.Choice([RAW_JSONL, OPENAI_ROUND_FEEDBACK]),
    default=RAW_JSONL,
    show_default=True,
    help="Export format",
)
@click.option("--export", type=click.Path(), required=True, help="Output JSONL file")
@click.option("--report", type=click.Path(), help="Output report JSON file")
@click.option("--limit", type=int, help="Limit number of results")
@click.pass_context
def filter_cmd(ctx, response_progress_rate, user_satisfied_rate, user_negative_feedback_rate, empty_response, session_merge_keep, session_merge_status, has_error, export_format, export, report, limit):
    """Filter samples and export to JSONL with optional report."""
    config = ctx.obj["config"]

    RATE_PATTERN = re.compile(r"^(>=|<=|>|<|!=|=)\s*([\d.]+)$")

    filter_spec = ExportFilterSpec(
        empty_response=empty_response,
        session_merge_keep=session_merge_keep,
        session_merge_status=session_merge_status,
        has_error=has_error,
    )
    if response_progress_rate:
        match = RATE_PATTERN.match(response_progress_rate.strip())
        if match:
            filter_spec.progress_op, value_str = match.groups()
            filter_spec.progress_val = float(value_str)
        else:
            raise ValueError(f"Invalid response-progress-rate expression: {response_progress_rate}")
    if user_satisfied_rate:
        match = RATE_PATTERN.match(user_satisfied_rate.strip())
        if match:
            filter_spec.satisfied_op, value_str = match.groups()
            filter_spec.satisfied_val = float(value_str)
        else:
            raise ValueError(f"Invalid user-satisfied-rate expression: {user_satisfied_rate}")
    if user_negative_feedback_rate:
        match = RATE_PATTERN.match(user_negative_feedback_rate.strip())
        if match:
            filter_spec.negative_feedback_op, value_str = match.groups()
            filter_spec.negative_feedback_val = float(value_str)
        else:
            raise ValueError(f"Invalid user-negative-feedback-rate expression: {user_negative_feedback_rate}")

    store = DuckDBStore(config.db_path)
    try:
        exporter = UnifiedExporter(store)
        count = exporter.export(
            ExportRequest(
                output_path=Path(export),
                export_format=export_format,
                filter_spec=filter_spec,
                limit=limit,
            )
        )
        click.echo(f"Exported {count} samples to {export}")

        if report:
            report_exporter = ReportExporter(store)
            report_exporter.export_report(Path(report))
            click.echo(f"Report saved to {report}")
    finally:
        store.close()


@cli.command()
@click.pass_context
def stats(ctx):
    """Show statistics about imported data and dual-level round judgments."""
    config = ctx.obj["config"]
    store = DuckDBStore(config.db_path)
    logger.info("CLI stats command starting: db_path=%s", config.db_path)

    try:
        stats_data = store.get_stats()
        click.echo("=== Statistics ===")
        click.echo(f"Total samples: {stats_data['total_samples']}")
        if stats_data['total_samples'] > 0:
            click.echo(f"Avg response progress rate (assistant steps): {stats_data['avg_response_progress_rate']:.2f}")
            click.echo(f"Avg response regress rate (assistant steps): {stats_data['avg_response_regress_rate']:.2f}")
            click.echo(f"Avg user satisfied rate (user episodes): {stats_data['avg_user_satisfied_rate']:.2f}")
            click.echo(f"Avg user negative feedback rate (user episodes): {stats_data['avg_user_negative_feedback_rate']:.2f}")
            click.echo(f"Error count: {stats_data['error_count']}")
        logger.info("CLI stats command complete")
    finally:
        store.close()


@cli.command()
@click.pass_context
def info(ctx):
    """Show database information."""
    config = ctx.obj["config"]
    store = DuckDBStore(config.db_path)

    try:
        click.echo(f"Database path: {config.db_path}")
        click.echo(f"Sample count: {store.get_sample_count()}")
    finally:
        store.close()


@cli.command()
@click.pass_context
def pressure_test(ctx):
    """Run pressure test before starting round feedback processing."""
    config = ctx.obj["config"]
    click.echo(f"Running pressure test with concurrency={config.max_concurrency}...")
    logger.info(
        "CLI pressure test command starting: db_path=%s concurrency=%s endpoint=%s model=%s",
        config.db_path,
        config.max_concurrency,
        config.llm_endpoint,
        config.llm_model_id,
    )

    from claw_data_filter.llm.async_client import AsyncLLMClient
    from claw_data_filter.processors.round_feedback import PressureTest

    async def _run_pressure_test():
        llm = AsyncLLMClient(
            endpoint=config.llm_endpoint,
            api_key=config.llm_api_key,
            model=config.llm_model_id,
            timeout=config.llm_timeout,
        )
        try:
            tester = PressureTest(llm)
            return await tester.run(config.max_concurrency)
        finally:
            await llm.close()

    passed = asyncio.run(_run_pressure_test())
    if passed:
        click.echo("Pressure test PASSED")
        logger.info("CLI pressure test command passed")
    else:
        click.echo("Pressure test FAILED")
        logger.error("CLI pressure test command failed")
        sys.exit(1)


@cli.command()
@click.option("--workers", type=int, default=None, help="Number of parallel workers")
@click.option("--batch-size", type=int, default=None, help="Batch size per worker")
@click.pass_context
def round_feedback(ctx, workers, batch_size):
    """Process round-level feedback judgments on samples."""
    config = ctx.obj["config"]
    if workers:
        config.max_concurrency = workers
    if batch_size is not None:
        config.batch_size = batch_size

    click.echo(f"Starting round feedback processing with concurrency={config.max_concurrency}...")
    logger.info(
        "CLI round feedback command starting: db_path=%s concurrency=%s batch_size=%s endpoint=%s model=%s",
        config.db_path,
        config.max_concurrency,
        config.batch_size,
        config.llm_endpoint,
        config.llm_model_id,
    )

    async def _run_round_feedback():
        from claw_data_filter.llm.async_client import AsyncLLMClient
        from claw_data_filter.processors.round_feedback import RoundFeedbackProcessor
        from claw_data_filter.storage.duckdb_store import DuckDBStore

        llm = AsyncLLMClient(
            endpoint=config.llm_endpoint,
            api_key=config.llm_api_key,
            model=config.llm_model_id,
            timeout=config.llm_timeout,
        )

        store = DuckDBStore(config.db_path)
        processor = RoundFeedbackProcessor(
            store,
            llm,
            config.max_concurrency,
            llm_max_retries=config.max_retries,
            llm_retry_base_delay=config.llm_retry_base_delay,
            llm_retry_max_delay=config.llm_retry_max_delay,
        )

        try:
            total_success = 0
            total_failures = 0

            while True:
                batch = store.claim_unprocessed_samples(limit=config.batch_size)
                if not batch:
                    break

                logger.info(
                    "Round feedback claimed batch: claimed_samples=%s total_success=%s total_failures=%s",
                    len(batch),
                    total_success,
                    total_failures,
                )
                success, failures = await processor.process_batch(batch)
                total_success += success
                total_failures += failures
                click.echo(f"Processed batch: {success} success, {failures} failures")

            click.echo(f"Round feedback processing complete: {total_success} success, {total_failures} failures")
        finally:
            await llm.close()
            store.close()

    asyncio.run(_run_round_feedback())


@cli.command(name="session-merge")
@click.option("--workers", type=int, default=DEFAULT_SESSION_MERGE_WORKERS, help="Number of parallel workers")
@click.option("--batch-size", type=int, default=512, help="Batch size per worker")
@click.option("--min-prefix-turns", type=int, default=2, help="Minimum shared user turns before collapsing a prefix")
@click.option("--dry-run", is_flag=True, help="Only print the summary without writing markers")
@click.pass_context
def session_merge_cmd(ctx, workers, batch_size, min_prefix_turns, dry_run):
    """Run content-driven session snapshot merge before round feedback."""
    from claw_data_filter.session_merge import run_session_merge

    config = ctx.obj["config"]
    logger.info(
        "CLI session merge command starting: db_path=%s workers=%s batch_size=%s min_prefix_turns=%s dry_run=%s",
        config.db_path,
        workers,
        batch_size,
        min_prefix_turns,
        dry_run,
    )
    summary = run_session_merge(
        config.db_path,
        dry_run=dry_run,
        batch_size=batch_size,
        workers=workers,
        min_prefix_turns=min_prefix_turns,
    )
    click.echo("=== Session Merge Summary ===")
    for key in sorted(summary):
        click.echo(f"{key}: {summary[key]}")


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
