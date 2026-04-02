"""Click CLI for agent data filter tool."""
import asyncio
import logging
import re
import sys
from pathlib import Path

import click

from claw_data_filter.config import Config
from claw_data_filter.exporters.jsonl_exporter import JSONLExporter
from claw_data_filter.exporters.report_exporter import ReportExporter
from claw_data_filter.filters.query import FilterQueryBuilder
from claw_data_filter.importers.jsonl_importer import JSONLImporter
from claw_data_filter.storage.duckdb_store import DuckDBStore

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@click.group()
@click.option("--db-path", type=click.Path(), default="./data.duckdb", help="DuckDB database path")
@click.option("--llm-endpoint", type=str, default=None, help="LLM API endpoint")
@click.pass_context
def cli(ctx, db_path, llm_endpoint):
    """Agent Data Filter - LLM-powered agent conversation filtering."""
    config = Config.from_env()
    if db_path:
        config.db_path = Path(db_path)
    if llm_endpoint:
        config.llm_endpoint = llm_endpoint
    ctx.obj["config"] = config


@cli.command()
@click.argument("input_file", type=click.Path(exists=True))
@click.pass_context
def import_cmd(ctx, input_file):
    """Import JSONL data file into database."""
    config = ctx.obj["config"]
    click.echo(f"Importing {input_file}...")

    importer = JSONLImporter(config.db_path)
    try:
        count = importer.import_file(Path(input_file))
        click.echo(f"Successfully imported {count} samples.")
    finally:
        importer.close()


@cli.command()
@click.option("--progress-score", type=str, help="Filter by progress score (e.g., '>=4')")
@click.option("--overall-score", type=str, help="Filter by overall score (e.g., '>7')")
@click.option("--task-type", type=str, multiple=True, help="Filter by task type")
@click.option("--export", type=click.Path(), required=True, help="Output JSONL file")
@click.option("--report", type=click.Path(), help="Output report JSON file")
@click.option("--limit", type=int, help="Limit number of results")
@click.pass_context
def filter_cmd(ctx, progress_score, overall_score, task_type, export, report, limit):
    """Filter samples and export to JSONL with optional report."""
    config = ctx.obj["config"]

    OVERALL_SCORE_PATTERN = re.compile(r"^(>=|<=|>|<|!=|=)\s*([\d.]+)$")

    builder = FilterQueryBuilder()
    if progress_score:
        builder.add_progress_score_filter(progress_score)
    if overall_score:
        match = OVERALL_SCORE_PATTERN.match(overall_score.strip())
        if match:
            op_str, value_str = match.groups()
            from claw_data_filter.filters.query import ComparisonOp
            op = ComparisonOp(op_str)
            value = float(value_str) if "." in value_str else int(value_str)
            builder.add_condition("overall_score", op, value)
        else:
            raise ValueError(f"Invalid overall-score expression: {overall_score}")
    if task_type:
        builder.add_task_type_filter(list(task_type))

    where_clause = builder.build_where_clause()

    store = DuckDBStore(config.db_path)
    try:
        exporter = JSONLExporter(store)
        count = exporter.export(Path(export), filter_query=where_clause, limit=limit)
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
    """Show statistics about imported data and round judgments."""
    config = ctx.obj["config"]
    store = DuckDBStore(config.db_path)

    try:
        stats_data = store.get_stats()
        click.echo("=== Statistics ===")
        click.echo(f"Total samples: {stats_data['total_samples']}")
        if stats_data['total_samples'] > 0:
            click.echo(f"Avg response helpful rate: {stats_data['avg_response_helpful_rate']:.2f}")
            click.echo(f"Avg user satisfied rate: {stats_data['avg_user_satisfied_rate']:.2f}")
            click.echo(f"Error count: {stats_data['error_count']}")
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

    from claw_data_filter.llm.async_client import AsyncLLMClient
    from claw_data_filter.processors.round_feedback import PressureTest

    async def _run_pressure_test():
        llm = AsyncLLMClient(
            endpoint=config.llm_endpoint,
            api_key=config.llm_api_key,
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
    else:
        click.echo("Pressure test FAILED")
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

    click.echo(f"Starting round feedback processing with concurrency={config.max_concurrency}...")

    async def _run_round_feedback():
        from claw_data_filter.llm.async_client import AsyncLLMClient
        from claw_data_filter.processors.round_feedback import RoundFeedbackProcessor
        from claw_data_filter.storage.duckdb_store import DuckDBStore

        llm = AsyncLLMClient(
            endpoint=config.llm_endpoint,
            api_key=config.llm_api_key,
            timeout=config.llm_timeout,
        )

        store = DuckDBStore(config.db_path)
        processor = RoundFeedbackProcessor(store, llm, config.max_concurrency)

        try:
            total_success = 0
            total_failures = 0

            while True:
                batch = store.get_unprocessed_samples(limit=config.batch_size)
                if not batch:
                    break

                success, failures = await processor.process_batch(batch)
                total_success += success
                total_failures += failures
                click.echo(f"Processed batch: {success} success, {failures} failures")

            click.echo(f"Round feedback processing complete: {total_success} success, {total_failures} failures")
        finally:
            await llm.close()
            store.close()

    asyncio.run(_run_round_feedback())


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
