"""Click CLI for agent data filter tool."""
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
from claw_data_filter.processors.evaluator import Evaluator
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
@click.option("--workers", type=int, default=None, help="Number of parallel workers")
@click.option("--batch-size", type=int, default=None, help="Batch size per worker")
@click.pass_context
def evaluate(ctx, workers, batch_size):
    """Evaluate all unevaluated samples using LLM."""
    config = ctx.obj["config"]
    if workers:
        config.worker_count = workers
    if batch_size:
        config.batch_size = batch_size

    click.echo(f"Starting evaluation with {config.worker_count} workers...")

    store = DuckDBStore(config.db_path)
    evaluator = Evaluator(store, config)

    try:
        success, failures = evaluator.evaluate_batch(workers=config.worker_count)
        click.echo(f"Evaluation complete: {success} success, {failures} failures")
    finally:
        evaluator.close()
        store.close()


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
    """Show statistics about imported data and evaluations."""
    config = ctx.obj["config"]
    store = DuckDBStore(config.db_path)

    try:
        stats_data = store.get_stats()
        click.echo("=== Statistics ===")
        click.echo(f"Total samples: {stats_data['total_samples']}")
        click.echo(f"Total evaluations: {stats_data['total_evaluations']}")
        if stats_data["total_evaluations"] > 0:
            click.echo(f"Avg progress score: {stats_data['avg_progress_score']:.2f}")
            click.echo(f"Avg tool quality: {stats_data['avg_tool_quality']:.2f}")
            click.echo(f"Avg tool success rate: {stats_data['avg_tool_success_rate']:.2f}")
            click.echo(f"Avg overall score: {stats_data['avg_overall_score']:.2f}")
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
        click.echo(f"Evaluation count: {store.get_evaluation_count()}")
    finally:
        store.close()


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
