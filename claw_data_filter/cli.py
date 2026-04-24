"""Click CLI for agent data filter tool."""
import asyncio
import logging
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import click

from claw_data_filter.config import Config
from claw_data_filter.exporters.report_exporter import ReportExporter
from claw_data_filter.exporters.unified_exporter import (
    OPENAI_ROUND_FEEDBACK,
    ExportFilterSpec,
    ExportRequest,
    UnifiedExporter,
)
from claw_data_filter.importers.jsonl_importer import JSONLImporter
from claw_data_filter.pipeline import PipelineConfig, PipelineService
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


def _default_isolated_round_feedback_db_path(source_db_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return source_db_path.parent / "isolated" / f"round_feedback_sample_{timestamp}.duckdb"


def _summarize_round_feedback_sample(sample_uid: str, sample_input: dict) -> dict[str, int]:
    from claw_data_filter.processors.round_feedback import TurnContextBuilder

    messages = sample_input.get("normalized_messages") or []
    builder = TurnContextBuilder()
    response_contexts = builder.extract_response_contexts(sample_uid, messages)
    episode_contexts = builder.extract_episode_contexts(sample_uid, messages)
    response_prompt_lengths = [len(builder.build_response_progress_prompt(context)) for context in response_contexts]
    episode_prompt_lengths = [len(builder.build_user_satisfied_prompt(context)) for context in episode_contexts]

    return {
        "message_count": len(messages),
        "response_context_count": len(response_contexts),
        "episode_context_count": len(episode_contexts),
        "max_response_prompt_chars": max(response_prompt_lengths, default=0),
        "max_episode_prompt_chars": max(episode_prompt_lengths, default=0),
    }

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
@click.option("--num-turns-min", type=int, help="Filter by minimum num_turns")
@click.option("--num-turns-max", type=int, help="Filter by maximum num_turns")
@click.option("--session-merge-keep", type=bool, help="Filter by session merge keep marker (true/false)")
@click.option("--session-merge-status", type=click.Choice(["keep", "merged", "skipped", "unmarked"]), help="Filter by session merge status")
@click.option("--has-error", type=bool, help="Filter by has error (true/false)")
@click.option(
    "--export-format",
    type=click.Choice([OPENAI_ROUND_FEEDBACK]),
    default=OPENAI_ROUND_FEEDBACK,
    show_default=True,
    help="Export format",
)
@click.option("--export", type=click.Path(), required=True, help="Output JSONL file")
@click.option("--report", type=click.Path(), help="Output report JSON file")
@click.option("--limit", type=int, help="Limit number of results")
@click.pass_context
def filter_cmd(ctx, response_progress_rate, user_satisfied_rate, user_negative_feedback_rate, empty_response, num_turns_min, num_turns_max, session_merge_keep, session_merge_status, has_error, export_format, export, report, limit):
    """Filter samples and export to JSONL with optional report."""
    config = ctx.obj["config"]

    RATE_PATTERN = re.compile(r"^(>=|<=|>|<|!=|=)\s*([\d.]+)$")

    filter_spec = ExportFilterSpec(
        empty_response=empty_response,
        num_turns_min=num_turns_min,
        num_turns_max=num_turns_max,
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


@cli.command(name="round-feedback-sample")
@click.option("--sample-uid", "sample_uids", multiple=True, required=True, help="Sample UID to isolate and process; repeat for multiple samples")
@click.option("--source-db-path", type=click.Path(exists=True, dir_okay=False, path_type=Path), help="Source DuckDB path; defaults to --db-path")
@click.option("--isolated-db-path", type=click.Path(dir_okay=False, path_type=Path), help="Output DuckDB path for isolated reproduction")
@click.option("--workers", type=int, default=1, show_default=True, help="Internal concurrency used while processing each isolated sample")
@click.pass_context
def round_feedback_sample(ctx, sample_uids, source_db_path, isolated_db_path, workers):
    """Run round-feedback on specific sample_uids in an isolated DuckDB."""
    from claw_data_filter.models.sample import Sample

    config = ctx.obj["config"]
    source_path = source_db_path or Path(config.db_path)
    isolated_path = isolated_db_path or _default_isolated_round_feedback_db_path(source_path)
    isolated_path.parent.mkdir(parents=True, exist_ok=True)

    if isolated_path.exists():
        raise click.ClickException(f"Isolated DB already exists: {isolated_path}")

    source_store = DuckDBStore(source_path, read_only=True)
    try:
        sample_records: list[tuple[str, dict, dict[str, int]]] = []
        for sample_uid in sample_uids:
            record = source_store.get_sample_by_uid(sample_uid)
            if not record:
                raise click.ClickException(f"Sample UID not found in source DB: {sample_uid}")
            runtime_input = {
                "normalized_messages": record["normalized_messages"],
                "normalized_tools": record["normalized_tools"],
                "source_metadata": record["source_metadata"],
            }
            summary = _summarize_round_feedback_sample(sample_uid, runtime_input)
            sample_records.append((sample_uid, runtime_input, summary))
    finally:
        source_store.close()

    click.echo(f"Preparing isolated round-feedback run for {len(sample_records)} sample(s)...")
    click.echo(f"Source DB: {source_path}")
    click.echo(f"Isolated DB: {isolated_path}")
    logger.info(
        "CLI round feedback sample command starting: source_db=%s isolated_db=%s sample_count=%s workers=%s endpoint=%s model=%s",
        source_path,
        isolated_path,
        len(sample_records),
        workers,
        config.llm_endpoint,
        config.llm_model_id,
    )
    for sample_uid, _, summary in sample_records:
        click.echo(
            "Sample summary: "
            f"sample_uid={sample_uid} messages={summary['message_count']} "
            f"response_contexts={summary['response_context_count']} episode_contexts={summary['episode_context_count']} "
            f"max_response_prompt_chars={summary['max_response_prompt_chars']} "
            f"max_episode_prompt_chars={summary['max_episode_prompt_chars']}"
        )

    async def _run_round_feedback_sample() -> None:
        from claw_data_filter.llm.async_client import AsyncLLMClient
        from claw_data_filter.processors.round_feedback import RoundFeedbackProcessor

        llm = AsyncLLMClient(
            endpoint=config.llm_endpoint,
            api_key=config.llm_api_key,
            model=config.llm_model_id,
            timeout=config.llm_timeout,
        )
        isolated_store = DuckDBStore(isolated_path)
        processor = RoundFeedbackProcessor(
            isolated_store,
            llm,
            max(1, workers),
            llm_max_retries=config.max_retries,
            llm_retry_base_delay=config.llm_retry_base_delay,
            llm_retry_max_delay=config.llm_retry_max_delay,
        )

        try:
            for sample_uid, sample_input, summary in sample_records:
                isolated_store.insert_sample(
                    Sample(
                        sample_uid=sample_uid,
                        normalized_messages=sample_input.get("normalized_messages") or [],
                        normalized_tools=sample_input.get("normalized_tools") or [],
                        source_metadata=sample_input.get("source_metadata") or {},
                        message_count=len(sample_input.get("normalized_messages") or []),
                    )
                )
                result = await processor.process_sample(sample_uid, sample_input)
                response_llm_errors = sum(1 for row in result.response_judgments if row.llm_error)
                episode_llm_errors = sum(1 for row in result.episode_judgments if row.llm_error)
                click.echo(
                    "Isolated sample complete: "
                    f"sample_uid={sample_uid} response_contexts={summary['response_context_count']} "
                    f"episode_contexts={summary['episode_context_count']} response_llm_errors={response_llm_errors} "
                    f"episode_llm_errors={episode_llm_errors} has_error={bool(result.tool_stats.get('has_error'))}"
                )
        finally:
            await llm.close()
            isolated_store.close()

    asyncio.run(_run_round_feedback_sample())


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


@cli.command(name="pipeline-run")
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
    help="Path to the incremental pipeline TOML config",
)
def pipeline_run_cmd(config_path: Path):
    """Run the incremental tar-to-Unisound pipeline once."""
    pipeline_config = PipelineConfig.from_toml(config_path)
    service = PipelineService(pipeline_config)
    try:
        summary = service.run_once()
    finally:
        service.close()

    click.echo("=== Pipeline Run Summary ===")
    for key in sorted(summary):
        click.echo(f"{key}: {summary[key]}")


def main():
    cli(obj={})


if __name__ == "__main__":
    main()
