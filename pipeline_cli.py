#!/usr/bin/env python3
"""
Click-based CLI for orchestrating the OSM-H3 AWS Batch pipeline.

This wraps the existing BatchPipelineRunner (download → shard → process → merge
→ tiles) and adds lightweight status monitoring so the legacy shell scripts can
be retired in favor of a single Python entry point.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Iterable

import boto3
import click
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError

from scripts.run_pipeline import BatchPipelineRunner, STAGES


def _resolve_region(explicit: str | None) -> str:
    region = (
        explicit or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    )
    if not region:
        raise click.UsageError(
            "AWS region not configured. Pass --region or export AWS_REGION."
        )
    return region


def _build_runner(
    *,
    region: str | None,
    project_name: str,
    run_id: str | None,
    bucket: str | None,
    job_queue: str | None,
    planet_url: str | None,
    max_resolution: int | None,
    max_nodes_per_shard: int | None,
    tiles_output: str,
) -> BatchPipelineRunner:
    resolved_region = _resolve_region(region)
    resolved_run_id = run_id or f"planet-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}"
    return BatchPipelineRunner(
        region=resolved_region,
        project_name=project_name,
        run_id=resolved_run_id,
        bucket=bucket,
        job_queue=job_queue,
        planet_url=planet_url,
        max_resolution=max_resolution,
        max_nodes_per_shard=max_nodes_per_shard,
        tiles_output=tiles_output,
    )


@click.group(help="OSM-H3 pipeline helpers (AWS Batch).")
def cli() -> None:
    """Create the root Click command group."""


@cli.command("run", help="Run download → shard → process → merge → tiles sequentially.")
@click.option(
    "--region", help="AWS region (defaults to AWS_REGION/AWS_DEFAULT_REGION)."
)
@click.option(
    "--project-name", default="osm-h3", show_default=True, help="Resource prefix."
)
@click.option(
    "--run-id", help="Custom run identifier (defaults to planet-<timestamp>)."
)
@click.option("--bucket", help="Override S3 bucket name.")
@click.option("--job-queue", help="Override AWS Batch job queue name.")
@click.option("--planet-url", help="Custom planet/region PBF URL to download.")
@click.option(
    "--max-resolution", type=int, help="Override MAX_RESOLUTION env for sharder."
)
@click.option(
    "--max-nodes-per-shard",
    type=int,
    help="Override MAX_NODES_PER_SHARD env for sharder.",
)
@click.option(
    "--tiles-output",
    default="pois.pmtiles",
    show_default=True,
    help="PMTiles filename.",
)
@click.option(
    "--start-at",
    type=click.Choice(STAGES),
    default="download",
    show_default=True,
    help="Stage to start from.",
)
def run_pipeline(
    *,
    region: str | None,
    project_name: str,
    run_id: str | None,
    bucket: str | None,
    job_queue: str | None,
    planet_url: str | None,
    max_resolution: int | None,
    max_nodes_per_shard: int | None,
    tiles_output: str,
    start_at: str,
) -> None:
    """Run the five Batch stages in order."""
    runner = _build_runner(
        region=region,
        project_name=project_name,
        run_id=run_id,
        bucket=bucket,
        job_queue=job_queue,
        planet_url=planet_url,
        max_resolution=max_resolution,
        max_nodes_per_shard=max_nodes_per_shard,
        tiles_output=tiles_output,
    )
    click.echo(f"Using bucket: {runner.bucket}")
    click.echo(f"Using job queue: {runner.job_queue}")
    click.echo(f"Run ID: {runner.run_id}")
    runner.run(start_at)


@cli.command("status", help="Show AWS Batch queue/job status (optionally watch).")
@click.option(
    "--region", help="AWS region (defaults to AWS_REGION/AWS_DEFAULT_REGION)."
)
@click.option(
    "--project-name", default="osm-h3", show_default=True, help="Resource prefix."
)
@click.option("--job-queue", help="Override AWS Batch job queue name.")
@click.option("--bucket", help="S3 bucket to count parquet outputs from.")
@click.option(
    "--watch/--no-watch",
    default=False,
    show_default=True,
    help="Continuously refresh status.",
)
@click.option(
    "--interval",
    default=30,
    show_default=True,
    help="Seconds between refreshes when --watch is used.",
)
def show_status(
    *,
    region: str | None,
    project_name: str,
    job_queue: str | None,
    bucket: str | None,
    watch: bool,
    interval: int,
) -> None:
    """List job counts per status plus a snapshot of running jobs."""
    resolved_region = _resolve_region(region)
    session = boto3.Session(region_name=resolved_region)
    queue_name = job_queue or f"{project_name}-queue"
    batch = session.client("batch")
    s3 = session.client("s3") if bucket else None

    def render_once() -> None:
        click.echo(f"Region: {resolved_region}")
        click.echo(f"Job queue: {queue_name}")
        click.echo("")
        render_queue_summary(batch, queue_name)
        click.echo("")
        render_running_jobs(batch, queue_name)
        if s3 and bucket:
            click.echo("")
            render_parquet_stats(s3, bucket)

    if watch:
        try:
            while True:
                click.clear()
                click.echo(
                    f"OSM-H3 Batch Status — {datetime.utcnow():%Y-%m-%d %H:%M:%S} UTC"
                )
                click.echo("=" * 72)
                render_once()
                click.echo("")
                click.echo("Press Ctrl+C to stop. Refreshing...")
                time.sleep(max(5, interval))
        except KeyboardInterrupt:
            click.echo("\nStopped watching.")
    else:
        render_once()


def render_queue_summary(batch_client: BaseClient, queue_name: str) -> None:
    """Print a single-line summary per AWS Batch status."""
    statuses: Iterable[str] = (
        "SUBMITTED",
        "PENDING",
        "RUNNABLE",
        "STARTING",
        "RUNNING",
        "SUCCEEDED",
        "FAILED",
    )
    click.echo("Job counts:")
    for status in statuses:
        try:
            resp = batch_client.list_jobs(
                jobQueue=queue_name, jobStatus=status, maxResults=100
            )
            count = len(resp.get("jobSummaryList", []))
        except (BotoCoreError, ClientError) as exc:
            raise click.ClickException(
                f"Unable to list jobs for {status}: {exc}"
            ) from exc
        click.echo(f"  {status:<10} {count}")


def render_running_jobs(
    batch_client: BaseClient, queue_name: str, limit: int = 5
) -> None:
    """Show a small table of currently running jobs."""
    try:
        resp = batch_client.list_jobs(
            jobQueue=queue_name, jobStatus="RUNNING", maxResults=limit
        )
    except (BotoCoreError, ClientError) as exc:
        raise click.ClickException(f"Unable to list running jobs: {exc}") from exc

    jobs = resp.get("jobSummaryList", [])
    if not jobs:
        click.echo("Running jobs: none")
        return

    click.echo("Running jobs:")
    for job in jobs:
        started_at = _fmt_timestamp(job.get("startedAt"))
        click.echo(f"  {job.get('jobName','?'):<40} started {started_at}")


def render_parquet_stats(s3_client: BaseClient, bucket: str) -> None:
    """Count parquet objects written so far."""
    prefix = "parquet/"
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        total = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            total += page.get("KeyCount", 0)
        click.echo(f"Parquet objects under s3://{bucket}/{prefix}: {total}")
    except (BotoCoreError, ClientError) as exc:
        raise click.ClickException(
            f"Unable to list s3://{bucket}/{prefix}: {exc}"
        ) from exc


def _fmt_timestamp(epoch_ms: int | None) -> str:
    if not epoch_ms:
        return "n/a"
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


if __name__ == "__main__":
    cli()
