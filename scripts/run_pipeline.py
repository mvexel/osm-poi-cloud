#!/usr/bin/env python3
"""
Simple AWS Batch orchestrator for the OSM-H3 pipeline.

Submits jobs in the following order:
    1. Download planet file
    2. Shard the planet into H3 cells
    3. Process each shard in parallel (fan-out)
    4. Merge shard outputs
    5. Generate PMTiles

Requires the infrastructure stack (S3 bucket, Batch queue, job definitions)
to be deployed ahead of time. Uses boto3 to interact with AWS Batch/S3.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Sequence

import boto3


STAGES: Sequence[str] = ("download", "shard", "process", "merge", "tiles")


@dataclass(frozen=True)
class JobNames:
    """Convenience wrapper for the five job definitions we need."""

    download: str
    sharder: str
    processor: str
    merge: str
    tiles: str


class BatchPipelineRunner:
    """Submit and monitor AWS Batch jobs for each pipeline stage."""

    def __init__(
        self,
        *,
        region: str,
        project_name: str,
        run_id: str,
        bucket: str | None = None,
        job_queue: str | None = None,
        planet_url: str | None = None,
        max_resolution: int | None = None,
        max_nodes_per_shard: int | None = None,
        tiles_output: str = "pois.pmtiles",
    ) -> None:
        session = boto3.Session(region_name=region or None)
        if session.region_name is None:
            raise SystemExit("ERROR: AWS region not configured. Set AWS_REGION or pass --region.")

        self.region = session.region_name
        self.project_name = project_name
        self.run_id = run_id
        self.planet_url = planet_url
        self.max_resolution = max_resolution
        self.max_nodes_per_shard = max_nodes_per_shard
        self.tiles_output = tiles_output

        sts = session.client("sts")
        identity = sts.get_caller_identity()
        self.account_id = identity["Account"]

        self.bucket = bucket or f"{self.project_name}-data-{self.account_id}"
        self.job_queue = job_queue or f"{self.project_name}-queue"
        self.jobs = JobNames(
            download=f"{self.project_name}-download",
            sharder=f"{self.project_name}-sharder",
            processor=f"{self.project_name}-processor",
            merge=f"{self.project_name}-merge",
            tiles=f"{self.project_name}-tiles",
        )

        self.batch = session.client("batch")
        self.s3 = session.client("s3")

    # ------------------------------------------------------------------
    # High-level stages
    # ------------------------------------------------------------------

    def run(self, start_stage: str) -> None:
        """Execute pipeline stages starting at the requested stage."""
        start_index = STAGES.index(start_stage)
        for stage in STAGES[start_index:]:
            print("")
            print("=" * 72)
            print(f"Stage: {stage.upper()} ({self.run_id})")
            print("=" * 72)
            if stage == "download":
                self._run_download()
            elif stage == "shard":
                self._run_shard()
            elif stage == "process":
                self._run_process()
            elif stage == "merge":
                self._run_merge()
            elif stage == "tiles":
                self._run_tiles()

    def _run_download(self) -> None:
        env = [
            {"name": "STAGE", "value": "download"},
            {"name": "RUN_ID", "value": self.run_id},
            {"name": "S3_BUCKET", "value": self.bucket},
        ]
        if self.planet_url:
            env.append({"name": "PLANET_URL", "value": self.planet_url})

        job_id = self._submit_job(
            name=f"{self.run_id}-download",
            job_definition=self.jobs.download,
            environment=env,
        )
        self._wait_for_job(job_id, "download")

    def _run_shard(self) -> None:
        env = [
            {"name": "RUN_ID", "value": self.run_id},
            {"name": "S3_BUCKET", "value": self.bucket},
        ]
        if self.max_resolution is not None:
            env.append({"name": "MAX_RESOLUTION", "value": str(self.max_resolution)})
        if self.max_nodes_per_shard is not None:
            env.append({"name": "MAX_NODES_PER_SHARD", "value": str(self.max_nodes_per_shard)})

        job_id = self._submit_job(
            name=f"{self.run_id}-shard",
            job_definition=self.jobs.sharder,
            environment=env,
        )
        self._wait_for_job(job_id, "shard")

    def _run_process(self) -> None:
        manifest_key = f"runs/{self.run_id}/shards/manifest.json"
        shards = self._load_shards(manifest_key)
        if not shards:
            raise SystemExit(f"ERROR: No shards found in s3://{self.bucket}/{manifest_key}")

        print(f"Submitting {len(shards)} shard processing jobs...")
        job_ids = []
        for shard in shards:
            env = [
                {"name": "STAGE", "value": "process"},
                {"name": "RUN_ID", "value": self.run_id},
                {"name": "S3_BUCKET", "value": self.bucket},
                {"name": "SHARD_H3_INDEX", "value": shard["h3_index"]},
                {"name": "SHARD_RESOLUTION", "value": str(shard["resolution"])},
            ]
            job_id = self._submit_job(
                name=f"{self.run_id}-{shard['h3_index']}",
                job_definition=self.jobs.processor,
                environment=env,
            )
            job_ids.append(job_id)

        self._wait_for_jobs(job_ids, label="process", poll_seconds=25)

    def _run_merge(self) -> None:
        job_id = self._submit_job(
            name=f"{self.run_id}-merge",
            job_definition=self.jobs.merge,
            environment=[
                {"name": "STAGE", "value": "merge"},
                {"name": "RUN_ID", "value": self.run_id},
                {"name": "S3_BUCKET", "value": self.bucket},
            ],
        )
        self._wait_for_job(job_id, "merge")

    def _run_tiles(self) -> None:
        env = [
            {"name": "S3_BUCKET", "value": self.bucket},
            {"name": "PMTILES_OUTPUT", "value": self.tiles_output},
        ]
        job_id = self._submit_job(
            name=f"{self.run_id}-tiles",
            job_definition=self.jobs.tiles,
            environment=env,
        )
        self._wait_for_job(job_id, "tiles")

    # ------------------------------------------------------------------
    # AWS helpers
    # ------------------------------------------------------------------

    def _submit_job(
        self,
        *,
        name: str,
        job_definition: str,
        environment: List[Dict[str, str]],
    ) -> str:
        """Submit a job and return its ID."""
        response = self.batch.submit_job(
            jobName=name,
            jobQueue=self.job_queue,
            jobDefinition=job_definition,
            containerOverrides={"environment": environment},
        )
        job_id = response["jobId"]
        print(f"Submitted job {name} ({job_id})")
        return job_id

    def _wait_for_job(self, job_id: str, label: str) -> None:
        """Poll until the job reaches a terminal state."""
        print(f"Waiting for {label} job ({job_id})...")
        while True:
            job = self._describe_single_job(job_id)
            status = job["status"]
            if status in {"SUCCEEDED", "FAILED"}:
                if status == "FAILED":
                    reason = job.get("statusReason", "Unknown reason")
                    raise SystemExit(f"{label.capitalize()} job failed: {reason}")
                print(f"{label.capitalize()} job succeeded.")
                return
            time.sleep(15)

    def _wait_for_jobs(self, job_ids: Sequence[str], *, label: str, poll_seconds: int) -> None:
        """Poll a batch of jobs until they all finish."""
        remaining = set(job_ids)
        finished = 0
        while remaining:
            chunk = list(remaining)[:100]
            jobs = self.batch.describe_jobs(jobs=chunk)["jobs"]
            for job in jobs:
                status = job["status"]
                if status in {"SUCCEEDED", "FAILED"}:
                    remaining.discard(job["jobId"])
                    finished += 1
                    if status == "FAILED":
                        reason = job.get("statusReason", "Unknown reason")
                        raise SystemExit(f"{label.capitalize()} job {job['jobName']} failed: {reason}")
                    print(f"[{finished}/{len(job_ids)}] {label} job {job['jobName']} succeeded")
            time.sleep(poll_seconds)
        print(f"All {label} jobs completed.")

    def _describe_single_job(self, job_id: str) -> dict:
        jobs = self.batch.describe_jobs(jobs=[job_id])["jobs"]
        if not jobs:
            raise SystemExit(f"ERROR: Unable to describe job {job_id}")
        return jobs[0]

    def _load_shards(self, manifest_key: str) -> List[Dict[str, str]]:
        """Read shard manifest from S3 and return list of {h3_index, resolution} dicts."""
        response = self.s3.get_object(Bucket=self.bucket, Key=manifest_key)
        payload = json.loads(response["Body"].read())
        features = payload.get("features", [])
        shards = []
        for feature in features:
            props = feature.get("properties", {})
            if "h3_index" in props and "resolution" in props:
                shards.append(
                    {
                        "h3_index": str(props["h3_index"]),
                        "resolution": int(props["resolution"]),
                    }
                )
        return shards


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the OSM-H3 pipeline via AWS Batch.")
    parser.add_argument("--region", help="AWS region (defaults to AWS_REGION env var).")
    parser.add_argument("--project-name", default="osm-h3", help="Resource prefix (default: %(default)s).")
    parser.add_argument("--run-id", help="Identifier for this run (default: planet-<timestamp>).")
    parser.add_argument("--bucket", help="Override the S3 bucket name.")
    parser.add_argument("--job-queue", help="Override the AWS Batch job queue name.")
    parser.add_argument("--planet-url", help="Optional custom planet file URL.")
    parser.add_argument(
        "--max-resolution",
        type=int,
        help="Override MAX_RESOLUTION for the sharder (default uses job image default).",
    )
    parser.add_argument(
        "--max-nodes-per-shard",
        type=int,
        help="Override MAX_NODES_PER_SHARD for the sharder (default uses job image default).",
    )
    parser.add_argument(
        "--tiles-output",
        default="pois.pmtiles",
        help="Filename for PMTiles output (default: %(default)s).",
    )
    parser.add_argument(
        "--start-at",
        choices=STAGES,
        default="download",
        help="Begin the pipeline at this stage (default: %(default)s).",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv or sys.argv[1:])
    region = args.region or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    run_id = args.run_id or f"planet-{datetime.utcnow():%Y%m%d-%H%M%S}"

    runner = BatchPipelineRunner(
        region=region,
        project_name=args.project_name,
        run_id=run_id,
        bucket=args.bucket,
        job_queue=args.job_queue,
        planet_url=args.planet_url,
        max_resolution=args.max_resolution,
        max_nodes_per_shard=args.max_nodes_per_shard,
        tiles_output=args.tiles_output,
    )
    print(f"Using bucket: {runner.bucket}")
    print(f"Using job queue: {runner.job_queue}")
    print(f"Run ID: {runner.run_id}")
    runner.run(args.start_at)


if __name__ == "__main__":
    main()
