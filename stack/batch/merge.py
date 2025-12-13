#!/usr/bin/env python3
"""
Merge stage: Combine all shard outputs into final dataset.

Environment variables:
  - RUN_ID: Unique identifier for this pipeline run
  - INPUT_PREFIX: S3 prefix for input files (e.g., /run/<run_id>)
  - OUTPUT_PREFIX: S3 prefix for output files (e.g., /run/<run_id>)
  - S3_BUCKET: S3 bucket name
"""

import os
import sys
import tempfile
from pathlib import Path

import duckdb

from common import get_s3_client, get_s3_bucket, require_env

# Configuration
RUN_ID = os.environ.get("RUN_ID")
INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "").lstrip("/")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "").lstrip("/")


def main() -> None:
    """Combine all shard outputs into final dataset."""
    require_env("RUN_ID", "INPUT_PREFIX", "OUTPUT_PREFIX", "S3_BUCKET")

    s3 = get_s3_client()
    bucket = get_s3_bucket()

    print("=" * 60)
    print("MERGE STAGE")
    print("=" * 60)
    print(f"Run ID: {RUN_ID}")
    print(f"Bucket: {bucket}")
    print(f"Input Prefix: {INPUT_PREFIX}")
    print(f"Output Prefix: {OUTPUT_PREFIX}")
    print()

    with tempfile.TemporaryDirectory() as tmpdir:
        work_dir = Path(tmpdir)
        shards_dir = work_dir / "shards"
        shards_dir.mkdir()

        # List all shard outputs
        prefix = f"{INPUT_PREFIX}/shards/"
        print(f"Listing shards in s3://{bucket}/{prefix}...")

        all_files = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                all_files.append(obj["Key"])

        parquet_keys = [k for k in all_files if k.endswith("/data.parquet")]

        print(f"Found {len(parquet_keys)} shards with data")

        if not parquet_keys:
            print("ERROR: No shard outputs found")
            sys.exit(1)

        # Download all shard parquet files
        print("Downloading shard outputs...")
        for i, key in enumerate(parquet_keys):
            shard_id = key.split("/")[-2]
            local_path = shards_dir / f"{shard_id}.parquet"
            s3.download_file(bucket, key, str(local_path))
            if (i + 1) % 10 == 0:
                print(f"  Downloaded {i + 1}/{len(parquet_keys)}")

        # Merge with DuckDB
        print("Merging parquet files...")
        conn = duckdb.connect()

        output_path = work_dir / "pois.parquet"
        conn.sql(
            f"""
            COPY (
                SELECT * FROM read_parquet('{shards_dir}/*.parquet')
            ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """
        )

        # Get final stats
        stats = conn.sql(
            f"""
            SELECT
                COUNT(*) as total,
                COUNT(DISTINCT class) as classes,
                COUNT(DISTINCT shard_id) as shards
            FROM read_parquet('{output_path}')
        """
        ).fetchone()

        print(
            f"Merged output: {stats[0]:,} POIs, {stats[1]} classes, {stats[2]} shards"
        )
        print(f"File size: {output_path.stat().st_size / (1024**2):.1f} MB")

        # Upload final output
        final_key = f"{OUTPUT_PREFIX}/output/pois.parquet"
        print(f"Uploading to s3://{bucket}/{final_key}...")
        s3.upload_file(str(output_path), bucket, final_key)

        # Also copy to the 'latest' location for the tiles job
        latest_key = "parquet/pois.parquet"
        print(f"Copying to s3://{bucket}/{latest_key}...")
        s3.upload_file(str(output_path), bucket, latest_key)

        conn.close()
        print("Done!")


if __name__ == "__main__":
    main()
