#!/usr/bin/env python3
"""
Download stage: Fetch planet.osm.pbf from OSM mirrors.

Environment variables:
  - RUN_ID: Unique identifier for this pipeline run
  - OUTPUT_PREFIX: S3 prefix for output files (e.g., /run/<run_id>)
  - S3_BUCKET: S3 bucket name
  - PLANET_URL: Optional custom URL for planet file
    (defaults to https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf)
"""

import os
import subprocess
import tempfile
from pathlib import Path

from common import get_s3_client, get_s3_bucket, require_env

# Configuration
RUN_ID = os.environ.get("RUN_ID")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "").lstrip("/")
PLANET_URL = (
    os.environ.get("PLANET_URL")
    or "https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf"
)


def main() -> None:
    """Download planet.osm.pbf from OSM mirrors."""
    require_env("RUN_ID", "OUTPUT_PREFIX", "S3_BUCKET")

    s3 = get_s3_client()
    bucket = get_s3_bucket()

    print("=" * 60)
    print("DOWNLOAD STAGE")
    print("=" * 60)
    print(f"Run ID: {RUN_ID}")
    print(f"Source: {PLANET_URL}")
    print(f"Output Prefix: {OUTPUT_PREFIX}")
    print(f"Destination: s3://{bucket}/{OUTPUT_PREFIX}/planet.osm.pbf")
    print()

    with tempfile.TemporaryDirectory() as tmpdir:
        work_dir = Path(tmpdir)
        planet_path = work_dir / "planet.osm.pbf"

        # Download with aria2c for parallel downloads, fallback to curl
        print("Downloading planet file...")
        try:
            subprocess.run(
                [
                    "aria2c",
                    "--max-connection-per-server=4",
                    "--split=4",
                    "--min-split-size=100M",
                    "--dir",
                    str(work_dir),
                    "--out",
                    "planet.osm.pbf",
                    PLANET_URL,
                ],
                check=True,
            )
        except FileNotFoundError:
            print("aria2c not found, falling back to curl...")
            subprocess.run(
                ["curl", "-L", "-f", "-o", str(planet_path), PLANET_URL],
                check=True,
            )

        size_gb = planet_path.stat().st_size / (1024**3)
        print(f"Downloaded {size_gb:.1f} GB")

        # Upload to S3
        print("Uploading to S3...")
        storage_key = f"{OUTPUT_PREFIX}/planet.osm.pbf"
        s3.upload_file(str(planet_path), bucket, storage_key)
        print(f"Uploaded to s3://{bucket}/{storage_key}")
        print("Done!")


if __name__ == "__main__":
    main()
