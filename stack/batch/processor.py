#!/usr/bin/env python3
"""
OSM-H3 batch processor dispatcher.

This is a backward-compatible wrapper that dispatches to individual stage scripts.
Handles multiple stages of the pipeline based on STAGE environment variable:
- download: Fetch planet.osm.pbf from OSM mirrors
- process: Process a single H3 shard to Parquet
- merge: Combine all shard outputs into final dataset

For direct script execution, use:
  - download.py
  - process.py
  - merge.py

Environment variables:
  - STAGE: Which stage to run (download, process, merge)
  - RUN_ID: Unique identifier for this pipeline run
  - STORAGE_TYPE: Storage backend (local, s3) - defaults to 'local'
  - STORAGE_PATH: Base path for storage (local dir or S3 bucket)

  Legacy (S3-only):
  - S3_BUCKET: S3 bucket (sets STORAGE_TYPE=s3, STORAGE_PATH=S3_BUCKET)

For 'process' stage:
  - SHARD_ID: Shard identifier (e.g. "10-512-384")
  - SHARD_Z: Web Mercator tile zoom
  - SHARD_X: Web Mercator tile x
  - SHARD_Y: Web Mercator tile y
  - PLANET_FILE: Optional planet file path/key

For 'download' stage:
  - PLANET_URL: Optional custom URL for planet file
"""

import os
import sys

# ============================================================
# Configuration
# ============================================================

STAGE = os.environ.get("STAGE", "process")

# Backward compatibility: convert S3_BUCKET to STORAGE_PATH
if os.environ.get("S3_BUCKET") and not os.environ.get("STORAGE_PATH"):
    os.environ["STORAGE_TYPE"] = "s3"
    os.environ["STORAGE_PATH"] = os.environ["S3_BUCKET"]
    print(f"Note: Using S3_BUCKET={os.environ['S3_BUCKET']} (legacy mode)")
    print(f"      Consider using STORAGE_TYPE=s3 and STORAGE_PATH instead")
    print()


# ============================================================
# Main
# ============================================================


def main() -> None:
    print(f"OSM-H3 Processor - Stage: {STAGE}")
    print()

    if STAGE == "download":
        import download

        download.main()
    elif STAGE == "process":
        import process

        process.main()
    elif STAGE == "merge":
        import merge

        merge.main()
    else:
        print(f"ERROR: Unknown stage '{STAGE}'")
        print("Valid stages: download, process, merge")
        sys.exit(1)


if __name__ == "__main__":
    main()
