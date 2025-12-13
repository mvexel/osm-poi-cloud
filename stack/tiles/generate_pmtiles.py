#!/usr/bin/env python3
"""
Generate PMTiles from Parquet POI data.

Downloads all Parquet files from S3, converts to GeoJSON,
runs tippecanoe to generate PMTiles, and uploads to S3.

Environment variables:
  - S3_BUCKET: S3 bucket name
  - PMTILES_OUTPUT: Output filename (default: pois.pmtiles)
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import boto3
import pyarrow.parquet as pq

S3_BUCKET = os.environ.get("S3_BUCKET")
PMTILES_OUTPUT = os.environ.get("PMTILES_OUTPUT", "pois.pmtiles")


def download_parquet_files(bucket: str, work_dir: Path) -> list[Path]:
    """Download all Parquet files from S3."""
    s3 = boto3.client("s3")
    parquet_dir = work_dir / "parquet"
    parquet_dir.mkdir(exist_ok=True)

    print("Listing Parquet files in S3...")
    response = s3.list_objects_v2(Bucket=bucket, Prefix="parquet/")

    files = []
    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".parquet"):
            local_path = parquet_dir / Path(key).name
            print(f"  Downloading {key}...")
            s3.download_file(bucket, key, str(local_path))
            files.append(local_path)

    # Handle pagination
    while response.get("IsTruncated"):
        response = s3.list_objects_v2(
            Bucket=bucket,
            Prefix="parquet/",
            ContinuationToken=response["NextContinuationToken"],
        )
        for obj in response.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                local_path = parquet_dir / Path(key).name
                print(f"  Downloading {key}...")
                s3.download_file(bucket, key, str(local_path))
                files.append(local_path)

    print(f"Downloaded {len(files)} Parquet files")
    return files


def parquet_to_geojson(parquet_files: list[Path], output_path: Path) -> int:
    """Convert Parquet files to newline-delimited GeoJSON."""
    print("Converting Parquet to GeoJSON...")
    total_features = 0

    def _require_columns(df: dict, cols: list[str], pq_file: Path) -> None:
        missing = [c for c in cols if c not in df]
        if missing:
            raise KeyError(
                f"Missing required Parquet columns {missing} in {pq_file.name}. "
                f"Available: {sorted(df.keys())}"
            )

    with open(output_path, "w") as f:
        for pq_file in parquet_files:
            print(f"  Processing {pq_file.name}...")
            table = pq.read_table(pq_file)
            df = table.to_pydict()

            _require_columns(df, ["lon", "lat", "name", "class"], pq_file)
            num_rows = len(df["name"])
            for i in range(num_rows):
                feature = {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [df["lon"][i], df["lat"][i]],
                    },
                    "properties": {
                        "name": df["name"][i],
                        "class": df["class"][i],
                    },
                }

                # Add optional properties if present
                for key in [
                    "state",
                    "shard_id",
                    "osm_id",
                    "osm_type",
                    "amenity",
                    "shop",
                    "cuisine",
                    "brand",
                    "opening_hours",
                    "website",
                    "phone",
                    "operator",
                ]:
                    if key in df and df[key][i] not in (None, ""):
                        feature["properties"][key] = df[key][i]

                # Include any H3 columns if present (e.g. h3_r3..h3_r9)
                for key in df.keys():
                    if key.startswith("h3_r") and df[key][i] not in (None, ""):
                        feature["properties"][key] = df[key][i]

                f.write(json.dumps(feature) + "\n")
                total_features += 1

            if total_features % 100000 == 0:
                print(f"    {total_features:,} features written...")

    print(f"Total features: {total_features:,}")
    return total_features


def generate_pmtiles(geojson_path: Path, output_path: Path) -> None:
    """Run tippecanoe to generate PMTiles."""
    print("Generating PMTiles with tippecanoe...")

    cmd = [
        "tippecanoe",
        "-o", str(output_path),
        "--force",  # Overwrite existing
        "--name", "OSM POIs",
        "--description", "Points of Interest from OpenStreetMap",
        "--attribution", "© OpenStreetMap contributors",
        "--minimum-zoom", "2",
        "--maximum-zoom", "14",
        "--drop-densest-as-needed",  # Drop points at low zooms to avoid overcrowding
        "--extend-zooms-if-still-dropping",
        "--layer", "pois",
        str(geojson_path),
    ]

    print(f"  Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"tippecanoe failed: {result.stderr}")
        sys.exit(1)

    size_mb = output_path.stat().st_size / 1024 / 1024
    print(f"Generated {output_path} ({size_mb:.1f} MB)")


def upload_pmtiles(local_path: Path, bucket: str, key: str) -> str:
    """Upload PMTiles to S3."""
    print(f"Uploading to s3://{bucket}/{key}...")
    s3 = boto3.client("s3")

    # Upload with correct content type for PMTiles
    s3.upload_file(
        str(local_path),
        bucket,
        key,
        ExtraArgs={"ContentType": "application/vnd.pmtiles"},
    )

    return f"s3://{bucket}/{key}"


def main():
    if not S3_BUCKET:
        print("ERROR: S3_BUCKET environment variable required")
        sys.exit(1)

    print("=" * 50)
    print("PMTiles Generation")
    print("=" * 50)
    print(f"Bucket: {S3_BUCKET}")
    print(f"Output: {PMTILES_OUTPUT}")
    print()

    with tempfile.TemporaryDirectory() as tmpdir:
        work_dir = Path(tmpdir)

        # Download all Parquet files
        parquet_files = download_parquet_files(S3_BUCKET, work_dir)

        if not parquet_files:
            print("ERROR: No Parquet files found in S3")
            sys.exit(1)

        # Convert to GeoJSON
        geojson_path = work_dir / "pois.geojson"
        total_features = parquet_to_geojson(parquet_files, geojson_path)

        if total_features == 0:
            print("ERROR: No features to process")
            sys.exit(1)

        # Generate PMTiles
        pmtiles_path = work_dir / PMTILES_OUTPUT
        generate_pmtiles(geojson_path, pmtiles_path)

        # Upload to S3
        s3_key = f"tiles/{PMTILES_OUTPUT}"
        s3_uri = upload_pmtiles(pmtiles_path, S3_BUCKET, s3_key)

        print()
        print("=" * 50)
        print("Done!")
        print("=" * 50)
        print(f"PMTiles uploaded to: {s3_uri}")
        print()
        print("To serve via CloudFront, the URL will be:")
        print(f"  https://<distribution>.cloudfront.net/{s3_key}")


if __name__ == "__main__":
    main()
