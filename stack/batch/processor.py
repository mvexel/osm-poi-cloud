#!/usr/bin/env python3
"""
Unified OSM-H3 batch processor.

Handles multiple stages of the pipeline based on STAGE environment variable:
- download: Fetch planet.osm.pbf from OSM mirrors
- process: Process a single H3 shard to Parquet
- merge: Combine all shard outputs into final dataset

Environment variables:
  - STAGE: Which stage to run (download, process, merge)
  - RUN_ID: Unique identifier for this pipeline run
  - S3_BUCKET: S3 bucket for input/output

For 'process' stage:
  - SHARD_H3_INDEX: H3 cell index to process
  - SHARD_RESOLUTION: H3 resolution of the shard

For 'download' stage:
  - PLANET_URL: Optional custom URL for planet file
"""

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import boto3
import duckdb


# ============================================================
# Configuration
# ============================================================

STAGE = os.environ.get("STAGE", "process")
RUN_ID = os.environ.get("RUN_ID")
S3_BUCKET = os.environ.get("S3_BUCKET")
SHARD_H3_INDEX = os.environ.get("SHARD_H3_INDEX")
SHARD_RESOLUTION = os.environ.get("SHARD_RESOLUTION")
PLANET_URL = (
    os.environ.get("PLANET_URL")
    or "https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf"
)


def require_env(*names: str) -> None:
    """Validate required environment variables."""
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


def configure_duckdb_extensions(conn: duckdb.DuckDBPyConnection) -> None:
    extension_directory = os.environ.get("DUCKDB_EXTENSION_DIRECTORY")
    if extension_directory:
        conn.execute(f"SET extension_directory='{extension_directory}'")


def load_duckdb_extension(
    conn: duckdb.DuckDBPyConnection, name: str, install_sql: str
) -> None:
    configure_duckdb_extensions(conn)
    try:
        conn.execute(f"LOAD {name}")
        return
    except Exception:
        pass

    try:
        conn.execute(install_sql)
        conn.execute(f"LOAD {name}")
    except Exception as e:
        raise RuntimeError(
            f"Failed to load DuckDB extension '{name}'. If running in a network-restricted "
            f"environment, pre-bundle extensions in the container and set DUCKDB_EXTENSION_DIRECTORY. "
            f"Original error: {e}"
        ) from e


# ============================================================
# Download Stage
# ============================================================


def stage_download() -> None:
    """Download planet.osm.pbf from OSM mirrors."""
    require_env("RUN_ID", "S3_BUCKET")

    print("=" * 60)
    print("DOWNLOAD STAGE")
    print("=" * 60)
    print(f"Run ID: {RUN_ID}")
    print(f"Source: {PLANET_URL}")
    print(f"Destination: s3://{S3_BUCKET}/runs/{RUN_ID}/planet.osm.pbf")
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
        s3_key = f"runs/{RUN_ID}/planet.osm.pbf"
        s3 = boto3.client("s3")
        s3.upload_file(
            str(planet_path),
            S3_BUCKET,
            s3_key,
            Config=boto3.s3.transfer.TransferConfig(
                multipart_threshold=100 * 1024 * 1024,
                max_concurrency=10,
            ),
        )
        print(f"Uploaded to s3://{S3_BUCKET}/{s3_key}")


# ============================================================
# Process Stage (per-shard)
# ============================================================


def stage_process() -> None:
    """Process a single H3 shard to Parquet."""
    require_env("RUN_ID", "S3_BUCKET", "SHARD_H3_INDEX", "SHARD_RESOLUTION")

    print("=" * 60)
    print("PROCESS STAGE")
    print("=" * 60)
    print(f"Run ID: {RUN_ID}")
    print(f"Shard: {SHARD_H3_INDEX} (resolution {SHARD_RESOLUTION})")
    print()

    with tempfile.TemporaryDirectory() as tmpdir:
        work_dir = Path(tmpdir)
        s3 = boto3.client("s3")

        # Download the planet file
        planet_key = f"runs/{RUN_ID}/planet.osm.pbf"
        planet_path = work_dir / "planet.osm.pbf"
        print(f"Downloading s3://{S3_BUCKET}/{planet_key}...")
        s3.download_file(S3_BUCKET, planet_key, str(planet_path))
        print(f"Downloaded {planet_path.stat().st_size / (1024**3):.1f} GB")

        # Get H3 cell bounding box for filtering
        bbox = get_h3_bbox(SHARD_H3_INDEX)
        print(f"Bounding box: {bbox}")

        # Filter PBF to this H3 cell's bounding box
        filtered_pbf = work_dir / "filtered.osm.pbf"
        print("Filtering PBF to bounding box...")
        subprocess.run(
            [
                "osmium",
                "extract",
                "--bbox",
                f"{bbox['west']},{bbox['south']},{bbox['east']},{bbox['north']}",
                "--strategy",
                "smart",
                str(planet_path),
                "-o",
                str(filtered_pbf),
            ],
            check=True,
        )
        # Remove planet file to free space
        planet_path.unlink()
        print(f"Filtered to {filtered_pbf.stat().st_size / (1024**2):.1f} MB")

        # Filter to POI-relevant tags
        poi_pbf = filter_to_pois(filtered_pbf, work_dir)
        filtered_pbf.unlink()

        # Convert to GeoJSON
        geojson_path = pbf_to_geojson(poi_pbf, work_dir)
        poi_pbf.unlink()

        # Process to Parquet with DuckDB
        parquet_path = process_to_parquet(geojson_path, SHARD_H3_INDEX, work_dir)

        if parquet_path is None:
            print("No POIs found in this shard, skipping upload")
            # Write empty marker so merge knows this shard was processed
            marker_key = f"runs/{RUN_ID}/shards/{SHARD_H3_INDEX}/_EMPTY"
            s3.put_object(Bucket=S3_BUCKET, Key=marker_key, Body=b"")
            return

        # Upload to S3
        s3_key = f"runs/{RUN_ID}/shards/{SHARD_H3_INDEX}/data.parquet"
        print(f"Uploading to s3://{S3_BUCKET}/{s3_key}...")
        s3.upload_file(str(parquet_path), S3_BUCKET, s3_key)
        print("Done!")


def get_h3_bbox(h3_index: str) -> dict:
    """Get bounding box for an H3 cell with some padding."""
    # Use DuckDB's H3 extension to get the boundary
    conn = duckdb.connect()
    load_duckdb_extension(conn, "h3", "INSTALL h3 FROM community")
    load_duckdb_extension(conn, "spatial", "INSTALL spatial")

    result = conn.sql(
        f"""
        WITH boundary AS (
            SELECT h3_cell_to_boundary_wkt('{h3_index}'::UBIGINT) as wkt
        )
        SELECT
            ST_XMin(ST_GeomFromText(wkt)) as west,
            ST_YMin(ST_GeomFromText(wkt)) as south,
            ST_XMax(ST_GeomFromText(wkt)) as east,
            ST_YMax(ST_GeomFromText(wkt)) as north
        FROM boundary
    """
    ).fetchone()

    # Add 1% padding to avoid edge effects
    west, south, east, north = result
    width = east - west
    height = north - south
    padding = max(width, height) * 0.01

    return {
        "west": west - padding,
        "south": south - padding,
        "east": east + padding,
        "north": north + padding,
    }


def filter_to_pois(input_pbf: Path, output_dir: Path) -> Path:
    """Filter PBF to POI-relevant features using osmium."""
    output_path = output_dir / "pois.osm.pbf"

    # First pass: keep only features with names
    named_pbf = output_dir / "named.osm.pbf"
    subprocess.run(
        [
            "osmium",
            "tags-filter",
            str(input_pbf),
            "nw/name",
            "-o",
            str(named_pbf),
        ],
        check=True,
    )

    # Second pass: filter to POI categories
    subprocess.run(
        [
            "osmium",
            "tags-filter",
            str(named_pbf),
            "nw/amenity",
            "nw/shop",
            "nw/leisure",
            "nw/tourism",
            "nw/office",
            "nw/healthcare",
            "nw/railway",
            "nw/aeroway",
            "nw/historic",
            "nw/man_made",
            "nw/natural",
            "nw/public_transport",
            "-o",
            str(output_path),
        ],
        check=True,
    )

    named_pbf.unlink()
    print(f"Filtered to POIs: {output_path.stat().st_size / (1024**2):.1f} MB")
    return output_path


def pbf_to_geojson(pbf_path: Path, output_dir: Path) -> Path:
    """Convert PBF to newline-delimited GeoJSON."""
    output_path = output_dir / "pois.ndjson"

    subprocess.run(
        [
            "osmium",
            "export",
            str(pbf_path),
            "-o",
            str(output_path),
            "-f",
            "geojsonseq",
            "-x",
            "print_record_separator=false",
            "-u",
            "type_id",
            "--geometry-types=point,polygon",
        ],
        check=True,
    )

    print(f"Converted to GeoJSON: {output_path.stat().st_size / (1024**2):.1f} MB")
    return output_path


def process_to_parquet(
    geojson_path: Path, shard_id: str, output_dir: Path
) -> Path | None:
    """Process GeoJSON to Parquet with POI classification."""
    output_path = output_dir / "data.parquet"

    conn = duckdb.connect()
    load_duckdb_extension(conn, "spatial", "INSTALL spatial")

    # Check if there's any data
    count = conn.sql(
        f"""
        SELECT COUNT(*) FROM read_json('{geojson_path}',
            columns={{id: 'VARCHAR', type: 'VARCHAR', geometry: 'JSON', properties: 'JSON'}},
            maximum_object_size=10485760
        )
    """
    ).fetchone()[0]

    if count == 0:
        print("No features found")
        return None

    print(f"Processing {count:,} features...")

    # Main processing query with POI classification
    query = f"""
    COPY (
        WITH raw_features AS (
            SELECT
                id as osm_id,
                properties->>'@type' as osm_type,
                properties->>'name' as name,
                properties->>'amenity' as amenity,
                properties->>'shop' as shop,
                properties->>'leisure' as leisure,
                properties->>'tourism' as tourism,
                properties->>'office' as office,
                properties->>'healthcare' as healthcare,
                properties->>'railway' as railway,
                properties->>'aeroway' as aeroway,
                properties->>'historic' as historic,
                properties->>'man_made' as man_made,
                properties->>'natural' as "natural",
                properties->>'public_transport' as public_transport,
                properties->>'cuisine' as cuisine,
                properties->>'opening_hours' as opening_hours,
                properties->>'phone' as phone,
                properties->>'website' as website,
                properties->>'brand' as brand,
                properties->>'operator' as "operator",
                ST_Centroid(ST_GeomFromGeoJSON(geometry)) as centroid
            FROM read_json('{geojson_path}',
                columns={{id: 'VARCHAR', type: 'VARCHAR', geometry: 'JSON', properties: 'JSON'}},
                maximum_object_size=10485760
            )
            WHERE properties->>'name' IS NOT NULL
              AND geometry IS NOT NULL
        ),
        classified AS (
            SELECT
                *,
                CASE
                    WHEN amenity IN ('restaurant', 'food_court', 'diner', 'bbq') THEN 'restaurant'
                    WHEN amenity IN ('cafe', 'coffee_shop', 'tea') THEN 'cafe_bakery'
                    WHEN amenity IN ('bar', 'pub', 'biergarten') THEN 'bar_pub'
                    WHEN amenity IN ('fast_food', 'food_truck', 'ice_cream', 'street_vendor') THEN 'fast_food'
                    WHEN shop IN ('ice_cream', 'dessert', 'frozen_yogurt') THEN 'ice_cream'
                    WHEN shop IN ('supermarket', 'convenience', 'grocery', 'marketplace') THEN 'grocery'
                    WHEN amenity = 'marketplace' THEN 'grocery'
                    WHEN shop IN ('bakery', 'butcher', 'cheese', 'confectionery', 'chocolate',
                                  'deli', 'fishmonger', 'frozen_food', 'greengrocer',
                                  'health_food', 'organic', 'pastry', 'tea', 'coffee') THEN 'specialty_food'
                    WHEN shop IN ('mall', 'department_store', 'car', 'clothes', 'fashion',
                                  'shoes', 'electronics', 'computer', 'hardware', 'doityourself',
                                  'furniture', 'jewelry', 'toys', 'books', 'gift', 'cosmetics') THEN 'retail'
                    WHEN amenity IN ('spa', 'sauna', 'hairdresser', 'beauty_salon', 'laundry', 'dry_cleaning') THEN 'personal_services'
                    WHEN shop IN ('hairdresser', 'beauty', 'massage') THEN 'personal_services'
                    WHEN amenity IN ('coworking_space', 'conference_centre') THEN 'professional_services'
                    WHEN office IN ('company', 'lawyer', 'architect', 'estate_agent', 'accountant') THEN 'professional_services'
                    WHEN amenity IN ('bank', 'atm', 'bureau_de_change', 'money_transfer') THEN 'finance'
                    WHEN tourism IN ('hotel', 'guest_house', 'hostel', 'motel', 'apartment',
                                     'chalet', 'alpine_hut', 'camp_site', 'caravan_site') THEN 'lodging'
                    WHEN amenity IN ('bus_station', 'ferry_terminal') THEN 'transport'
                    WHEN railway IN ('station', 'halt', 'stop', 'tram_stop') THEN 'transport'
                    WHEN public_transport = 'station' THEN 'transport'
                    WHEN aeroway IN ('aerodrome', 'terminal') THEN 'transport'
                    WHEN amenity IN ('fuel', 'charging_station', 'car_wash', 'car_rental', 'car_repair') THEN 'auto_services'
                    WHEN shop IN ('car_repair', 'tyres') THEN 'auto_services'
                    WHEN amenity IN ('parking', 'bicycle_parking', 'motorcycle_parking') THEN 'parking'
                    WHEN amenity IN ('hospital', 'clinic', 'doctors', 'dentist', 'pharmacy', 'ambulance_station') THEN 'healthcare'
                    WHEN healthcare IS NOT NULL THEN 'healthcare'
                    WHEN amenity IN ('school', 'kindergarten', 'college', 'university',
                                     'music_school', 'language_school', 'library') THEN 'education'
                    WHEN amenity IN ('townhall', 'courthouse', 'police', 'fire_station',
                                     'post_office', 'embassy') THEN 'government'
                    WHEN office = 'government' THEN 'government'
                    WHEN amenity IN ('community_centre', 'social_centre', 'youth_centre',
                                     'social_facility', 'shelter') THEN 'community'
                    WHEN amenity IN ('place_of_worship', 'church', 'mosque', 'temple', 'synagogue') THEN 'religious'
                    WHEN tourism IN ('museum', 'gallery') THEN 'culture'
                    WHEN amenity IN ('arts_centre', 'theatre', 'concert_hall', 'planetarium') THEN 'culture'
                    WHEN amenity IN ('cinema', 'nightclub', 'casino', 'bowling_alley', 'amusement_arcade') THEN 'entertainment'
                    WHEN leisure IN ('bowling_alley', 'escape_game') THEN 'entertainment'
                    WHEN leisure IN ('sports_centre', 'fitness_centre', 'gym', 'swimming_pool',
                                     'stadium', 'pitch', 'ice_rink', 'golf_course') THEN 'sports_fitness'
                    WHEN leisure IN ('park', 'garden', 'nature_reserve', 'playground', 'dog_park') THEN 'parks_outdoors'
                    WHEN tourism IN ('picnic_site', 'viewpoint') THEN 'parks_outdoors'
                    WHEN "natural" = 'beach' THEN 'parks_outdoors'
                    WHEN tourism IN ('attraction', 'information') THEN 'landmark'
                    WHEN historic IN ('monument', 'memorial', 'castle', 'ruins') THEN 'landmark'
                    WHEN man_made IN ('lighthouse', 'tower') THEN 'landmark'
                    WHEN amenity IN ('veterinary', 'animal_boarding', 'animal_shelter') THEN 'animal_services'
                    WHEN shop = 'pet' THEN 'animal_services'
                    WHEN shop IS NOT NULL THEN 'retail'
                    WHEN amenity IS NOT NULL OR leisure IS NOT NULL OR tourism IS NOT NULL THEN 'misc'
                    ELSE NULL
                END as class
            FROM raw_features
        )
        SELECT
            osm_id,
            osm_type,
            name,
            class,
            ST_X(centroid)::DOUBLE as lon,
            ST_Y(centroid)::DOUBLE as lat,
            '{shard_id}' as shard_id,
            amenity,
            shop,
            leisure,
            tourism,
            cuisine,
            opening_hours,
            phone,
            website,
            brand,
            "operator"
        FROM classified
        WHERE class IS NOT NULL
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """

    conn.sql(query)

    # Get stats
    stats = conn.sql(
        f"""
        SELECT COUNT(*) as total, COUNT(DISTINCT class) as classes
        FROM read_parquet('{output_path}')
    """
    ).fetchone()

    print(f"Output: {stats[0]:,} POIs in {stats[1]} classes")
    print(f"File size: {output_path.stat().st_size / (1024**2):.1f} MB")

    conn.close()
    return output_path if stats[0] > 0 else None


# ============================================================
# Merge Stage
# ============================================================


def stage_merge() -> None:
    """Combine all shard outputs into final dataset."""
    require_env("RUN_ID", "S3_BUCKET")

    print("=" * 60)
    print("MERGE STAGE")
    print("=" * 60)
    print(f"Run ID: {RUN_ID}")
    print()

    s3 = boto3.client("s3")

    with tempfile.TemporaryDirectory() as tmpdir:
        work_dir = Path(tmpdir)
        shards_dir = work_dir / "shards"
        shards_dir.mkdir()

        # List all shard outputs
        prefix = f"runs/{RUN_ID}/shards/"
        print(f"Listing shards in s3://{S3_BUCKET}/{prefix}...")

        parquet_keys = []
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith("/data.parquet"):
                    parquet_keys.append(obj["Key"])

        print(f"Found {len(parquet_keys)} shards with data")

        if not parquet_keys:
            print("ERROR: No shard outputs found")
            sys.exit(1)

        # Download all shard parquet files
        print("Downloading shard outputs...")
        for i, key in enumerate(parquet_keys):
            shard_id = key.split("/")[-2]
            local_path = shards_dir / f"{shard_id}.parquet"
            s3.download_file(S3_BUCKET, key, str(local_path))
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
        final_key = f"runs/{RUN_ID}/output/pois.parquet"
        print(f"Uploading to s3://{S3_BUCKET}/{final_key}...")
        s3.upload_file(str(output_path), S3_BUCKET, final_key)

        # Also copy to the 'latest' location for the tiles job
        latest_key = "parquet/pois.parquet"
        print(f"Copying to s3://{S3_BUCKET}/{latest_key}...")
        s3.copy_object(
            Bucket=S3_BUCKET,
            CopySource=f"{S3_BUCKET}/{final_key}",
            Key=latest_key,
        )

        conn.close()
        print("Done!")


# ============================================================
# Main
# ============================================================


def main() -> None:
    print(f"OSM-H3 Processor - Stage: {STAGE}")
    print()

    if STAGE == "download":
        stage_download()
    elif STAGE == "process":
        stage_process()
    elif STAGE == "merge":
        stage_merge()
    else:
        print(f"ERROR: Unknown stage '{STAGE}'")
        print("Valid stages: download, process, merge")
        sys.exit(1)


if __name__ == "__main__":
    main()
