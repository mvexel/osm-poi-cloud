#!/usr/bin/env python3
"""
Process stage: Process a single H3 shard to Parquet.

Environment variables:
  - RUN_ID: Unique identifier for this pipeline run
  - INPUT_PREFIX: S3 prefix for input files (e.g., /run/<run_id>)
  - OUTPUT_PREFIX: S3 prefix for output files (e.g., /run/<run_id>)
  - S3_BUCKET: S3 bucket name
  - SHARD_ID: Shard identifier (e.g. "10-512-384")
  - SHARD_Z: Web Mercator tile zoom
  - SHARD_X: Web Mercator tile x
  - SHARD_Y: Web Mercator tile y
  - PLANET_FILE: Path/URL to planet file (optional, uses INPUT_PREFIX/planet.osm.pbf if not set)
  - H3_MIN_RESOLUTION: Minimum H3 resolution (default: 3)
  - H3_MAX_RESOLUTION: Maximum H3 resolution (default: 9)
"""

import os
import subprocess
import tempfile
from pathlib import Path

import duckdb

from common import (
    get_s3_client,
    get_s3_bucket,
    load_duckdb_extension,
    get_tile_bbox,
    require_env,
)

# Configuration
RUN_ID = os.environ.get("RUN_ID")
INPUT_PREFIX = os.environ.get("INPUT_PREFIX", "").lstrip("/")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "").lstrip("/")
SHARD_ID = os.environ.get("SHARD_ID")
SHARD_Z = os.environ.get("SHARD_Z")
SHARD_X = os.environ.get("SHARD_X")
SHARD_Y = os.environ.get("SHARD_Y")
PLANET_FILE = os.environ.get("PLANET_FILE")
H3_MIN_RESOLUTION = os.environ.get("H3_MIN_RESOLUTION")
H3_MAX_RESOLUTION = os.environ.get("H3_MAX_RESOLUTION")


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
    load_duckdb_extension(conn, "h3", "INSTALL h3 FROM community")

    # DuckDB H3 extension has had naming differences across versions.
    # Resolve the function name dynamically to keep the pipeline portable.
    h3_cell_to_string_fn = None
    for candidate in ("h3_cell_to_cell_string", "h3_cell_to_string"):
        exists = conn.execute(
            "SELECT 1 FROM duckdb_functions() WHERE function_name = ? LIMIT 1",
            [candidate],
        ).fetchone()
        if exists:
            h3_cell_to_string_fn = candidate
            break
    if h3_cell_to_string_fn is None:
        raise RuntimeError(
            "DuckDB H3 extension is missing a cell-to-string function; expected one of "
            "'h3_cell_to_cell_string' or 'h3_cell_to_string'."
        )
    print(f"DuckDB H3: using {h3_cell_to_string_fn}()")

    try:
        h3_min = int(H3_MIN_RESOLUTION) if H3_MIN_RESOLUTION is not None else 3
        h3_max = int(H3_MAX_RESOLUTION) if H3_MAX_RESOLUTION is not None else 9
    except ValueError:
        raise ValueError("H3_MIN_RESOLUTION and H3_MAX_RESOLUTION must be integers")

    h3_min = max(0, min(15, h3_min))
    h3_max = max(0, min(15, h3_max))
    if h3_min > h3_max:
        h3_min, h3_max = h3_max, h3_min

    h3_columns = []
    for res in range(h3_min, h3_max + 1):
        h3_columns.append(
            f"{h3_cell_to_string_fn}(h3_latlng_to_cell(ST_Y(centroid)::DOUBLE, ST_X(centroid)::DOUBLE, "
            f"{res})) as h3_r{res}"
        )
    h3_columns_sql = ",\n            ".join(h3_columns)

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
            {h3_columns_sql},
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


def main() -> None:
    """Process a single tile shard to Parquet."""
    require_env("RUN_ID", "INPUT_PREFIX", "OUTPUT_PREFIX", "S3_BUCKET", "SHARD_ID", "SHARD_Z", "SHARD_X", "SHARD_Y")

    s3 = get_s3_client()
    bucket = get_s3_bucket()

    print("=" * 60)
    print("PROCESS STAGE")
    print("=" * 60)
    print(f"Run ID: {RUN_ID}")
    print(f"Bucket: {bucket}")
    print(f"Input Prefix: {INPUT_PREFIX}")
    print(f"Output Prefix: {OUTPUT_PREFIX}")
    print(f"Shard: {SHARD_ID} (z/x/y {SHARD_Z}/{SHARD_X}/{SHARD_Y})")
    print()

    with tempfile.TemporaryDirectory() as tmpdir:
        work_dir = Path(tmpdir)

        # Download or use provided planet file
        planet_path = work_dir / "planet.osm.pbf"
        if PLANET_FILE:
            # Use provided planet file directly if it's a local path
            if Path(PLANET_FILE).exists():
                print(f"Using local planet file: {PLANET_FILE}")
                import shutil

                shutil.copy2(PLANET_FILE, planet_path)
            else:
                # Assume it's a S3 key
                print(f"Downloading planet file from S3: {PLANET_FILE}")
                s3.download_file(bucket, PLANET_FILE, str(planet_path))
        else:
            # Default to run-specific location
            planet_key = f"{INPUT_PREFIX}/planet.osm.pbf"
            print(f"Downloading s3://{bucket}/{planet_key}...")
            s3.download_file(bucket, planet_key, str(planet_path))

        print(f"Downloaded {planet_path.stat().st_size / (1024**3):.1f} GB")

        # Get tile bounding box for filtering
        bbox = get_tile_bbox(int(SHARD_Z), int(SHARD_X), int(SHARD_Y))
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
        parquet_path = process_to_parquet(geojson_path, SHARD_ID, work_dir)

        if parquet_path is None:
            print("No POIs found in this shard, skipping upload")
            # Write empty marker so merge knows this shard was processed
            marker_key = f"{OUTPUT_PREFIX}/shards/{SHARD_ID}/_EMPTY"
            s3.put_object(Bucket=bucket, Key=marker_key, Body=b"")
            return

        # Upload to S3
        storage_key = f"{OUTPUT_PREFIX}/shards/{SHARD_ID}/data.parquet"
        print(f"Uploading to s3://{bucket}/{storage_key}...")
        s3.upload_file(str(parquet_path), bucket, storage_key)
        print("Done!")


if __name__ == "__main__":
    main()
