#!/usr/bin/env python3
"""
AWS Batch job script for processing a single OSM region to Parquet.

Environment variables:
  - REGION_PATH: Geofabrik region path (e.g., "north-america/us/utah")
  - S3_BUCKET: Output S3 bucket name
"""

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import boto3
import duckdb


def download_pbf(region_path: str, output_dir: Path) -> Path:
    """Download PBF from Geofabrik."""
    region_slug = region_path.replace("/", "_")
    output_path = output_dir / f"{region_slug}-latest.osm.pbf"
    url = f"https://download.geofabrik.de/{region_path}-latest.osm.pbf"

    print(f"Downloading {url}...")
    subprocess.run(["curl", "-L", "-f", "-o", str(output_path), url], check=True)
    print(f"Downloaded {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    return output_path


def filter_pbf(input_pbf: Path, output_dir: Path) -> Path:
    """Filter PBF to POI-relevant features using osmium."""
    output_path = output_dir / f"{input_pbf.stem}-filtered.osm.pbf"

    print("Filtering PBF to POIs...")
    subprocess.run(
        [
            "osmium",
            "tags-filter",
            str(input_pbf),
            "nw/name",
            "-o",
            str(output_path) + ".named.osm.pbf",
        ],
        check=True,
    )

    subprocess.run(
        [
            "osmium",
            "tags-filter",
            str(output_path) + ".named.osm.pbf",
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

    (Path(str(output_path) + ".named.osm.pbf")).unlink(missing_ok=True)

    print(f"Filtered to {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    return output_path


def pbf_to_geojson(pbf_path: Path, output_dir: Path) -> Path:
    """Convert PBF to newline-delimited GeoJSON using osmium export."""
    output_path = output_dir / f"{pbf_path.stem}.ndjson"

    print("Converting to GeoJSON...")
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

    print(f"Converted to {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    return output_path


def process_geojson_to_parquet(
    geojson_path: Path, region_name: str, output_dir: Path
) -> Path:
    """Process GeoJSON to Parquet using DuckDB with spatial extension."""
    output_path = output_dir / f"{region_name}.parquet"

    print("Processing GeoJSON to Parquet with DuckDB...")

    conn = duckdb.connect()
    conn.sql("LOAD spatial;")

    # Debug: check record count
    count = conn.sql(f"""
        SELECT COUNT(*) FROM read_json('{geojson_path}',
            columns={{id: 'VARCHAR', type: 'VARCHAR', geometry: 'JSON', properties: 'JSON'}},
            maximum_object_size=10485760
        )
    """).fetchone()[0]
    print(f"  Total records in file: {count:,}")

    # SQL query with POI classification and centroid computation
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
                    -- Restaurant
                    WHEN amenity IN ('restaurant', 'food_court', 'diner', 'bbq') THEN 'restaurant'
                    -- Cafe/Bakery
                    WHEN amenity IN ('cafe', 'coffee_shop', 'tea') THEN 'cafe_bakery'
                    -- Bar/Pub
                    WHEN amenity IN ('bar', 'pub', 'biergarten') THEN 'bar_pub'
                    -- Fast Food
                    WHEN amenity IN ('fast_food', 'food_truck', 'ice_cream', 'street_vendor') THEN 'fast_food'
                    -- Ice Cream (shop takes precedence after amenity ice_cream caught above)
                    WHEN shop IN ('ice_cream', 'dessert', 'frozen_yogurt') THEN 'ice_cream'
                    -- Grocery
                    WHEN shop IN ('supermarket', 'convenience', 'grocery', 'marketplace') THEN 'grocery'
                    WHEN amenity = 'marketplace' THEN 'grocery'
                    -- Specialty Food
                    WHEN shop IN ('bakery', 'butcher', 'cheese', 'confectionery', 'chocolate',
                                  'deli', 'fishmonger', 'frozen_food', 'greengrocer',
                                  'health_food', 'organic', 'pastry', 'tea', 'coffee') THEN 'specialty_food'
                    -- Retail
                    WHEN shop IN ('mall', 'department_store', 'car', 'clothes', 'fashion',
                                  'shoes', 'electronics', 'computer', 'hardware', 'doityourself',
                                  'furniture', 'jewelry', 'toys', 'books', 'gift', 'cosmetics') THEN 'retail'
                    -- Personal Services
                    WHEN amenity IN ('spa', 'sauna', 'hairdresser', 'beauty_salon', 'laundry', 'dry_cleaning') THEN 'personal_services'
                    WHEN shop IN ('hairdresser', 'beauty', 'massage') THEN 'personal_services'
                    -- Professional Services
                    WHEN amenity IN ('coworking_space', 'conference_centre') THEN 'professional_services'
                    WHEN office IN ('company', 'lawyer', 'architect', 'estate_agent', 'accountant') THEN 'professional_services'
                    -- Finance
                    WHEN amenity IN ('bank', 'atm', 'bureau_de_change', 'money_transfer') THEN 'finance'
                    -- Lodging
                    WHEN tourism IN ('hotel', 'guest_house', 'hostel', 'motel', 'apartment',
                                     'chalet', 'alpine_hut', 'camp_site', 'caravan_site') THEN 'lodging'
                    -- Transport
                    WHEN amenity IN ('bus_station', 'ferry_terminal') THEN 'transport'
                    WHEN railway IN ('station', 'halt', 'stop', 'tram_stop') THEN 'transport'
                    WHEN public_transport = 'station' THEN 'transport'
                    WHEN aeroway IN ('aerodrome', 'terminal') THEN 'transport'
                    -- Auto Services
                    WHEN amenity IN ('fuel', 'charging_station', 'car_wash', 'car_rental', 'car_repair') THEN 'auto_services'
                    WHEN shop IN ('car_repair', 'tyres') THEN 'auto_services'
                    -- Parking
                    WHEN amenity IN ('parking', 'bicycle_parking', 'motorcycle_parking') THEN 'parking'
                    -- Healthcare
                    WHEN amenity IN ('hospital', 'clinic', 'doctors', 'dentist', 'pharmacy', 'ambulance_station') THEN 'healthcare'
                    WHEN healthcare IS NOT NULL THEN 'healthcare'
                    -- Education
                    WHEN amenity IN ('school', 'kindergarten', 'college', 'university',
                                     'music_school', 'language_school', 'library') THEN 'education'
                    -- Government
                    WHEN amenity IN ('townhall', 'courthouse', 'police', 'fire_station',
                                     'post_office', 'embassy') THEN 'government'
                    WHEN office = 'government' THEN 'government'
                    -- Community
                    WHEN amenity IN ('community_centre', 'social_centre', 'youth_centre',
                                     'social_facility', 'shelter') THEN 'community'
                    -- Religious
                    WHEN amenity IN ('place_of_worship', 'church', 'mosque', 'temple', 'synagogue') THEN 'religious'
                    -- Culture
                    WHEN tourism IN ('museum', 'gallery') THEN 'culture'
                    WHEN amenity IN ('arts_centre', 'theatre', 'concert_hall', 'planetarium') THEN 'culture'
                    -- Entertainment
                    WHEN amenity IN ('cinema', 'nightclub', 'casino', 'bowling_alley', 'amusement_arcade') THEN 'entertainment'
                    WHEN leisure IN ('bowling_alley', 'escape_game') THEN 'entertainment'
                    -- Sports/Fitness
                    WHEN leisure IN ('sports_centre', 'fitness_centre', 'gym', 'swimming_pool',
                                     'stadium', 'pitch', 'ice_rink', 'golf_course') THEN 'sports_fitness'
                    -- Parks/Outdoors
                    WHEN leisure IN ('park', 'garden', 'nature_reserve', 'playground', 'dog_park') THEN 'parks_outdoors'
                    WHEN tourism IN ('picnic_site', 'viewpoint') THEN 'parks_outdoors'
                    WHEN "natural" = 'beach' THEN 'parks_outdoors'
                    -- Landmark
                    WHEN tourism IN ('attraction', 'information') THEN 'landmark'
                    WHEN historic IN ('monument', 'memorial', 'castle', 'ruins') THEN 'landmark'
                    WHEN man_made IN ('lighthouse', 'tower') THEN 'landmark'
                    -- Animal Services
                    WHEN amenity IN ('veterinary', 'animal_boarding', 'animal_shelter') THEN 'animal_services'
                    WHEN shop = 'pet' THEN 'animal_services'
                    -- Fallbacks
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
            '{region_name}' as state,
            amenity,
            shop,
            leisure,
            tourism,
            cuisine,
            opening_hours,
            phone,
            website,
            brand,
            "operator",
            FLOOR(ST_X(centroid))::INTEGER as lon_bucket,
            FLOOR(ST_Y(centroid))::INTEGER as lat_bucket
        FROM classified
        WHERE class IS NOT NULL
    ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """

    conn.sql(query)

    # Get stats
    stats = conn.sql(f"""
        SELECT
            COUNT(*) as total,
            COUNT(DISTINCT class) as classes
        FROM read_parquet('{output_path}')
    """).fetchone()

    print(f"Total POIs: {stats[0]:,}")
    print(f"Unique classes: {stats[1]}")

    # Show class breakdown
    class_counts = conn.sql(f"""
        SELECT class, COUNT(*) as cnt
        FROM read_parquet('{output_path}')
        GROUP BY class
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()

    print("Top classes:")
    for cls, cnt in class_counts:
        print(f"  {cls}: {cnt:,}")

    file_size = output_path.stat().st_size / 1024 / 1024
    print(f"Wrote {output_path} ({file_size:.1f} MB)")

    conn.close()

    if stats[0] == 0:
        print("WARNING: No POIs found!")
        return None

    return output_path


def upload_to_s3(local_path: Path, bucket: str, region_name: str) -> str:
    """Upload Parquet file to S3."""
    s3_key = f"parquet/{region_name}.parquet"

    print(f"Uploading to s3://{bucket}/{s3_key}...")
    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), bucket, s3_key)

    return f"s3://{bucket}/{s3_key}"


def main():
    region_path = os.environ.get("REGION_PATH")
    if not region_path:
        print("ERROR: REGION_PATH environment variable required")
        sys.exit(1)

    s3_bucket = os.environ.get("S3_BUCKET")
    if not s3_bucket:
        print("ERROR: S3_BUCKET environment variable required")
        sys.exit(1)

    # Derive region name
    region_name = region_path.split("/")[-1]

    print(f"Processing region: {region_path} -> {region_name}")
    print(f"Output bucket: {s3_bucket}")

    with tempfile.TemporaryDirectory() as tmpdir:
        work_dir = Path(tmpdir)

        pbf_path = download_pbf(region_path, work_dir)
        filtered_pbf = filter_pbf(pbf_path, work_dir)
        geojson_path = pbf_to_geojson(filtered_pbf, work_dir)
        parquet_path = process_geojson_to_parquet(geojson_path, region_name, work_dir)

        if parquet_path is None:
            print("WARNING: No POIs found, nothing to upload")
            sys.exit(0)

        s3_uri = upload_to_s3(parquet_path, s3_bucket, region_name)

        print(f"\nDone! Output: {s3_uri}")


if __name__ == "__main__":
    main()
