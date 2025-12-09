#!/usr/bin/env python3
"""
AWS Batch job script for processing a single OSM region to Parquet.

Environment variables:
  - REGION_PATH: Geofabrik region path (e.g., "north-america/us/utah")
  - S3_BUCKET: Output S3 bucket name
"""

import json
import math
import os
import subprocess
import sys
import tempfile
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

# POI category mapping (extracted from lua/poi_mapping.lua)
POI_MAPPING = [
    {
        "class": "restaurant",
        "tags": [
            ("amenity", "restaurant"),
            ("amenity", "food_court"),
            ("amenity", "diner"),
            ("amenity", "bbq"),
        ],
    },
    {
        "class": "cafe_bakery",
        "tags": [("amenity", "cafe"), ("amenity", "coffee_shop"), ("amenity", "tea")],
    },
    {
        "class": "bar_pub",
        "tags": [("amenity", "bar"), ("amenity", "pub"), ("amenity", "biergarten")],
    },
    {
        "class": "fast_food",
        "tags": [
            ("amenity", "fast_food"),
            ("amenity", "food_truck"),
            ("amenity", "ice_cream"),
            ("amenity", "street_vendor"),
        ],
    },
    {
        "class": "ice_cream",
        "tags": [
            ("amenity", "ice_cream"),
            ("shop", "ice_cream"),
            ("shop", "dessert"),
            ("shop", "frozen_yogurt"),
        ],
    },
    {
        "class": "grocery",
        "tags": [
            ("shop", "supermarket"),
            ("shop", "convenience"),
            ("shop", "grocery"),
            ("shop", "marketplace"),
            ("amenity", "marketplace"),
        ],
    },
    {
        "class": "specialty_food",
        "tags": [
            ("shop", "bakery"),
            ("shop", "butcher"),
            ("shop", "cheese"),
            ("shop", "confectionery"),
            ("shop", "chocolate"),
            ("shop", "deli"),
            ("shop", "fishmonger"),
            ("shop", "frozen_food"),
            ("shop", "greengrocer"),
            ("shop", "health_food"),
            ("shop", "organic"),
            ("shop", "pastry"),
            ("shop", "tea"),
            ("shop", "coffee"),
        ],
    },
    {
        "class": "retail",
        "tags": [
            ("shop", "mall"),
            ("shop", "department_store"),
            ("shop", "car"),
            ("shop", "clothes"),
            ("shop", "fashion"),
            ("shop", "shoes"),
            ("shop", "electronics"),
            ("shop", "computer"),
            ("shop", "hardware"),
            ("shop", "doityourself"),
            ("shop", "furniture"),
            ("shop", "jewelry"),
            ("shop", "toys"),
            ("shop", "books"),
            ("shop", "gift"),
            ("shop", "cosmetics"),
        ],
    },
    {
        "class": "personal_services",
        "tags": [
            ("amenity", "spa"),
            ("amenity", "sauna"),
            ("amenity", "hairdresser"),
            ("amenity", "beauty_salon"),
            ("amenity", "laundry"),
            ("amenity", "dry_cleaning"),
            ("shop", "hairdresser"),
            ("shop", "beauty"),
            ("shop", "massage"),
        ],
    },
    {
        "class": "professional_services",
        "tags": [
            ("amenity", "coworking_space"),
            ("amenity", "conference_centre"),
            ("office", "company"),
            ("office", "lawyer"),
            ("office", "architect"),
            ("office", "estate_agent"),
            ("office", "accountant"),
        ],
    },
    {
        "class": "finance",
        "tags": [
            ("amenity", "bank"),
            ("amenity", "atm"),
            ("amenity", "bureau_de_change"),
            ("amenity", "money_transfer"),
        ],
    },
    {
        "class": "lodging",
        "tags": [
            ("tourism", "hotel"),
            ("tourism", "guest_house"),
            ("tourism", "hostel"),
            ("tourism", "motel"),
            ("tourism", "apartment"),
            ("tourism", "chalet"),
            ("tourism", "alpine_hut"),
            ("tourism", "camp_site"),
            ("tourism", "caravan_site"),
        ],
    },
    {
        "class": "transport",
        "tags": [
            ("amenity", "bus_station"),
            ("railway", "station"),
            ("railway", "halt"),
            ("railway", "stop"),
            ("railway", "tram_stop"),
            ("public_transport", "station"),
            ("aeroway", "aerodrome"),
            ("aeroway", "terminal"),
            ("amenity", "ferry_terminal"),
        ],
    },
    {
        "class": "auto_services",
        "tags": [
            ("amenity", "fuel"),
            ("amenity", "charging_station"),
            ("amenity", "car_wash"),
            ("amenity", "car_rental"),
            ("amenity", "car_repair"),
            ("shop", "car_repair"),
            ("shop", "tyres"),
        ],
    },
    {
        "class": "parking",
        "tags": [
            ("amenity", "parking"),
            ("amenity", "bicycle_parking"),
            ("amenity", "motorcycle_parking"),
        ],
    },
    {
        "class": "healthcare",
        "tags": [
            ("amenity", "hospital"),
            ("amenity", "clinic"),
            ("amenity", "doctors"),
            ("amenity", "dentist"),
            ("amenity", "pharmacy"),
            ("amenity", "ambulance_station"),
        ],
    },
    {
        "class": "education",
        "tags": [
            ("amenity", "school"),
            ("amenity", "kindergarten"),
            ("amenity", "college"),
            ("amenity", "university"),
            ("amenity", "music_school"),
            ("amenity", "language_school"),
            ("amenity", "library"),
        ],
    },
    {
        "class": "government",
        "tags": [
            ("amenity", "townhall"),
            ("amenity", "courthouse"),
            ("amenity", "police"),
            ("amenity", "fire_station"),
            ("amenity", "post_office"),
            ("amenity", "embassy"),
            ("office", "government"),
        ],
    },
    {
        "class": "community",
        "tags": [
            ("amenity", "community_centre"),
            ("amenity", "social_centre"),
            ("amenity", "youth_centre"),
            ("amenity", "social_facility"),
            ("amenity", "shelter"),
        ],
    },
    {
        "class": "religious",
        "tags": [
            ("amenity", "place_of_worship"),
            ("amenity", "church"),
            ("amenity", "mosque"),
            ("amenity", "temple"),
            ("amenity", "synagogue"),
        ],
    },
    {
        "class": "culture",
        "tags": [
            ("tourism", "museum"),
            ("tourism", "gallery"),
            ("amenity", "arts_centre"),
            ("amenity", "theatre"),
            ("amenity", "concert_hall"),
            ("amenity", "planetarium"),
        ],
    },
    {
        "class": "entertainment",
        "tags": [
            ("amenity", "cinema"),
            ("amenity", "nightclub"),
            ("amenity", "casino"),
            ("amenity", "bowling_alley"),
            ("amenity", "amusement_arcade"),
            ("leisure", "bowling_alley"),
            ("leisure", "escape_game"),
        ],
    },
    {
        "class": "sports_fitness",
        "tags": [
            ("leisure", "sports_centre"),
            ("leisure", "fitness_centre"),
            ("leisure", "gym"),
            ("leisure", "swimming_pool"),
            ("leisure", "stadium"),
            ("leisure", "pitch"),
            ("leisure", "ice_rink"),
            ("leisure", "golf_course"),
        ],
    },
    {
        "class": "parks_outdoors",
        "tags": [
            ("leisure", "park"),
            ("leisure", "garden"),
            ("leisure", "nature_reserve"),
            ("leisure", "playground"),
            ("leisure", "dog_park"),
            ("tourism", "picnic_site"),
            ("tourism", "viewpoint"),
            ("natural", "beach"),
        ],
    },
    {
        "class": "landmark",
        "tags": [
            ("tourism", "attraction"),
            ("tourism", "information"),
            ("historic", "monument"),
            ("historic", "memorial"),
            ("historic", "castle"),
            ("historic", "ruins"),
            ("man_made", "lighthouse"),
            ("man_made", "tower"),
        ],
    },
    {
        "class": "animal_services",
        "tags": [
            ("amenity", "veterinary"),
            ("amenity", "animal_boarding"),
            ("amenity", "animal_shelter"),
            ("shop", "pet"),
        ],
    },
]

# Build lookup for fast classification
POI_LOOKUP = {}
for cat in POI_MAPPING:
    for key, value in cat["tags"]:
        if key not in POI_LOOKUP:
            POI_LOOKUP[key] = []
        POI_LOOKUP[key].append((value, cat["class"]))


def classify_poi(tags: dict) -> str | None:
    """Classify a POI based on its OSM tags."""
    for key in [
        "amenity",
        "shop",
        "leisure",
        "tourism",
        "office",
        "railway",
        "aeroway",
        "historic",
        "man_made",
        "natural",
        "public_transport",
    ]:
        if key in tags and key in POI_LOOKUP:
            value = tags[key]
            for expected_value, poi_class in POI_LOOKUP[key]:
                if value == expected_value:
                    return poi_class

    # Fallback to wildcard matches
    if "shop" in tags:
        return "retail"
    if "healthcare" in tags:
        return "healthcare"

    # Generic fallback
    if any(k in tags for k in ["amenity", "shop", "leisure", "tourism"]):
        return "misc"

    return None


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
    """Convert PBF to GeoJSON using osmium export."""
    output_path = output_dir / f"{pbf_path.stem}.geojsonseq"

    print("Converting to GeoJSON...")
    # Export all geometry types - we'll compute centroids for polygons in Python
    subprocess.run(
        [
            "osmium",
            "export",
            str(pbf_path),
            "-o",
            str(output_path),
            "-f",
            "geojsonseq",
            "-u",
            "type_id",
        ],
        check=True,
    )

    print(f"Converted to {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"file: {output_path}")

    return output_path


def process_geojson_to_parquet(
    geojson_path: Path, region_name: str, output_dir: Path
) -> Path:
    """Process GeoJSON to Parquet with partition columns for Athena."""
    output_path = output_dir / f"{region_name}.parquet"

    print("Processing GeoJSON to Parquet...")

    # Collect all records
    records = []
    total_pois = 0
    skipped_no_name = 0
    skipped_no_class = 0
    skipped_no_geom = 0

    with open(geojson_path, "r") as f:
        # Debug: print first few lines to see format
        first_lines = []
        for i, line in enumerate(f):
            if i < 3:
                first_lines.append(line[:500])
            else:
                break
        print(f"  First few lines of GeoJSON:")
        for line in first_lines:
            print(f"    {line[:200]}...")

        # Reset file position
        f.seek(0)

        for line_num, line in enumerate(f):
            if not line.lstrip("\x1e").strip():
                continue

            try:
                feature = json.loads(line.lstrip("\x1e"))
            except json.JSONDecodeError:
                print(f"  Warning: Could not parse line {line_num + 1}, skipping")
                continue

            props = feature.get("properties", {})
            geom = feature.get("geometry")

            if not geom:
                skipped_no_geom += 1
                continue

            if not props.get("name"):
                skipped_no_name += 1
                continue

            # Get coordinates - compute centroid for non-point geometries
            geom_type = geom["type"]
            if geom_type == "Point":
                lon, lat = geom["coordinates"]
            elif geom_type == "Polygon":
                # Compute centroid of first ring
                coords = geom["coordinates"][0]
                lon = sum(c[0] for c in coords) / len(coords)
                lat = sum(c[1] for c in coords) / len(coords)
            elif geom_type == "LineString":
                # Use midpoint
                coords = geom["coordinates"]
                mid = len(coords) // 2
                lon, lat = coords[mid]
            elif geom_type == "MultiPolygon":
                # Use centroid of first polygon
                coords = geom["coordinates"][0][0]
                lon = sum(c[0] for c in coords) / len(coords)
                lat = sum(c[1] for c in coords) / len(coords)
            else:
                continue

            poi_class = classify_poi(props)
            if not poi_class:
                skipped_no_class += 1
                continue

            # Build record with partition columns
            # lon_bucket and lat_bucket for efficient bbox queries
            lon_bucket = math.floor(lon)
            lat_bucket = math.floor(lat)

            tags_to_store = {
                k: v
                for k, v in props.items()
                if not k.startswith("@")
                and k
                not in [
                    "name",
                    "amenity",
                    "shop",
                    "leisure",
                    "tourism",
                    "cuisine",
                    "opening_hours",
                    "phone",
                    "website",
                    "brand",
                    "operator",
                ]
            }

            record = {
                "osm_id": str(props.get("@id", "")),
                "osm_type": props.get("@type", "node"),
                "name": props.get("name"),
                "class": poi_class,
                "lon": lon,
                "lat": lat,
                "state": region_name,
                "amenity": props.get("amenity"),
                "shop": props.get("shop"),
                "leisure": props.get("leisure"),
                "tourism": props.get("tourism"),
                "cuisine": props.get("cuisine"),
                "opening_hours": props.get("opening_hours"),
                "phone": props.get("phone"),
                "website": props.get("website"),
                "brand": props.get("brand"),
                "operator": props.get("operator"),
                "tags": json.dumps(tags_to_store) if tags_to_store else None,
                # Partition columns
                "lon_bucket": lon_bucket,
                "lat_bucket": lat_bucket,
            }
            records.append(record)
            total_pois += 1

            if total_pois % 50000 == 0:
                print(f"  Processed {total_pois:,} POIs...")

    print(f"Total POIs: {total_pois:,}")
    print(f"  Skipped - no geometry: {skipped_no_geom:,}")
    print(f"  Skipped - no name: {skipped_no_name:,}")
    print(f"  Skipped - no class: {skipped_no_class:,}")

    if not records:
        print("WARNING: No POIs found!")
        return None

    # Define schema
    schema = pa.schema([
        ("osm_id", pa.string()),
        ("osm_type", pa.string()),
        ("name", pa.string()),
        ("class", pa.string()),
        ("lon", pa.float64()),
        ("lat", pa.float64()),
        ("state", pa.string()),
        ("amenity", pa.string()),
        ("shop", pa.string()),
        ("leisure", pa.string()),
        ("tourism", pa.string()),
        ("cuisine", pa.string()),
        ("opening_hours", pa.string()),
        ("phone", pa.string()),
        ("website", pa.string()),
        ("brand", pa.string()),
        ("operator", pa.string()),
        ("tags", pa.string()),
        ("lon_bucket", pa.int32()),
        ("lat_bucket", pa.int32()),
    ])

    # Create table from records
    table = pa.Table.from_pylist(records, schema=schema)

    # Write parquet file
    pq.write_table(table, output_path, compression="snappy")

    print(f"Wrote {output_path} ({output_path.stat().st_size / 1024 / 1024:.1f} MB)")
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
