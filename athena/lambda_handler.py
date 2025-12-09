"""
Lambda function for querying POIs via Athena.

Endpoints:
  GET /pois?bbox=minLon,minLat,maxLon,maxLat[&class=restaurant][&limit=1000]
  GET /classes
  GET /health
"""

import json
import math
import os
import time

import boto3

ATHENA_DATABASE = os.environ.get("ATHENA_DATABASE", "osm_pois")
ATHENA_TABLE = os.environ.get("ATHENA_TABLE", "pois")
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT")  # s3://bucket/athena-results/
ATHENA_WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "primary")

athena = boto3.client("athena")


def lambda_handler(event, context):
    """Main Lambda handler for API Gateway proxy integration."""
    path = event.get("path", "/")
    method = event.get("httpMethod", "GET")
    params = event.get("queryStringParameters") or {}

    # CORS headers
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Content-Type": "application/json",
    }

    # Handle OPTIONS for CORS
    if method == "OPTIONS":
        return {"statusCode": 200, "headers": headers, "body": ""}

    try:
        if path == "/health":
            return {
                "statusCode": 200,
                "headers": headers,
                "body": json.dumps({"status": "healthy", "database": ATHENA_DATABASE}),
            }

        if path == "/classes":
            return handle_classes(headers)

        if path == "/pois":
            return handle_pois(params, headers)

        return {
            "statusCode": 404,
            "headers": headers,
            "body": json.dumps({"error": "Not found"}),
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": headers,
            "body": json.dumps({"error": str(e)}),
        }


def handle_classes(headers):
    """Return list of available POI classes."""
    query = f"""
        SELECT DISTINCT class, COUNT(*) as count
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        GROUP BY class
        ORDER BY count DESC
    """
    results = run_athena_query(query)

    classes = [{"class": row[0], "count": int(row[1])} for row in results]

    return {
        "statusCode": 200,
        "headers": headers,
        "body": json.dumps({"classes": classes}),
    }


def handle_pois(params, headers):
    """Return POIs within a bounding box."""
    bbox = params.get("bbox")
    if not bbox:
        return {
            "statusCode": 400,
            "headers": headers,
            "body": json.dumps(
                {"error": "bbox parameter required (minLon,minLat,maxLon,maxLat)"}
            ),
        }

    try:
        min_lon, min_lat, max_lon, max_lat = map(float, bbox.split(","))
    except (ValueError, AttributeError):
        return {
            "statusCode": 400,
            "headers": headers,
            "body": json.dumps(
                {"error": "Invalid bbox format. Use: minLon,minLat,maxLon,maxLat"}
            ),
        }

    # Validate bbox
    if min_lon > max_lon or min_lat > max_lat:
        return {
            "statusCode": 400,
            "headers": headers,
            "body": json.dumps(
                {"error": "Invalid bbox: min values must be less than max values"}
            ),
        }

    # Limit bbox size to prevent huge queries
    if (max_lon - min_lon) > 5 or (max_lat - min_lat) > 5:
        return {
            "statusCode": 400,
            "headers": headers,
            "body": json.dumps(
                {"error": "Bbox too large. Max 5 degrees on each side."}
            ),
        }

    poi_class = params.get("class")
    limit = min(int(params.get("limit", 1000)), 10000)

    # Calculate bucket ranges for partition pruning
    min_lon_bucket = math.floor(min_lon)
    max_lon_bucket = math.floor(max_lon)
    min_lat_bucket = math.floor(min_lat)
    max_lat_bucket = math.floor(max_lat)

    # Build query with partition pruning
    query = f"""
        SELECT osm_id, osm_type, name, class, lon, lat, state,
               amenity, shop, leisure, tourism, cuisine, opening_hours,
               phone, website, brand, operator, tags
        FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE lon_bucket BETWEEN {min_lon_bucket} AND {max_lon_bucket}
          AND lat_bucket BETWEEN {min_lat_bucket} AND {max_lat_bucket}
          AND lon BETWEEN {min_lon} AND {max_lon}
          AND lat BETWEEN {min_lat} AND {max_lat}
    """

    if poi_class:
        query += f" AND class = '{poi_class}'"

    query += f" LIMIT {limit}"

    results = run_athena_query(query)

    # Convert to GeoJSON
    features = []
    for row in results:
        (
            osm_id,
            osm_type,
            name,
            poi_class,
            lon,
            lat,
            state,
            amenity,
            shop,
            leisure,
            tourism,
            cuisine,
            opening_hours,
            phone,
            website,
            brand,
            operator,
            tags,
        ) = row

        properties = {
            "osm_id": osm_id,
            "osm_type": osm_type,
            "name": name,
            "class": poi_class,
            "state": state,
        }

        # Add optional fields if present
        for key, val in [
            ("amenity", amenity),
            ("shop", shop),
            ("leisure", leisure),
            ("tourism", tourism),
            ("cuisine", cuisine),
            ("opening_hours", opening_hours),
            ("phone", phone),
            ("website", website),
            ("brand", brand),
            ("operator", operator),
        ]:
            if val:
                properties[key] = val

        if tags:
            try:
                properties["tags"] = json.loads(tags)
            except json.JSONDecodeError:
                pass

        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [float(lon), float(lat)],
                },
                "properties": properties,
            }
        )

    return {
        "statusCode": 200,
        "headers": headers,
        "body": json.dumps(
            {
                "type": "FeatureCollection",
                "features": features,
                "count": len(features),
                "bbox": [min_lon, min_lat, max_lon, max_lat],
            }
        ),
    }


def run_athena_query(query, timeout=30):
    """Execute Athena query and wait for results."""
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup=ATHENA_WORKGROUP,
    )

    query_id = response["QueryExecutionId"]

    # Poll for completion
    start = time.time()
    while time.time() - start < timeout:
        status = athena.get_query_execution(QueryExecutionId=query_id)
        state = status["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            break
        elif state in ("FAILED", "CANCELLED"):
            reason = status["QueryExecution"]["Status"].get(
                "StateChangeReason", "Unknown"
            )
            raise Exception(f"Query {state}: {reason}")

        time.sleep(0.5)
    else:
        raise Exception("Query timeout")

    # Get results
    results = []
    paginator = athena.get_paginator("get_query_results")

    for page in paginator.paginate(QueryExecutionId=query_id):
        for row in page["ResultSet"]["Rows"][1:]:  # Skip header
            results.append([col.get("VarCharValue") for col in row["Data"]])

    return results
