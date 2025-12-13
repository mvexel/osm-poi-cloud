"""
Common utilities for OSM-H3 batch processing.
"""

import os
import sys
from pathlib import Path

import boto3
import duckdb


# ============================================================
# S3 Helpers
# ============================================================

def get_s3_client():
    """Get boto3 S3 client."""
    return boto3.client("s3")


def get_s3_bucket() -> str:
    """Get S3 bucket from environment."""
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise ValueError("S3_BUCKET environment variable is required")
    return bucket


# ============================================================
# Utilities
# ============================================================


def require_env(*names: str) -> None:
    """Validate required environment variables."""
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        sys.exit(1)


def configure_duckdb_extensions(conn: duckdb.DuckDBPyConnection) -> None:
    """Configure DuckDB extension directory if set."""
    extension_directory = os.environ.get("DUCKDB_EXTENSION_DIRECTORY")
    if extension_directory:
        conn.execute(f"SET extension_directory='{extension_directory}'")


def load_duckdb_extension(
    conn: duckdb.DuckDBPyConnection, name: str, install_sql: str
) -> None:
    """Load a DuckDB extension, installing if needed."""
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


def parse_h3_index_to_uint64(h3_index: str) -> int:
    """Parse H3 index string to uint64."""
    value = h3_index.strip().lower()
    if value.startswith("0x"):
        return int(value, 16)
    if any(c in "abcdef" for c in value):
        return int(value, 16)
    return int(value, 10)


def get_tile_bbox(z: int, x: int, y: int) -> dict:
    """Calculate bounding box for a Web Mercator tile."""
    import math

    n = 2**z
    west = x / n * 360.0 - 180.0
    east = (x + 1) / n * 360.0 - 180.0

    def tile_y_to_lat_deg(tile_y: int) -> float:
        lat_rad = math.atan(math.sinh(math.pi * (1 - 2 * tile_y / n)))
        return lat_rad * 180.0 / math.pi

    north = tile_y_to_lat_deg(y)
    south = tile_y_to_lat_deg(y + 1)

    return {"west": west, "south": south, "east": east, "north": north}
