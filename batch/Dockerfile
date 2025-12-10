# OSM POI Batch Processor
# Processes OSM extracts to Parquet with DuckDB spatial

FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.0

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    osmium-tool \
    curl \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Pre-install DuckDB spatial extension (avoids network call at runtime)
RUN python3 -c "import duckdb; duckdb.sql('INSTALL spatial')"

# Copy processing script
COPY process_region.py /app/process_region.py
RUN chmod +x /app/process_region.py

WORKDIR /app

# Default entrypoint
ENTRYPOINT ["python3", "/app/process_region.py"]
