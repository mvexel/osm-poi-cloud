# OSM-H3 Batch Processor
# Multi-stage processor for download, shard processing, and merge

FROM ghcr.io/osgeo/gdal:ubuntu-small-3.8.0

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    osmium-tool \
    curl \
    aria2 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Pre-install DuckDB extensions (avoids network calls at runtime)
RUN python3 -c "import duckdb; conn = duckdb.connect(); conn.sql('INSTALL spatial'); conn.sql('INSTALL h3 FROM community')"

# Copy processing scripts
COPY processor.py /app/processor.py
COPY process_region.py /app/process_region.py
RUN chmod +x /app/processor.py /app/process_region.py

WORKDIR /app

# Default entrypoint - uses STAGE env var to determine behavior
ENTRYPOINT ["python3", "/app/processor.py"]
