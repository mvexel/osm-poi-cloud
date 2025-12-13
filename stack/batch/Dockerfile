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

# Pre-install DuckDB extensions into a shared directory (avoids network calls at runtime)
ENV DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions
RUN mkdir -p "${DUCKDB_EXTENSION_DIRECTORY}" && \
    python3 - <<'PY'
import os
import duckdb

conn = duckdb.connect()
conn.execute(f"SET extension_directory='{os.environ['DUCKDB_EXTENSION_DIRECTORY']}'")
conn.execute("INSTALL spatial")
conn.execute("INSTALL h3 FROM community")
conn.close()
PY

# Copy processing scripts
COPY common.py /app/common.py
COPY download.py /app/download.py
COPY process.py /app/process.py
COPY merge.py /app/merge.py
COPY processor.py /app/processor.py

WORKDIR /app

# Default entrypoint
# For backward compatibility, processor.py dispatches to the individual scripts
# You can also invoke the scripts directly: python3 /app/download.py, etc.
ENTRYPOINT ["python3", "/app/processor.py"]
