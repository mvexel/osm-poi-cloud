# OSM-H3 Batch Processor
# Multi-stage processor for download, shard processing, and merge

FROM python:3.13-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    osmium-tool \
    curl \
    aria2 \
    && rm -rf /var/lib/apt/lists/*

# copy UV and app code
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY . /app
WORKDIR /app

# Install Python dependencies
ENV UV_NO_DEV=1
RUN uv sync --locked

# Pre-install DuckDB extensions into a shared directory (avoids network calls at runtime)
ENV DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions
RUN mkdir -p "${DUCKDB_EXTENSION_DIRECTORY}" && \
    uv run python3 - <<'PY'
import os
import duckdb

conn = duckdb.connect()
conn.execute(f"SET extension_directory='{os.environ['DUCKDB_EXTENSION_DIRECTORY']}'")
conn.execute("INSTALL spatial")
conn.execute("INSTALL h3 FROM community")
conn.close()
PY


# Default entrypoint
# For backward compatibility, processor.py dispatches to the individual scripts
# You can also invoke the scripts directly: python3 /app/download.py, etc.
ENTRYPOINT ["python3", "/app/processor.py"]
