#!/bin/bash
set -euo pipefail

# Environment variables:
# - RUN_ID: Pipeline run identifier
# - S3_BUCKET: S3 bucket for input/output
# - MAX_ZOOM: Max Web Mercator zoom (optional; defaults handled by the sharder binary)
# - MAX_NODES_PER_SHARD: Max nodes per shard (optional; defaults handled by the sharder binary)

echo "========================================"
echo "OSM-H3 Sharder"
echo "========================================"
echo "Run ID: ${RUN_ID:-not set}"
echo "S3 Bucket: ${S3_BUCKET:-not set}"
echo "Max Zoom (MAX_ZOOM): ${MAX_ZOOM:-<unset>}"
echo "Max Nodes Per Shard (MAX_NODES_PER_SHARD): ${MAX_NODES_PER_SHARD:-<unset>}"
echo ""

# Validate required env vars
if [ -z "${RUN_ID:-}" ] || [ -z "${S3_BUCKET:-}" ]; then
    echo "ERROR: RUN_ID and S3_BUCKET are required"
    exit 1
fi

# Download planet file from S3 (prefer authenticated AWS CLI).
PLANET_KEY="runs/${RUN_ID}/planet.osm.pbf"
PLANET_PATH="/data/planet.osm.pbf"

echo "Downloading s3://${S3_BUCKET}/${PLANET_KEY}..."
if ! aws s3 cp "s3://${S3_BUCKET}/${PLANET_KEY}" "${PLANET_PATH}"; then
    echo "aws s3 cp failed, falling back to HTTPS..."
    # -f makes curl exit nonzero on 4xx/5xx so we don't accept AccessDenied XML.
    curl -sS -L -f "https://${S3_BUCKET}.s3.amazonaws.com/${PLANET_KEY}" -o "${PLANET_PATH}"
fi

SIZE_BYTES=$(stat -c%s "${PLANET_PATH}" 2>/dev/null || stat -f%z "${PLANET_PATH}")
if [ "${SIZE_BYTES}" -lt 10485760 ]; then
    echo "ERROR: Downloaded file is too small (${SIZE_BYTES} bytes)."
    echo "Likely missing permissions or incomplete upload."
    exit 1
fi

echo "Downloaded $(du -h ${PLANET_PATH} | cut -f1)"

# Run the sharder
echo ""
echo "Running sharder..."
osm-planet-sharding \
    "${PLANET_PATH}" \
    --s3-bucket "${S3_BUCKET}" \
    --run-id "${RUN_ID}"

# Cleanup
rm -f "${PLANET_PATH}"

echo ""
echo "Sharding complete!"
