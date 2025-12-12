#!/bin/bash
set -euo pipefail

# Environment variables:
# - RUN_ID: Pipeline run identifier
# - S3_BUCKET: S3 bucket for input/output
# - MAX_RESOLUTION: H3 resolution (default: 7)
# - MAX_NODES_PER_SHARD: Max nodes per shard (default: 5000000)

echo "========================================"
echo "OSM-H3 Sharder"
echo "========================================"
echo "Run ID: ${RUN_ID:-not set}"
echo "S3 Bucket: ${S3_BUCKET:-not set}"
echo "Max Resolution: ${MAX_RESOLUTION:-7}"
echo "Max Nodes Per Shard: ${MAX_NODES_PER_SHARD:-5000000}"
echo ""

# Validate required env vars
if [ -z "${RUN_ID:-}" ] || [ -z "${S3_BUCKET:-}" ]; then
    echo "ERROR: RUN_ID and S3_BUCKET are required"
    exit 1
fi

# Download planet file from S3
PLANET_KEY="runs/${RUN_ID}/planet.osm.pbf"
PLANET_PATH="/data/planet.osm.pbf"

echo "Downloading s3://${S3_BUCKET}/${PLANET_KEY}..."
curl -s "https://${S3_BUCKET}.s3.amazonaws.com/${PLANET_KEY}" -o "${PLANET_PATH}" || \
    aws s3 cp "s3://${S3_BUCKET}/${PLANET_KEY}" "${PLANET_PATH}"

echo "Downloaded $(du -h ${PLANET_PATH} | cut -f1)"

# Run the sharder
echo ""
echo "Running sharder..."
osm-planet-sharding \
    "${PLANET_PATH}" \
    "${MAX_RESOLUTION:-7}" \
    "${MAX_NODES_PER_SHARD:-5000000}" \
    --s3-bucket "${S3_BUCKET}" \
    --run-id "${RUN_ID}"

# Cleanup
rm -f "${PLANET_PATH}"

echo ""
echo "Sharding complete!"
