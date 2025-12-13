#!/bin/bash
set -euo pipefail

# Environment variables:
# - RUN_ID: Pipeline run identifier
# - S3_BUCKET: S3 bucket for input/output
# - INPUT_PREFIX: S3 prefix for input files (e.g., /run/<run_id>)
# - OUTPUT_PREFIX: S3 prefix for output files (e.g., /run/<run_id>)
# - MAX_ZOOM: Max Web Mercator zoom (optional; defaults handled by the sharder binary)
# - MAX_NODES_PER_SHARD: Max nodes per shard (optional; defaults handled by the sharder binary)

echo "========================================"
echo "OSM-H3 Sharder"
echo "========================================"
echo "Run ID: ${RUN_ID:-not set}"
echo "S3 Bucket: ${S3_BUCKET:-not set}"
echo "Input Prefix: ${INPUT_PREFIX:-not set}"
echo "Output Prefix: ${OUTPUT_PREFIX:-not set}"
echo "Max Zoom (MAX_ZOOM): ${MAX_ZOOM:-<unset>}"
echo "Max Nodes Per Shard (MAX_NODES_PER_SHARD): ${MAX_NODES_PER_SHARD:-<unset>}"
echo ""

# Validate required env vars
if [ -z "${RUN_ID:-}" ] || [ -z "${S3_BUCKET:-}" ] || [ -z "${INPUT_PREFIX:-}" ] || [ -z "${OUTPUT_PREFIX:-}" ]; then
    echo "ERROR: RUN_ID, S3_BUCKET, INPUT_PREFIX, and OUTPUT_PREFIX are required"
    exit 1
fi

# Download planet file from S3 (prefer authenticated AWS CLI).
PLANET_KEY="${INPUT_PREFIX#/}/planet.osm.pbf"
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

# Run the sharder (outputs GeoJSON to stdout)
echo ""
echo "Running sharder..."
MANIFEST_PATH="/data/manifest.json"
osm-planet-sharding "${PLANET_PATH}" > "${MANIFEST_PATH}"

# Upload manifest to S3
MANIFEST_KEY="${OUTPUT_PREFIX#/}/shards/manifest.json"
echo ""
echo "Uploading manifest to s3://${S3_BUCKET}/${MANIFEST_KEY}..."
aws s3 cp "${MANIFEST_PATH}" "s3://${S3_BUCKET}/${MANIFEST_KEY}"

# Cleanup
rm -f "${PLANET_PATH}" "${MANIFEST_PATH}"

echo ""
echo "Sharding complete!"
