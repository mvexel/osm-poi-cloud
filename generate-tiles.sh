#!/bin/bash
# Submit AWS Batch job to generate PMTiles from Parquet data

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/scripts/common.sh"

load_env
require_env AWS_REGION JOB_QUEUE S3_BUCKET

if [ -z "${TILES_JOB_DEFINITION:-}" ]; then
    echo "ERROR: TILES_JOB_DEFINITION not set. Run setup-tiles.sh first."
    exit 1
fi

echo "========================================"
echo "PMTiles Generation Job"
echo "========================================"
echo "Job definition: ${TILES_JOB_DEFINITION}"
echo "Output bucket: ${S3_BUCKET}"
echo ""

# Check if Parquet files exist
PARQUET_COUNT=$(aws s3 ls "s3://${S3_BUCKET}/parquet/" --region "${AWS_REGION}" | grep -c ".parquet$" || echo "0")
echo "Found ${PARQUET_COUNT} Parquet files in S3"

if [ "${PARQUET_COUNT}" -eq 0 ]; then
    echo "ERROR: No Parquet files found. Run batch jobs first."
    exit 1
fi

echo ""
echo "Submitting PMTiles generation job..."

JOB_ID=$(aws batch submit-job \
    --job-name "osm-poi-tiles-$(date +%Y%m%d-%H%M%S)" \
    --job-queue "${JOB_QUEUE}" \
    --job-definition "${TILES_JOB_DEFINITION}" \
    --query "jobId" \
    --output text \
    --region "${AWS_REGION}")

echo "Job submitted: ${JOB_ID}"
echo ""
echo "Monitor progress:"
echo "  aws batch describe-jobs --jobs ${JOB_ID} --region ${AWS_REGION}"
echo ""
echo "View logs:"
echo "  aws logs tail /aws/batch/osm-h3 --region ${AWS_REGION} --follow"
echo ""
echo "Once complete, PMTiles will be available at:"
echo "  https://${CLOUDFRONT_DOMAIN}/pois.pmtiles"
