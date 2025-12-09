#!/bin/bash
# Submit AWS Batch jobs to process all US states and territories

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/scripts/common.sh"

load_env
require_env AWS_REGION JOB_QUEUE JOB_DEFINITION S3_BUCKET

# All 50 US states + DC + territories (Geofabrik paths)
REGIONS=(
    "north-america/us/alabama"
    "north-america/us/alaska"
    "north-america/us/arizona"
    "north-america/us/arkansas"
    "north-america/us/california"
    "north-america/us/colorado"
    "north-america/us/connecticut"
    "north-america/us/delaware"
    "north-america/us/district-of-columbia"
    "north-america/us/florida"
    "north-america/us/georgia"
    "north-america/us/hawaii"
    "north-america/us/idaho"
    "north-america/us/illinois"
    "north-america/us/indiana"
    "north-america/us/iowa"
    "north-america/us/kansas"
    "north-america/us/kentucky"
    "north-america/us/louisiana"
    "north-america/us/maine"
    "north-america/us/maryland"
    "north-america/us/massachusetts"
    "north-america/us/michigan"
    "north-america/us/minnesota"
    "north-america/us/mississippi"
    "north-america/us/missouri"
    "north-america/us/montana"
    "north-america/us/nebraska"
    "north-america/us/nevada"
    "north-america/us/new-hampshire"
    "north-america/us/new-jersey"
    "north-america/us/new-mexico"
    "north-america/us/new-york"
    "north-america/us/north-carolina"
    "north-america/us/north-dakota"
    "north-america/us/ohio"
    "north-america/us/oklahoma"
    "north-america/us/oregon"
    "north-america/us/pennsylvania"
    "north-america/us/rhode-island"
    "north-america/us/south-carolina"
    "north-america/us/south-dakota"
    "north-america/us/tennessee"
    "north-america/us/texas"
    "north-america/us/utah"
    "north-america/us/vermont"
    "north-america/us/virginia"
    "north-america/us/washington"
    "north-america/us/west-virginia"
    "north-america/us/wisconsin"
    "north-america/us/wyoming"
    "north-america/us/puerto-rico"
    "north-america/us/us-virgin-islands"
    "australia-oceania/american-samoa"
    "australia-oceania/guam"
    "australia-oceania/northern-mariana-islands"
)

TOTAL_REGIONS=${#REGIONS[@]}

echo "========================================"
echo "OSM POI Batch Job Submission"
echo "========================================"
echo "Total regions: ${TOTAL_REGIONS}"
echo "Job queue: ${JOB_QUEUE}"
echo "Job definition: ${JOB_DEFINITION}"
echo "Output bucket: ${S3_BUCKET}"
echo ""
echo "Submitting jobs..."
echo ""

JOB_IDS=()

for i in "${!REGIONS[@]}"; do
    REGION="${REGIONS[$i]}"
    REGION_NAME=$(basename "${REGION}")

    printf "[%2d/%d] %-25s " "$((i+1))" "${TOTAL_REGIONS}" "${REGION_NAME}"

    JOB_ID=$(aws batch submit-job \
        --job-name "osm-poi-${REGION_NAME}" \
        --job-queue "${JOB_QUEUE}" \
        --job-definition "${JOB_DEFINITION}" \
        --container-overrides "{\"environment\":[{\"name\":\"REGION_PATH\",\"value\":\"${REGION}\"}]}" \
        --query "jobId" \
        --output text \
        --region "${AWS_REGION}")

    JOB_IDS+=("${JOB_ID}")
    echo "→ ${JOB_ID}"

    # Small delay to avoid API throttling
    sleep 0.1
done

echo ""
echo "========================================"
echo "All ${TOTAL_REGIONS} jobs submitted!"
echo "========================================"
echo ""
echo "Monitor progress:"
echo "  ./monitor.sh"
echo ""
echo "Or check status:"
echo "  aws batch list-jobs --job-queue ${JOB_QUEUE} --job-status RUNNING"
echo ""
echo "List completed outputs:"
echo "  aws s3 ls s3://${S3_BUCKET}/parquet/"
echo ""
echo "Once complete, query via API:"
echo "  source .env && curl \"\${API_ENDPOINT}/pois?bbox=-122.5,37.7,-122.3,37.9\""
