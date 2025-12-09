#!/bin/bash
# Monitor AWS Batch job progress

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# Load configuration
if [ -f .env ]; then
    source .env
else
    echo "ERROR: .env file not found. Run setup-aws.sh first."
    exit 1
fi

echo "========================================"
echo "OSM-H3 Batch Job Monitor"
echo "========================================"
echo ""

while true; do
    clear
    echo "OSM-H3 Batch Processing Status - $(date)"
    echo "========================================"
    echo ""

    # Get job counts by status
    for STATUS in SUBMITTED PENDING RUNNABLE STARTING RUNNING SUCCEEDED FAILED; do
        COUNT=$(aws batch list-jobs \
            --job-queue "${JOB_QUEUE}" \
            --job-status "${STATUS}" \
            --query "length(jobSummaryList)" \
            --output text \
            --region "${AWS_REGION}" 2>/dev/null || echo "0")
        printf "  %-12s %s\n" "${STATUS}:" "${COUNT}"
    done

    echo ""
    echo "----------------------------------------"
    echo "Running Jobs:"
    aws batch list-jobs \
        --job-queue "${JOB_QUEUE}" \
        --job-status RUNNING \
        --query "jobSummaryList[*].[jobName,startedAt]" \
        --output table \
        --region "${AWS_REGION}" 2>/dev/null || echo "  None"

    echo ""
    echo "----------------------------------------"
    echo "Recent Failures (last 5):"
    aws batch list-jobs \
        --job-queue "${JOB_QUEUE}" \
        --job-status FAILED \
        --query "jobSummaryList[:5].[jobName,statusReason]" \
        --output table \
        --region "${AWS_REGION}" 2>/dev/null || echo "  None"

    echo ""
    echo "----------------------------------------"
    echo "Output Files:"
    aws s3 ls "s3://${S3_BUCKET}/geoparquet/" --recursive 2>/dev/null | wc -l | xargs -I{} echo "  {} parquet files written"

    echo ""
    echo "Press Ctrl+C to exit, refreshing in 30s..."
    sleep 30
done
