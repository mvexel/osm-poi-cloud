#!/bin/bash
# Build and push Docker image to ECR

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

echo "Building Docker image..."
docker buildx build --platform linux/amd64 -t "${ECR_URI}:latest" batch/

echo ""
echo "Logging into ECR..."
aws ecr get-login-password --region "${AWS_REGION}" | \
    docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo ""
echo "Pushing image to ECR..."
docker push "${ECR_URI}:latest"

echo ""
echo "Done! Image pushed to ${ECR_URI}:latest"
