#!/bin/bash
# Build and push Docker image to ECR

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/scripts/common.sh"

load_env
require_env ECR_URI AWS_REGION AWS_ACCOUNT_ID
require_command docker

echo "Building Docker image..."
docker buildx build --platform linux/amd64 -t "${ECR_URI}:latest" batch/

echo ""
echo "Logging into ECR..."
aws_ecr_login

echo ""
echo "Pushing image to ECR..."
docker push "${ECR_URI}:latest"

echo ""
echo "Done! Image pushed to ${ECR_URI}:latest"
