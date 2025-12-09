#!/bin/bash
# Setup PMTiles generation and CloudFront distribution

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

TILES_ECR_REPO="osm-h3-tiles"
TILES_JOB_DEFINITION="osm-h3-tiles-job"

echo "========================================"
echo "Setting up PMTiles + CloudFront"
echo "========================================"
echo "Region: ${AWS_REGION}"
echo "S3 Bucket: ${S3_BUCKET}"
echo ""

# Create ECR repository for tiles container
echo "Creating ECR repository for tiles..."
TILES_ECR_URI=$(aws ecr create-repository \
    --repository-name "${TILES_ECR_REPO}" \
    --region "${AWS_REGION}" \
    --query "repository.repositoryUri" \
    --output text 2>/dev/null) || \
TILES_ECR_URI=$(aws ecr describe-repositories \
    --repository-names "${TILES_ECR_REPO}" \
    --region "${AWS_REGION}" \
    --query "repositories[0].repositoryUri" \
    --output text)

echo "  ECR URI: ${TILES_ECR_URI}"

# Build and push tiles container
echo ""
echo "Building tiles Docker image..."
docker buildx build --platform linux/amd64 -t "${TILES_ECR_URI}:latest" tiles/

echo ""
echo "Logging into ECR..."
aws ecr get-login-password --region "${AWS_REGION}" | \
    docker login --username AWS --password-stdin "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo ""
echo "Pushing tiles image to ECR..."
docker push "${TILES_ECR_URI}:latest"

# Create Batch job definition for tiles generation
echo ""
echo "Creating Batch job definition for tiles..."
JOB_DEF_JSON=$(cat <<EOF
{
    "jobDefinitionName": "${TILES_JOB_DEFINITION}",
    "type": "container",
    "containerProperties": {
        "image": "${TILES_ECR_URI}:latest",
        "resourceRequirements": [
            {"type": "VCPU", "value": "4"},
            {"type": "MEMORY", "value": "16384"}
        ],
        "environment": [
            {"name": "S3_BUCKET", "value": "${S3_BUCKET}"}
        ],
        "jobRoleArn": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/osm-h3-batch-role",
        "executionRoleArn": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/osm-h3-batch-execution-role",
        "logConfiguration": {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": "/aws/batch/osm-h3",
                "awslogs-region": "${AWS_REGION}",
                "awslogs-stream-prefix": "tiles"
            }
        }
    },
    "timeout": {"attemptDurationSeconds": 7200},
    "retryStrategy": {"attempts": 1}
}
EOF
)

aws batch register-job-definition \
    --cli-input-json "${JOB_DEF_JSON}" \
    --region "${AWS_REGION}" > /dev/null

echo "  Job definition created: ${TILES_JOB_DEFINITION}"

# Update S3 bucket policy for CloudFront OAC
echo ""
echo "Configuring S3 bucket for CloudFront..."

# Create CloudFront Origin Access Control
echo "Creating CloudFront Origin Access Control..."
OAC_ID=$(aws cloudfront create-origin-access-control \
    --origin-access-control-config "{
        \"Name\": \"${S3_BUCKET}-oac\",
        \"Description\": \"OAC for ${S3_BUCKET}\",
        \"SigningProtocol\": \"sigv4\",
        \"SigningBehavior\": \"always\",
        \"OriginAccessControlOriginType\": \"s3\"
    }" \
    --query "OriginAccessControl.Id" \
    --output text 2>/dev/null) || {
    echo "  OAC may already exist, looking it up..."
    OAC_ID=$(aws cloudfront list-origin-access-controls \
        --query "OriginAccessControlList.Items[?Name=='${S3_BUCKET}-oac'].Id | [0]" \
        --output text)
}

echo "  OAC ID: ${OAC_ID}"

# Create CloudFront distribution
echo ""
echo "Creating CloudFront distribution..."
DISTRIBUTION_CONFIG=$(cat <<EOF
{
    "CallerReference": "osm-h3-tiles-$(date +%s)",
    "Comment": "OSM POI Tiles",
    "Enabled": true,
    "Origins": {
        "Quantity": 1,
        "Items": [
            {
                "Id": "S3-${S3_BUCKET}",
                "DomainName": "${S3_BUCKET}.s3.${AWS_REGION}.amazonaws.com",
                "OriginPath": "/tiles",
                "S3OriginConfig": {
                    "OriginAccessIdentity": ""
                },
                "OriginAccessControlId": "${OAC_ID}"
            }
        ]
    },
    "DefaultCacheBehavior": {
        "TargetOriginId": "S3-${S3_BUCKET}",
        "ViewerProtocolPolicy": "redirect-to-https",
        "AllowedMethods": {
            "Quantity": 2,
            "Items": ["GET", "HEAD"],
            "CachedMethods": {
                "Quantity": 2,
                "Items": ["GET", "HEAD"]
            }
        },
        "CachePolicyId": "658327ea-f89d-4fab-a63d-7e88639e58f6",
        "OriginRequestPolicyId": "88a5eaf4-2fd4-4709-b370-b4c650ea3fcf",
        "ResponseHeadersPolicyId": "5cc3b908-e619-4b99-88e5-2cf7f45965bd",
        "Compress": true
    },
    "PriceClass": "PriceClass_100",
    "HttpVersion": "http2and3"
}
EOF
)

DISTRIBUTION_RESULT=$(aws cloudfront create-distribution \
    --distribution-config "${DISTRIBUTION_CONFIG}" \
    --output json 2>/dev/null) || {
    echo "  Distribution may already exist"
    DISTRIBUTION_RESULT=""
}

if [ -n "${DISTRIBUTION_RESULT}" ]; then
    DISTRIBUTION_ID=$(echo "${DISTRIBUTION_RESULT}" | jq -r '.Distribution.Id')
    DISTRIBUTION_DOMAIN=$(echo "${DISTRIBUTION_RESULT}" | jq -r '.Distribution.DomainName')
else
    # Find existing distribution
    DISTRIBUTION_ID=$(aws cloudfront list-distributions \
        --query "DistributionList.Items[?Comment=='OSM POI Tiles'].Id | [0]" \
        --output text)
    DISTRIBUTION_DOMAIN=$(aws cloudfront list-distributions \
        --query "DistributionList.Items[?Comment=='OSM POI Tiles'].DomainName | [0]" \
        --output text)
fi

echo "  Distribution ID: ${DISTRIBUTION_ID}"
echo "  Domain: ${DISTRIBUTION_DOMAIN}"

# Update S3 bucket policy to allow CloudFront access
echo ""
echo "Updating S3 bucket policy..."
BUCKET_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowCloudFrontServicePrincipal",
            "Effect": "Allow",
            "Principal": {
                "Service": "cloudfront.amazonaws.com"
            },
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::${S3_BUCKET}/tiles/*",
            "Condition": {
                "StringEquals": {
                    "AWS:SourceArn": "arn:aws:cloudfront::${AWS_ACCOUNT_ID}:distribution/${DISTRIBUTION_ID}"
                }
            }
        }
    ]
}
EOF
)

aws s3api put-bucket-policy \
    --bucket "${S3_BUCKET}" \
    --policy "${BUCKET_POLICY}"

# Save to .env
if ! grep -q "TILES_ECR_URI" .env 2>/dev/null; then
    echo "export TILES_ECR_URI=\"${TILES_ECR_URI}\"" >> .env
    echo "export TILES_JOB_DEFINITION=\"${TILES_JOB_DEFINITION}\"" >> .env
    echo "export CLOUDFRONT_DOMAIN=\"${DISTRIBUTION_DOMAIN}\"" >> .env
    echo "export CLOUDFRONT_DISTRIBUTION_ID=\"${DISTRIBUTION_ID}\"" >> .env
fi

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "CloudFront Distribution: https://${DISTRIBUTION_DOMAIN}"
echo ""
echo "To generate PMTiles after batch jobs complete:"
echo "  ./generate-tiles.sh"
echo ""
echo "PMTiles URL (after generation):"
echo "  https://${DISTRIBUTION_DOMAIN}/pois.pmtiles"
