#!/bin/bash
# AWS Batch Infrastructure Setup for OSM-H3 Processing
# This script creates all required AWS resources

set -euo pipefail

# Configuration - modify these as needed
PROJECT_NAME="osm-h3"
AWS_REGION="${AWS_REGION:-us-west-2}"
S3_BUCKET="${S3_BUCKET:-osm-h3-data-$(aws sts get-caller-identity --query Account --output text)}"

# Derived names
ECR_REPO="${PROJECT_NAME}-processor"
COMPUTE_ENV="${PROJECT_NAME}-compute"
JOB_QUEUE="${PROJECT_NAME}-queue"
JOB_DEFINITION="${PROJECT_NAME}-job"
MERGE_JOB_DEFINITION="${PROJECT_NAME}-merge-job"

echo "========================================"
echo "OSM-H3 AWS Batch Setup"
echo "========================================"
echo "Region: ${AWS_REGION}"
echo "S3 Bucket: ${S3_BUCKET}"
echo "ECR Repo: ${ECR_REPO}"
echo ""

# Get AWS account ID
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}"

# ============================================
# 1. Create S3 Bucket
# ============================================
echo "1. Creating S3 bucket..."
if aws s3api head-bucket --bucket "${S3_BUCKET}" 2>/dev/null; then
    echo "   Bucket ${S3_BUCKET} already exists"
else
    aws s3api create-bucket \
        --bucket "${S3_BUCKET}" \
        --region "${AWS_REGION}" \
        --create-bucket-configuration LocationConstraint="${AWS_REGION}"
    echo "   Created bucket ${S3_BUCKET}"
fi

# Enable intelligent tiering for cost optimization
aws s3api put-bucket-intelligent-tiering-configuration \
    --bucket "${S3_BUCKET}" \
    --id "auto-archive" \
    --intelligent-tiering-configuration '{
        "Id": "auto-archive",
        "Status": "Enabled",
        "Tierings": [
            {"Days": 90, "AccessTier": "ARCHIVE_ACCESS"},
            {"Days": 180, "AccessTier": "DEEP_ARCHIVE_ACCESS"}
        ]
    }' 2>/dev/null || echo "   (Intelligent tiering already configured)"

# ============================================
# 2. Create ECR Repository
# ============================================
echo ""
echo "2. Creating ECR repository..."
if aws ecr describe-repositories --repository-names "${ECR_REPO}" --region "${AWS_REGION}" >/dev/null 2>&1; then
    echo "   Repository ${ECR_REPO} already exists"
else
    aws ecr create-repository \
        --repository-name "${ECR_REPO}" \
        --region "${AWS_REGION}" \
        --image-scanning-configuration scanOnPush=true
    echo "   Created repository ${ECR_REPO}"
fi

# ============================================
# 3. Create IAM Roles
# ============================================
echo ""
echo "3. Creating IAM roles..."

# Batch Service Role
BATCH_SERVICE_ROLE="${PROJECT_NAME}-batch-service-role"
cat > /tmp/batch-trust-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "batch.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}
EOF

if aws iam get-role --role-name "${BATCH_SERVICE_ROLE}" >/dev/null 2>&1; then
    echo "   Role ${BATCH_SERVICE_ROLE} already exists"
else
    aws iam create-role \
        --role-name "${BATCH_SERVICE_ROLE}" \
        --assume-role-policy-document file:///tmp/batch-trust-policy.json
    aws iam attach-role-policy \
        --role-name "${BATCH_SERVICE_ROLE}" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole
    echo "   Created ${BATCH_SERVICE_ROLE}"
fi

# ECS Instance Role (for EC2 compute environment)
ECS_INSTANCE_ROLE="${PROJECT_NAME}-ecs-instance-role"
cat > /tmp/ecs-trust-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "ec2.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}
EOF

if aws iam get-role --role-name "${ECS_INSTANCE_ROLE}" >/dev/null 2>&1; then
    echo "   Role ${ECS_INSTANCE_ROLE} already exists"
else
    aws iam create-role \
        --role-name "${ECS_INSTANCE_ROLE}" \
        --assume-role-policy-document file:///tmp/ecs-trust-policy.json
    aws iam attach-role-policy \
        --role-name "${ECS_INSTANCE_ROLE}" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role
    echo "   Created ${ECS_INSTANCE_ROLE}"
fi

# Create instance profile for ECS instances
if aws iam get-instance-profile --instance-profile-name "${ECS_INSTANCE_ROLE}" >/dev/null 2>&1; then
    echo "   Instance profile ${ECS_INSTANCE_ROLE} already exists"
else
    aws iam create-instance-profile --instance-profile-name "${ECS_INSTANCE_ROLE}"
    aws iam add-role-to-instance-profile \
        --instance-profile-name "${ECS_INSTANCE_ROLE}" \
        --role-name "${ECS_INSTANCE_ROLE}"
    echo "   Created instance profile ${ECS_INSTANCE_ROLE}"
    echo "   Waiting for instance profile propagation..."
    sleep 10
fi

# Job Execution Role (for the container)
JOB_ROLE="${PROJECT_NAME}-job-role"
cat > /tmp/ecs-task-trust-policy.json << 'EOF'
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "ecs-tasks.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}
EOF

# S3 access policy for job
cat > /tmp/s3-access-policy.json << EOF
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:ListBucket"
        ],
        "Resource": [
            "arn:aws:s3:::${S3_BUCKET}",
            "arn:aws:s3:::${S3_BUCKET}/*"
        ]
    }]
}
EOF

if aws iam get-role --role-name "${JOB_ROLE}" >/dev/null 2>&1; then
    echo "   Role ${JOB_ROLE} already exists"
else
    aws iam create-role \
        --role-name "${JOB_ROLE}" \
        --assume-role-policy-document file:///tmp/ecs-task-trust-policy.json
    # Also attach basic execution role for CloudWatch logs
    aws iam attach-role-policy \
        --role-name "${JOB_ROLE}" \
        --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
    echo "   Created ${JOB_ROLE}"
fi

# Always update inline S3 access policy in case the bucket changed
aws iam put-role-policy \
    --role-name "${JOB_ROLE}" \
    --policy-name "S3Access" \
    --policy-document file:///tmp/s3-access-policy.json

# ============================================
# 4. Get Default VPC and Subnets
# ============================================
echo ""
echo "4. Getting VPC configuration..."

# Get default VPC
VPC_ID=$(aws ec2 describe-vpcs \
    --filters "Name=isDefault,Values=true" \
    --query "Vpcs[0].VpcId" \
    --output text \
    --region "${AWS_REGION}")

if [ "${VPC_ID}" = "None" ] || [ -z "${VPC_ID}" ]; then
    echo "   ERROR: No default VPC found. Please create one or modify this script."
    exit 1
fi
echo "   VPC: ${VPC_ID}"

# Get subnets
SUBNET_IDS=$(aws ec2 describe-subnets \
    --filters "Name=vpc-id,Values=${VPC_ID}" \
    --query "Subnets[*].SubnetId" \
    --output text \
    --region "${AWS_REGION}" | tr '\t' ',')
echo "   Subnets: ${SUBNET_IDS}"

# Get or create security group
SG_NAME="${PROJECT_NAME}-batch-sg"
SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=${SG_NAME}" "Name=vpc-id,Values=${VPC_ID}" \
    --query "SecurityGroups[0].GroupId" \
    --output text \
    --region "${AWS_REGION}" 2>/dev/null || echo "None")

if [ "${SG_ID}" = "None" ] || [ -z "${SG_ID}" ]; then
    SG_ID=$(aws ec2 create-security-group \
        --group-name "${SG_NAME}" \
        --description "Security group for OSM-H3 Batch jobs" \
        --vpc-id "${VPC_ID}" \
        --query "GroupId" \
        --output text \
        --region "${AWS_REGION}")
    # Allow outbound traffic (for downloading OSM data)
    aws ec2 authorize-security-group-egress \
        --group-id "${SG_ID}" \
        --protocol all \
        --cidr 0.0.0.0/0 \
        --region "${AWS_REGION}" 2>/dev/null || true
    echo "   Created security group: ${SG_ID}"
else
    echo "   Security group: ${SG_ID}"
fi

# ============================================
# 5. Create Batch Compute Environment
# ============================================
echo ""
echo "5. Creating Batch compute environment..."

# Check if compute environment exists (query returns "None" string if not found, not error)
CE_STATE=$(aws batch describe-compute-environments \
    --compute-environments "${COMPUTE_ENV}" \
    --query "computeEnvironments[0].state" \
    --output text \
    --region "${AWS_REGION}" 2>/dev/null || echo "")

if [ "${CE_STATE}" = "ENABLED" ] || [ "${CE_STATE}" = "DISABLED" ]; then
    echo "   Compute environment ${COMPUTE_ENV} already exists (state: ${CE_STATE})"
else
    # Create compute environment with spot instances for cost savings
    aws batch create-compute-environment \
        --compute-environment-name "${COMPUTE_ENV}" \
        --type MANAGED \
        --state ENABLED \
        --service-role "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${BATCH_SERVICE_ROLE}" \
        --compute-resources "{
            \"type\": \"SPOT\",
            \"allocationStrategy\": \"SPOT_CAPACITY_OPTIMIZED\",
            \"minvCpus\": 0,
            \"maxvCpus\": 256,
            \"desiredvCpus\": 0,
            \"instanceTypes\": [\"m6i.xlarge\", \"m6i.2xlarge\", \"m5.xlarge\", \"m5.2xlarge\", \"r6i.xlarge\", \"r5.xlarge\"],
            \"subnets\": [$(echo ${SUBNET_IDS} | sed 's/,/","/g' | sed 's/^/"/' | sed 's/$/"/')],
            \"securityGroupIds\": [\"${SG_ID}\"],
            \"instanceRole\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:instance-profile/${ECS_INSTANCE_ROLE}\",
            \"spotIamFleetRole\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:role/aws-ec2-spot-fleet-tagging-role\"
        }" \
        --region "${AWS_REGION}"
    echo "   Created compute environment ${COMPUTE_ENV}"
    echo "   Waiting for compute environment to be VALID..."

    # Wait for compute environment to be valid
    for i in {1..30}; do
        STATUS=$(aws batch describe-compute-environments \
            --compute-environments "${COMPUTE_ENV}" \
            --query "computeEnvironments[0].status" \
            --output text \
            --region "${AWS_REGION}")
        if [ "${STATUS}" = "VALID" ]; then
            echo "   Compute environment is VALID"
            break
        fi
        echo "   Status: ${STATUS}, waiting..."
        sleep 10
    done
fi

# ============================================
# 6. Create Job Queue
# ============================================
echo ""
echo "6. Creating job queue..."

# Check if job queue exists (query returns "None" string if not found, not error)
JQ_STATE=$(aws batch describe-job-queues \
    --job-queues "${JOB_QUEUE}" \
    --query "jobQueues[0].state" \
    --output text \
    --region "${AWS_REGION}" 2>/dev/null || echo "")

if [ "${JQ_STATE}" = "ENABLED" ] || [ "${JQ_STATE}" = "DISABLED" ]; then
    echo "   Job queue ${JOB_QUEUE} already exists (state: ${JQ_STATE})"
else
    aws batch create-job-queue \
        --job-queue-name "${JOB_QUEUE}" \
        --state ENABLED \
        --priority 1 \
        --compute-environment-order "order=1,computeEnvironment=${COMPUTE_ENV}" \
        --region "${AWS_REGION}"
    echo "   Created job queue ${JOB_QUEUE}"
fi

# ============================================
# 7. Create Job Definitions
# ============================================
echo ""
echo "7. Creating job definitions..."

# Worker job definition (per-state processing)
aws batch register-job-definition \
    --job-definition-name "${JOB_DEFINITION}" \
    --type container \
    --platform-capabilities EC2 \
    --container-properties "{
        \"image\": \"${ECR_URI}:latest\",
        \"resourceRequirements\": [
            {\"type\": \"VCPU\", \"value\": \"2\"},
            {\"type\": \"MEMORY\", \"value\": \"8192\"}
        ],
        \"jobRoleArn\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:role/${JOB_ROLE}\",
        \"executionRoleArn\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:role/${JOB_ROLE}\",
        \"environment\": [
            {\"name\": \"S3_BUCKET\", \"value\": \"${S3_BUCKET}\"},
            {\"name\": \"H3_RESOLUTIONS\", \"value\": \"7,9\"}
        ],
        \"logConfiguration\": {
            \"logDriver\": \"awslogs\",
            \"options\": {
                \"awslogs-group\": \"/aws/batch/${PROJECT_NAME}\",
                \"awslogs-region\": \"${AWS_REGION}\",
                \"awslogs-stream-prefix\": \"job\"
            }
        }
    }" \
    --retry-strategy "attempts=2" \
    --timeout "attemptDurationSeconds=7200" \
    --region "${AWS_REGION}"

echo "   Registered job definition ${JOB_DEFINITION}"

# Merge job definition: higher resources
aws batch register-job-definition \
    --job-definition-name "${MERGE_JOB_DEFINITION}" \
    --type container \
    --platform-capabilities EC2 \
    --container-properties "{
        \"image\": \"${ECR_URI}:latest\",
        \"resourceRequirements\": [
            {\"type\": \"VCPU\", \"value\": \"4\"},
            {\"type\": \"MEMORY\", \"value\": \"16384\"}
        ],
        \"jobRoleArn\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:role/${JOB_ROLE}\",
        \"executionRoleArn\": \"arn:aws:iam::${AWS_ACCOUNT_ID}:role/${JOB_ROLE}\",
        \"environment\": [
            {\"name\": \"S3_BUCKET\", \"value\": \"${S3_BUCKET}\"}
        ],
        \"logConfiguration\": {
            \"logDriver\": \"awslogs\",
            \"options\": {
                \"awslogs-group\": \"/aws/batch/${PROJECT_NAME}\",
                \"awslogs-region\": \"${AWS_REGION}\",
                \"awslogs-stream-prefix\": \"job\"
            }
        }
    }" \
    --retry-strategy "attempts=1" \
    --timeout "attemptDurationSeconds=3600" \
    --region "${AWS_REGION}"

echo "   Registered merge job definition ${MERGE_JOB_DEFINITION}"

# Create CloudWatch log group
aws logs create-log-group \
    --log-group-name "/aws/batch/${PROJECT_NAME}" \
    --region "${AWS_REGION}" 2>/dev/null || echo "   (Log group already exists)"

# ============================================
# Summary
# ============================================
echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "Resources created:"
echo "  - S3 Bucket: ${S3_BUCKET}"
echo "  - ECR Repository: ${ECR_URI}"
echo "  - Compute Environment: ${COMPUTE_ENV}"
echo "  - Job Queue: ${JOB_QUEUE}"
echo "  - Job Definition: ${JOB_DEFINITION}"
echo "  - Merge Job Definition: ${MERGE_JOB_DEFINITION}"
echo ""
echo "Next steps:"
echo "  1. Build and push the Docker image:"
echo "     ./build-and-push.sh"
echo ""
echo "  2. Run the pipeline:"
echo "     ./run-all-states.sh"
echo ""

# Save configuration for other scripts
cat > .env << EOF
export AWS_REGION="${AWS_REGION}"
export S3_BUCKET="${S3_BUCKET}"
export ECR_URI="${ECR_URI}"
export JOB_QUEUE="${JOB_QUEUE}"
export JOB_DEFINITION="${JOB_DEFINITION}"
export MERGE_JOB_DEFINITION="${MERGE_JOB_DEFINITION}"
export AWS_ACCOUNT_ID="${AWS_ACCOUNT_ID}"
export JOB_ROLE="${JOB_ROLE}"
EOF

echo "Configuration saved to .env"
