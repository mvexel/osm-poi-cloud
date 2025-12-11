#!/bin/bash
# Setup Athena database, Lambda function, and API Gateway for POI queries

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

# shellcheck source=scripts/common.sh
source "${SCRIPT_DIR}/scripts/common.sh"

load_env
require_env AWS_REGION S3_BUCKET AWS_ACCOUNT_ID

# Athena configuration
ATHENA_DATABASE="osm_pois"
ATHENA_TABLE="pois"
ATHENA_OUTPUT="s3://${S3_BUCKET}/athena-results/"
ATHENA_WORKGROUP="osm-pois-workgroup"

# Lambda configuration
LAMBDA_NAME="osm-pois-api"
LAMBDA_ROLE_NAME="osm-pois-lambda-role"
API_NAME="osm-pois-api"

echo "========================================"
echo "Setting up Athena + Lambda + API Gateway"
echo "========================================"
echo "Region: ${AWS_REGION}"
echo "S3 Bucket: ${S3_BUCKET}"
echo ""

# Create Athena results directory
echo "Creating Athena results location..."
aws s3api put-object --bucket "${S3_BUCKET}" --key "athena-results/" --region "${AWS_REGION}" || true

# Create Athena workgroup
echo "Creating Athena workgroup..."
aws athena create-work-group \
    --name "${ATHENA_WORKGROUP}" \
    --configuration "ResultConfiguration={OutputLocation=${ATHENA_OUTPUT}},EnforceWorkGroupConfiguration=false,PublishCloudWatchMetricsEnabled=true" \
    --region "${AWS_REGION}" 2>/dev/null || echo "  Workgroup already exists"

# Create Athena database
echo "Creating Athena database..."
aws athena start-query-execution \
    --query-string "CREATE DATABASE IF NOT EXISTS ${ATHENA_DATABASE}" \
    --result-configuration "OutputLocation=${ATHENA_OUTPUT}" \
    --work-group "${ATHENA_WORKGROUP}" \
    --region "${AWS_REGION}"

sleep 2

# Create Athena table
echo "Creating Athena table..."
TABLE_SQL=$(cat <<EOF
CREATE EXTERNAL TABLE IF NOT EXISTS ${ATHENA_DATABASE}.${ATHENA_TABLE} (
    osm_id STRING,
    osm_type STRING,
    name STRING,
    class STRING,
    lon DOUBLE,
    lat DOUBLE,
    state STRING,
    amenity STRING,
    shop STRING,
    leisure STRING,
    tourism STRING,
    cuisine STRING,
    opening_hours STRING,
    phone STRING,
    website STRING,
    brand STRING,
    operator STRING,
    tags STRING,
    lon_bucket INT,
    lat_bucket INT
)
STORED AS PARQUET
LOCATION 's3://${S3_BUCKET}/parquet/'
TBLPROPERTIES ('parquet.compression'='SNAPPY')
EOF
)

aws athena start-query-execution \
    --query-string "${TABLE_SQL}" \
    --query-execution-context "Database=${ATHENA_DATABASE}" \
    --result-configuration "OutputLocation=${ATHENA_OUTPUT}" \
    --work-group "${ATHENA_WORKGROUP}" \
    --region "${AWS_REGION}"

sleep 2

# Create Lambda execution role
echo "Creating Lambda execution role..."
TRUST_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "lambda.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}
EOF
)

LAMBDA_ROLE_ARN=$(aws iam create-role \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --assume-role-policy-document "${TRUST_POLICY}" \
    --query "Role.Arn" \
    --output text 2>/dev/null) || \
LAMBDA_ROLE_ARN=$(aws iam get-role \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --query "Role.Arn" \
    --output text)

echo "  Role ARN: ${LAMBDA_ROLE_ARN}"

# Attach policies to Lambda role
echo "Attaching policies to Lambda role..."
aws iam attach-role-policy \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" 2>/dev/null || true

# Create inline policy for Athena and S3 access
ATHENA_POLICY=$(cat <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution"
            ],
            "Resource": "*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:ListBucket",
                "s3:GetBucketLocation",
                "s3:PutObject"
            ],
            "Resource": [
                "arn:aws:s3:::${S3_BUCKET}",
                "arn:aws:s3:::${S3_BUCKET}/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetPartitions"
            ],
            "Resource": "*"
        }
    ]
}
EOF
)

aws iam put-role-policy \
    --role-name "${LAMBDA_ROLE_NAME}" \
    --policy-name "athena-s3-access" \
    --policy-document "${ATHENA_POLICY}"

# Wait for role to propagate
echo "Waiting for IAM role to propagate..."
sleep 10

# Create Lambda deployment package
echo "Creating Lambda deployment package..."
LAMBDA_DIR=$(mktemp -d)
cp athena/lambda_handler.py "${LAMBDA_DIR}/"
cd "${LAMBDA_DIR}"
zip -q lambda.zip lambda_handler.py
cd "${SCRIPT_DIR}"

# Create or update Lambda function
echo "Creating Lambda function..."
LAMBDA_ARN=$(aws lambda create-function \
    --function-name "${LAMBDA_NAME}" \
    --runtime python3.11 \
    --role "${LAMBDA_ROLE_ARN}" \
    --handler lambda_handler.lambda_handler \
    --zip-file "fileb://${LAMBDA_DIR}/lambda.zip" \
    --timeout 30 \
    --memory-size 256 \
    --environment "Variables={ATHENA_DATABASE=${ATHENA_DATABASE},ATHENA_TABLE=${ATHENA_TABLE},ATHENA_OUTPUT=${ATHENA_OUTPUT},ATHENA_WORKGROUP=${ATHENA_WORKGROUP}}" \
    --region "${AWS_REGION}" \
    --query "FunctionArn" \
    --output text 2>/dev/null) || {
    echo "  Function exists, updating..."
    aws lambda update-function-code \
        --function-name "${LAMBDA_NAME}" \
        --zip-file "fileb://${LAMBDA_DIR}/lambda.zip" \
        --region "${AWS_REGION}" > /dev/null

    aws lambda update-function-configuration \
        --function-name "${LAMBDA_NAME}" \
        --environment "Variables={ATHENA_DATABASE=${ATHENA_DATABASE},ATHENA_TABLE=${ATHENA_TABLE},ATHENA_OUTPUT=${ATHENA_OUTPUT},ATHENA_WORKGROUP=${ATHENA_WORKGROUP}}" \
        --region "${AWS_REGION}" > /dev/null

    LAMBDA_ARN=$(aws lambda get-function \
        --function-name "${LAMBDA_NAME}" \
        --query "Configuration.FunctionArn" \
        --output text \
        --region "${AWS_REGION}")
}

echo "  Lambda ARN: ${LAMBDA_ARN}"

# Clean up temp directory
rm -rf "${LAMBDA_DIR}"

# Create API Gateway HTTP API
echo "Creating API Gateway HTTP API..."
API_ID=$(aws apigatewayv2 create-api \
    --name "${API_NAME}" \
    --protocol-type HTTP \
    --cors-configuration "AllowOrigins=*,AllowMethods=GET,OPTIONS,AllowHeaders=Content-Type" \
    --region "${AWS_REGION}" \
    --query "ApiId" \
    --output text 2>/dev/null) || {
    echo "  API may exist, checking..."
    API_ID=$(aws apigatewayv2 get-apis \
        --region "${AWS_REGION}" \
        --query "Items[?Name=='${API_NAME}'].ApiId | [0]" \
        --output text)
}

echo "  API ID: ${API_ID}"

# Create Lambda integration
echo "Creating Lambda integration..."
INTEGRATION_ID=$(aws apigatewayv2 create-integration \
    --api-id "${API_ID}" \
    --integration-type AWS_PROXY \
    --integration-uri "${LAMBDA_ARN}" \
    --payload-format-version "2.0" \
    --region "${AWS_REGION}" \
    --query "IntegrationId" \
    --output text 2>/dev/null) || {
    INTEGRATION_ID=$(aws apigatewayv2 get-integrations \
        --api-id "${API_ID}" \
        --region "${AWS_REGION}" \
        --query "Items[0].IntegrationId" \
        --output text)
}

echo "  Integration ID: ${INTEGRATION_ID}"

# Create routes
echo "Creating routes..."
for ROUTE in "GET /pois" "GET /classes" "GET /health"; do
    aws apigatewayv2 create-route \
        --api-id "${API_ID}" \
        --route-key "${ROUTE}" \
        --target "integrations/${INTEGRATION_ID}" \
        --region "${AWS_REGION}" 2>/dev/null || echo "  Route ${ROUTE} may already exist"
done

# Create default stage with auto-deploy
echo "Creating default stage..."
aws apigatewayv2 create-stage \
    --api-id "${API_ID}" \
    --stage-name '$default' \
    --auto-deploy \
    --region "${AWS_REGION}" 2>/dev/null || echo "  Stage may already exist"

# Add Lambda permission for API Gateway
echo "Adding Lambda permission for API Gateway..."
aws lambda add-permission \
    --function-name "${LAMBDA_NAME}" \
    --statement-id "apigateway-invoke" \
    --action "lambda:InvokeFunction" \
    --principal "apigateway.amazonaws.com" \
    --source-arn "arn:aws:execute-api:${AWS_REGION}:${AWS_ACCOUNT_ID}:${API_ID}/*" \
    --region "${AWS_REGION}" 2>/dev/null || echo "  Permission may already exist"

# Get API endpoint
API_ENDPOINT=$(aws apigatewayv2 get-api \
    --api-id "${API_ID}" \
    --region "${AWS_REGION}" \
    --query "ApiEndpoint" \
    --output text)

echo ""
echo "========================================"
echo "Setup Complete!"
echo "========================================"
echo ""
echo "API Endpoint: ${API_ENDPOINT}"
echo ""
echo "Test the API:"
echo "  curl '${API_ENDPOINT}/health'"
echo "  curl '${API_ENDPOINT}/classes'"
echo "  curl '${API_ENDPOINT}/pois?bbox=-122.5,37.7,-122.3,37.9'"
echo ""
echo "Note: Make sure to run Batch jobs first to populate data:"
echo "  ./run-all-states.sh"
echo ""

# Save API endpoint to .env
if ! grep -q "API_ENDPOINT" .env 2>/dev/null; then
    echo "export API_ENDPOINT=\"${API_ENDPOINT}\"" >> .env
    echo "export ATHENA_DATABASE=\"${ATHENA_DATABASE}\"" >> .env
    echo "export ATHENA_WORKGROUP=\"${ATHENA_WORKGROUP}\"" >> .env
fi
