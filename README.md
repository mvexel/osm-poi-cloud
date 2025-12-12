# OSM-H3 Pipeline

A scalable AWS Batch-based pipeline for processing OpenStreetMap data into H3-indexed POIs, stored in Parquet format and served via PMTiles.

## Quick Start

```bash
# Deploy infrastructure
cd pulumi
pulumi up

# Run the pipeline
./pipeline_cli.py run

# Monitor progress
./pipeline_cli.py status --watch
```

## Pulumi Deployment Guide

### Prerequisites

- [Pulumi CLI](https://www.pulumi.com/docs/get-started/install/) installed
- AWS credentials configured (`aws configure` or environment variables)
- Python 3.11+ with `uv` or `pip`

### Initial Setup

```bash
# Install Python dependencies
cd pulumi
uv pip install -r requirements.txt

# Login to Pulumi (local state or cloud)
pulumi login --local  # or just `pulumi login` for Pulumi Cloud

# Initialize stack (use 'dev', 'prod', etc.)
pulumi stack init dev

# Configure AWS region
pulumi config set aws:region us-west-2
```

### Deploy Infrastructure

```bash
cd pulumi

# Preview changes
pulumi preview

# Deploy everything (ECR, S3, Batch compute, job definitions, Athena, API Gateway, CloudFront)
pulumi up

# Deploy non-interactively
pulumi up --yes
```

The deployment creates:
- S3 bucket for data storage
- ECR repositories for Docker images
- AWS Batch compute environment, job queue, and job definitions
- Athena database and table
- Lambda function + API Gateway for queries
- CloudFront distribution for PMTiles

### Check Status

```bash
# Show stack outputs (API endpoint, bucket name, etc.)
pulumi stack output

# Get specific output
pulumi stack output api_endpoint
pulumi stack output cloudfront_domain

# View full stack state
pulumi stack

# Export stack to JSON
pulumi stack export > stack-backup.json
```

### Update Infrastructure

```bash
# After modifying pulumi/__main__.py
pulumi preview  # see what will change
pulumi up       # apply changes
```

### View Logs

```bash
# CloudWatch logs for Batch jobs
aws logs tail /aws/batch/osm-h3 --follow --region us-west-2

# Lambda logs
aws logs tail /aws/lambda/osm-pois-api --follow --region us-west-2

# List log streams
aws logs describe-log-streams \
  --log-group-name /aws/batch/osm-h3 \
  --order-by LastEventTime \
  --descending \
  --max-items 10
```

### Destroy Infrastructure

```bash
cd pulumi

# Preview what will be deleted
pulumi destroy --preview-only

# Destroy all resources
pulumi destroy

# Destroy non-interactively
pulumi destroy --yes

# Remove stack entirely
pulumi stack rm dev
```

**Note**: S3 bucket must be empty before destruction. Delete objects first:

```bash
aws s3 rm s3://$(pulumi stack output bucket_name) --recursive
```

### Stack Management

```bash
# List all stacks
pulumi stack ls

# Switch to different stack
pulumi stack select prod

# Rename stack
pulumi stack rename dev staging

# Clone stack (copy config to new stack)
pulumi stack init staging --copy-config-from dev
```

### Refresh State

If resources were modified outside Pulumi (e.g., via AWS Console):

```bash
# Sync Pulumi state with actual AWS resources
pulumi refresh
```

## Running the Pipeline

There are two ways to run the OSM-H3 pipeline: using the **Step Functions state machine** (recommended for production) or the **Pipeline CLI** (for development/testing).

### Option 1: Step Functions State Machine (Recommended)

The state machine orchestrates all stages automatically with proper error handling and retry logic.

#### Get the State Machine ARN

```bash
cd pulumi
pulumi stack output state_machine_arn
```

#### Start an Execution

```bash
# Generate a unique run ID (e.g., timestamp)
RUN_ID="run-$(date +%Y%m%d-%H%M%S)"

# Start the execution
aws stepfunctions start-execution \
  --state-machine-arn $(pulumi stack output state_machine_arn) \
  --name "$RUN_ID" \
  --input "{\"run_id\": \"$RUN_ID\"}"
```

**Required Input Parameters:**
- `run_id`: Unique identifier for this pipeline run (used for S3 paths)

**Optional Environment Variables (set in Pulumi config):**
- `PLANET_URL`: OSM data source (default: configured in `pulumi/config.py`)
- `MAX_RESOLUTION`: H3 resolution for sharding (default: 7)
- `MAX_NODES_PER_SHARD`: Max nodes per shard (default: 1000000)

#### Monitor Execution

```bash
# Get execution ARN from start-execution output, or construct it:
EXECUTION_ARN="arn:aws:states:REGION:ACCOUNT_ID:execution:osm-h3-pipeline-sfn:$RUN_ID"

# Check execution status
aws stepfunctions describe-execution \
  --execution-arn "$EXECUTION_ARN"

# Get execution history (all state transitions)
aws stepfunctions get-execution-history \
  --execution-arn "$EXECUTION_ARN"

# Watch in AWS Console
# https://console.aws.amazon.com/states/home?region=REGION#/statemachines
```

#### State Machine Flow

The pipeline executes these stages sequentially:

1. **Download Job**: Download planet.osm.pbf to S3
2. **Shard Job**: Split PBF into H3-indexed shards
3. **Get Manifest**: Lambda reads shard manifest from S3
4. **Process Shards**: Map state runs up to 50 parallel Batch jobs per shard
5. **Merge Job**: Combine all shard Parquet files
6. **Tiles Job**: Generate PMTiles for visualization

Each Batch job waits for the previous stage to complete before starting.

#### Check Job Logs

```bash
# View CloudWatch logs for a specific stage
aws logs tail /aws/batch/osm-h3-download --follow
aws logs tail /aws/batch/osm-h3-sharder --follow
aws logs tail /aws/batch/osm-h3-processor --follow
aws logs tail /aws/batch/osm-h3-merger --follow
aws logs tail /aws/batch/osm-h3-tiles --follow
```

#### Stop a Running Execution

```bash
aws stepfunctions stop-execution \
  --execution-arn "$EXECUTION_ARN" \
  --cause "Manual stop"
```

## Troubleshooting

### Deployment fails with "role not found"
IAM roles need time to propagate. Wait 10-15 seconds and retry:
```bash
pulumi up --yes
```

### "Bucket already exists" error
S3 bucket names are globally unique. Change the project name or delete the existing bucket.

### Jobs stuck in RUNNABLE
Check Batch compute environment:
```bash
aws batch describe-compute-environments \
  --compute-environments osm-h3-compute \
  --query "computeEnvironments[0].status"
```

### API returns empty results
Verify Athena table has data:
```bash
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM osm_pois.pois" \
  --result-configuration "OutputLocation=s3://$(pulumi stack output bucket_name)/athena-results/" \
  --query-execution-context "Database=osm_pois"
```

### Pipeline fails at specific stage
Check CloudWatch logs for the failed job:
```bash
# Find failed job ID
aws batch list-jobs --job-queue osm-h3-queue --job-status FAILED

# Get logs (replace JOB_ID)
aws logs tail /aws/batch/osm-h3 --follow --filter-pattern "JOB_ID"
```

## Architecture

```
planet.pbf → [download] → S3
              ↓
           [shard] → shards/*.pbf
              ↓
           [process] → parquet/*.parquet (parallel Batch jobs)
              ↓
           [merge] → merged.parquet → Athena table
              ↓
           [tiles] → pois.pmtiles → CloudFront
```

## License

MIT
