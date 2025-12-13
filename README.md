# OSM-H3 Pipeline

A scalable AWS Batch-based pipeline for processing OpenStreetMap data into H3-indexed POIs, stored in Parquet format and served via PMTiles.

## Pulumi Deployment Guide

### Prerequisites

- [Pulumi CLI](https://www.pulumi.com/docs/get-started/install/) installed
- AWS credentials configured (`aws configure` or environment variables)
- Python 3.13+ with `uv` (see Local Development Setup above)

### Initial Setup

```bash
# Login to Pulumi
pulumi login --local  

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

# Start the execution (execution name becomes the run_id)
aws stepfunctions start-execution \
  --state-machine-arn $(pulumi stack output state_machine_arn) \
  --name "$RUN_ID"
```

**Note:** The execution name automatically becomes the `run_id` via the Init state. All pipeline stages will use `/run/{run_id}` as their S3 prefix for input and output files.

**Optional Environment Variables (set in Pulumi config):**
- `PLANET_URL`: OSM data source (default: configured in `pulumi/config.py`)
- `MAX_ZOOM`: Max Web Mercator zoom for sharding (optional; defaults are in `stack/sharding/src/main.rs`)
- `MAX_NODES_PER_SHARD`: Max nodes per shard (optional; defaults are in `stack/sharding/src/main.rs`)

Override sharder settings via Pulumi config:

```bash
cd pulumi
pulumi config set max_zoom 11
pulumi config set max_nodes_per_shard 1000000
pulumi up
```

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

1. **Init**: Initializes execution state with `run_id` from execution name
2. **Download Job**: Download planet.osm.pbf to S3 at `/run/{run_id}/planet.osm.pbf`
3. **Shard Job**: Split PBF into quadtree tiles, outputs manifest to `/run/{run_id}/shards/manifest.json`
4. **Get Manifest**: Lambda reads shard manifest from S3
5. **Process Shards**: Map state runs up to 50 parallel Batch jobs per shard
6. **Merge Job**: Combine all shard Parquet files into `/run/{run_id}/output/pois.parquet`
7. **Tiles Job**: Generate PMTiles for visualization at `/run/{run_id}/tiles/pois.pmtiles`

Each stage receives `INPUT_PREFIX` and `OUTPUT_PREFIX` environment variables (both set to `/run/{run_id}`), ensuring all jobs use consistent paths without hardcoded assumptions.

#### Restart From Processor Stage

If `/run/{RUN_ID}/planet.osm.pbf` and `/run/{RUN_ID}/shards/manifest.json` already exist in S3, you can rerun only the **processor** jobs (and then **merge**/**tiles**) without rerunning download/sharding.

```bash
cd pulumi

RUN_ID="run-20251212-123456"  # reuse the existing run ID
BUCKET="$(pulumi stack output data_bucket_name)"
QUEUE="$(pulumi stack output job_queue_name)"
JOB_DEFS_JSON="$(pulumi stack output --json job_definition_arns)"
PROCESSOR_JOB_DEF_ARN="$(echo "$JOB_DEFS_JSON" | jq -r .processor)"
MERGER_JOB_DEF_ARN="$(echo "$JOB_DEFS_JSON" | jq -r .merger)"
TILES_JOB_DEF_ARN="$(echo "$JOB_DEFS_JSON" | jq -r .tiles)"

# Rerun processor for shards that don't have output yet
aws s3 cp "s3://$BUCKET/run/$RUN_ID/shards/manifest.json" - | \
  jq -r '.features[].properties | "\(.shard_id) \(.z) \(.x) \(.y)"' | \
  while read -r SHARD_ID Z X Y; do
    aws s3 ls "s3://$BUCKET/run/$RUN_ID/shards/$SHARD_ID/data.parquet" >/dev/null 2>&1 && continue
    aws s3 ls "s3://$BUCKET/run/$RUN_ID/shards/$SHARD_ID/_EMPTY" >/dev/null 2>&1 && continue
    aws batch submit-job \
      --job-name "osm-h3-process-$RUN_ID-$SHARD_ID" \
      --job-queue "$QUEUE" \
      --job-definition "$PROCESSOR_JOB_DEF_ARN" \
      --container-overrides "{\"environment\":[{\"name\":\"RUN_ID\",\"value\":\"$RUN_ID\"},{\"name\":\"INPUT_PREFIX\",\"value\":\"/run/$RUN_ID\"},{\"name\":\"OUTPUT_PREFIX\",\"value\":\"/run/$RUN_ID\"},{\"name\":\"SHARD_ID\",\"value\":\"$SHARD_ID\"},{\"name\":\"SHARD_Z\",\"value\":\"$Z\"},{\"name\":\"SHARD_X\",\"value\":\"$X\"},{\"name\":\"SHARD_Y\",\"value\":\"$Y\"}]}"
  done

# After processor jobs complete, rerun merge + tiles
aws batch submit-job \
  --job-name "osm-h3-merge-$RUN_ID" \
  --job-queue "$QUEUE" \
  --job-definition "$MERGER_JOB_DEF_ARN" \
  --container-overrides "{\"environment\":[{\"name\":\"RUN_ID\",\"value\":\"$RUN_ID\"},{\"name\":\"INPUT_PREFIX\",\"value\":\"/run/$RUN_ID\"},{\"name\":\"OUTPUT_PREFIX\",\"value\":\"/run/$RUN_ID\"}]}"

aws batch submit-job \
  --job-name "osm-h3-tiles-$RUN_ID" \
  --job-queue "$QUEUE" \
  --job-definition "$TILES_JOB_DEF_ARN" \
  --container-overrides "{\"environment\":[{\"name\":\"RUN_ID\",\"value\":\"$RUN_ID\"},{\"name\":\"OUTPUT_PREFIX\",\"value\":\"/run/$RUN_ID\"}]}"
```

To force reprocessing of a shard, delete its existing output (`/run/{RUN_ID}/shards/{SHARD_ID}/data.parquet` or `/run/{RUN_ID}/shards/{SHARD_ID}/_EMPTY`) and resubmit that shard.

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

### Pipeline Flow

```
planet.pbf → [download] → S3:/run/{run_id}/planet.osm.pbf
              ↓
           [shard] → /run/{run_id}/shards/manifest.json
              ↓
           [process] → /run/{run_id}/shards/*/data.parquet (parallel Batch jobs)
              ↓
           [merge] → /run/{run_id}/output/pois.parquet + parquet/pois.parquet
              ↓
           [tiles] → /run/{run_id}/tiles/pois.pmtiles + tiles/pois.pmtiles
```

### Design Principles

**Single Source of Truth**: The Step Functions state machine defines all S3 paths via `INPUT_PREFIX` and `OUTPUT_PREFIX` environment variables. Jobs don't hardcode paths or make assumptions about directory structure.

**Decoupled Components**: 
- The Rust sharder binary has zero AWS dependencies - it reads a local PBF file and writes GeoJSON to stdout
- The sharder's entrypoint shell script handles S3 I/O separately from the core logic
- Python batch jobs use boto3 directly instead of a custom storage abstraction layer

**Run Isolation**: Each pipeline execution is identified by a unique `run_id` (from the Step Functions execution name), and all artifacts are stored under `/run/{run_id}/`, making it easy to:
- Run multiple pipelines concurrently
- Inspect outputs from past runs
- Restart failed runs without affecting others

## License

MIT
