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

## Pipeline CLI

Once infrastructure is deployed, use `pipeline_cli.py` to orchestrate the data processing pipeline.

### Run the Pipeline

```bash
# Run the full pipeline (download → shard → process → merge → tiles)
./pipeline_cli.py run

# Start from a specific stage (e.g., skip download if planet.pbf already exists)
./pipeline_cli.py run --start-at shard

# Use a custom region extract instead of full planet
./pipeline_cli.py run --planet-url https://download.geofabrik.de/north-america/us/california-latest.osm.pbf

# Custom run ID
./pipeline_cli.py run --run-id california-test-20240101

# Override max H3 resolution or nodes per shard
./pipeline_cli.py run --max-resolution 9 --max-nodes-per-shard 2000000

# Async mode: submit all jobs with dependencies and return immediately
./pipeline_cli.py run --async
# Then monitor in another terminal:
./pipeline_cli.py status --watch
```

Available stages (use with `--start-at`):
- `download`: Download planet/region PBF file
- `shard`: Split PBF into H3-indexed shards
- `process`: Extract POIs from each shard in parallel
- `merge`: Combine all POI Parquet files
- `tiles`: Generate PMTiles for visualization

#### Synchronous vs Async Mode

**Synchronous (default)**: The CLI waits for each stage to complete before moving to the next. This is useful for seeing immediate progress and errors.

```bash
./pipeline_cli.py run  # Blocks until all stages complete
```

**Async mode (`--async`)**: Submits all jobs with AWS Batch dependencies and returns immediately. Jobs automatically wait for their dependencies to complete. Use this for long-running pipelines where you want to monitor separately.

```bash
# Terminal 1: Submit jobs and return
./pipeline_cli.py run --async

# Terminal 2: Watch progress
./pipeline_cli.py status --watch
```

### Monitor Pipeline Progress

```bash
# Watch job status continuously
./pipeline_cli.py status --watch

# Single snapshot
./pipeline_cli.py status

# Custom refresh interval (default 30s)
./pipeline_cli.py status --watch --interval 10
```

The status command shows:
- Job counts by status (SUBMITTED, RUNNING, SUCCEEDED, FAILED, etc.)
- Currently running jobs with start times
- Total Parquet files written to S3

### Pipeline CLI Options

All commands support:
- `--region`: AWS region (defaults to `AWS_REGION` environment variable)
- `--project-name`: Resource prefix (default: `osm-h3`)
- `--bucket`: Override S3 bucket name
- `--job-queue`: Override AWS Batch job queue name

## Environment Variables

The pipeline respects these environment variables:

- `AWS_REGION` / `AWS_DEFAULT_REGION`: Default AWS region
- `MAX_RESOLUTION`: H3 resolution for sharding (default: 7)
- `MAX_NODES_PER_SHARD`: Max nodes per shard file (default: 1000000)

Override via CLI flags or export before running:

```bash
export AWS_REGION=us-west-2
export MAX_RESOLUTION=8
./pipeline_cli.py run
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
