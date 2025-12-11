# OSM-H3 Infrastructure

AWS CDK infrastructure for the OSM-H3 processing pipeline. The stack provisions
the S3 bucket, ECR repos, Batch compute environment, job queue, and job
definitions that the Python runner submits jobs to.

## Architecture

```
┌─────────┬──────────┬────────────┬──────────┬────────────┐
│Download │  Shard   │ Process ×N │  Merge   │   Tiles    │
└─────────┴──────────┴────────────┴──────────┴────────────┘
           ▲                          ▲
           └──── AWS Batch jobs submitted by scripts/run_pipeline.py ────┘
```

## Stacks

### OsmH3InfraStack

Core infrastructure resources:

- **S3 Bucket** - Data storage with intelligent tiering (90/180 day archive)
- **ECR Repositories** - Container images for processor, sharder, tiles
- **VPC** - Uses default VPC with public subnets
- **Batch Compute Environment** - Spot instances (m6i, m5, r6i, r5)
- **Job Queue** - Single queue for all job types
- **Job Definitions** - Download, Sharder, Processor, Merge, Tiles
- **IAM Roles** - Least-privilege roles for ECS tasks

## Prerequisites

1. Node.js 18+
2. AWS CLI configured with appropriate credentials
3. CDK bootstrapped in target account/region:
   ```bash
   cd infra/cdk
   npx cdk bootstrap aws://ACCOUNT/REGION
   ```

## Deployment

```bash
cd infra/cdk
npm install

# Synthesize CloudFormation (preview)
npm run synth

# Deploy infrastructure
npm run deploy

# (Optional) Change defaults via context (projectName)
npx cdk deploy --all -c projectName=my-prefix
```

## Configuration

Context parameters in `cdk.json`:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `projectName` | `osm-h3` | Resource name prefix |

Override via CLI:
```bash
npx cdk deploy --all -c projectName=my-prefix
```

## Running the Pipeline

Once the infrastructure stack is deployed and container images are pushed to ECR,
run the Batch pipeline with:

```bash
make run                      # download → shard → process → merge → tiles
make run START_AT=process     # resume a previous run
make run PLANET_URL=...       # override the download source
```

The command wraps `scripts/run_pipeline.py`, which loads credentials from the
standard AWS CLI configuration and blocks until each stage completes. All Batch
jobs are visible in the AWS console (Job queue: `<projectName>-queue`).

## Building Container Images

After deployment, build and push container images:

```bash
# Get ECR URIs from CDK outputs
PROCESSOR_URI=$(aws cloudformation describe-stacks \
    --stack-name OsmH3InfraStack \
    --query "Stacks[0].Outputs[?OutputKey=='ProcessorRepoUri'].OutputValue" \
    --output text)

SHARDER_URI=$(aws cloudformation describe-stacks \
    --stack-name OsmH3InfraStack \
    --query "Stacks[0].Outputs[?OutputKey=='SharderRepoUri'].OutputValue" \
    --output text)

TILES_URI=$(aws cloudformation describe-stacks \
    --stack-name OsmH3InfraStack \
    --query "Stacks[0].Outputs[?OutputKey=='TilesRepoUri'].OutputValue" \
    --output text)

# Login to ECR
aws ecr get-login-password | docker login --username AWS --password-stdin "${PROCESSOR_URI%%/*}"

# Build and push processor
docker buildx build --platform linux/amd64 -t "$PROCESSOR_URI:latest" ../batch --push

# Build and push sharder
docker buildx build --platform linux/amd64 -t "$SHARDER_URI:latest" ../sharding --push

# Build and push tiles
docker buildx build --platform linux/amd64 -t "$TILES_URI:latest" ../tiles --push
```

## S3 Data Layout

```
s3://osm-h3-data-{account}/
├── runs/
│   └── {runId}/
│       ├── planet.osm.pbf           # Downloaded planet file
│       ├── shards/
│       │   ├── manifest.json        # Shard definitions (GeoJSON)
│       │   ├── {h3_index}/
│       │   │   └── data.parquet     # Per-shard output
│       │   └── ...
│       └── output/
│           └── pois.parquet         # Merged final output
├── parquet/
│   └── pois.parquet                 # Latest merged output (for tiles)
└── tiles/
    └── pois.pmtiles                 # Generated vector tiles
```

## Cost Optimization

- **Spot Instances** - Compute environment uses spot with 80% bid
- **Intelligent Tiering** - S3 objects auto-archive after 90/180 days
- **Run Lifecycle** - Old runs expire after 30 days
- **Right-sizing** - Job definitions tuned for each workload

## Cleanup

```bash
# Destroy all stacks (keeps S3 bucket)
npx cdk destroy --all

# To also delete S3 bucket, first empty it:
aws s3 rm s3://osm-h3-data-{account} --recursive
```
