# OSM-H3 Pipeline

Serverless pipeline for processing OpenStreetMap POI data using H3 spatial indexing and AWS Batch.

## Architecture

```
┌──────────┬──────────┬────────────┬────────────┬────────────┐
│ Download │  Shard   │ Process ×N │   Merge    │  Tiles     │
│ (Batch)  │  (Rust)  │   (Batch)  │  (Batch)   │  (Batch)   │
└──────────┴──────────┴────────────┴────────────┴────────────┘
        ▲                  ▲
        │                  │
        └───── Python orchestrator submits AWS Batch jobs ─────┘
```

- **Orchestration**: Lightweight Python script that submits AWS Batch jobs sequentially
- **Processing**: AWS Batch with Spot instances
- **Sharding**: Rust-based H3 spatial partitioning
- **Storage**: S3 with Parquet + PMTiles
- **Query**: Athena with partition pruning / CloudFront for tiles
- **Infrastructure**: AWS CDK (TypeScript) for Batch/S3/ECR/IAM

## Quick Start

```bash
# 1. Deploy infrastructure
make deploy

# 2. Build and push container images
make build-images

# 3. Run the pipeline (AWS Batch download → shard → process → merge → tiles)
make run [RUN_ID=custom-id]

# 4. Check status
make status
```

## Pipeline Stages

| Stage | Description | Duration |
|-------|-------------|----------|
| **Download** | Fetch planet.osm.pbf (~70GB) | ~2 hours |
| **Shard** | Rust H3 partitioner creates spatial shards | ~4 hours |
| **Process** | Fan-out to many AWS Batch jobs (one per H3 shard) | ~2 hours |
| **Merge** | Combine shard outputs to final Parquet | ~30 min |
| **Tiles** | Generate PMTiles with tippecanoe | ~1 hour |

## Batch Pipeline Runner

The `make run` target wraps `scripts/run_pipeline.py`, a small Python program that
submits AWS Batch jobs for each stage. Key options:

```bash
# Default (download full planet, generate tiles)
make run

# Custom run ID + resume from a specific stage
make run RUN_ID=planet-20250101-010101 START_AT=process

# US-only pipeline (Geofabrik extract)
make run-us                  # alias for make run PLANET_URL=...

# Pass any script flag directly
python scripts/run_pipeline.py --help
```

The runner automatically infers the S3 bucket (`osm-h3-data-<account>`) and job
queue (`osm-h3-queue`) created by CDK. It will block until each stage finishes
and prints progress as process jobs complete.

## Project Structure

```
osm-h3/
├── infra/cdk/              # AWS CDK infrastructure
│   ├── lib/
│   │   └── infrastructure-stack.ts   # S3, ECR, Batch, IAM
│   └── bin/pipeline.ts
├── batch/                  # Processing container
│   ├── Dockerfile
│   ├── processor.py        # Unified download/process/merge stages
│   └── process_region.py   # Legacy per-region processor
├── sharding/               # Rust H3 sharder
│   ├── Cargo.toml
│   ├── Dockerfile
│   └── src/main.rs
├── tiles/                  # PMTiles generator
│   ├── Dockerfile
│   └── generate_pmtiles.py
├── athena/                 # Query layer
│   ├── create_table.sql
│   └── lambda_handler.py
├── frontend/               # React map viewer
└── Makefile               # Build/deploy commands
```

## Commands

```bash
make help              # Show all commands

# Infrastructure
make deploy            # Deploy CDK stacks
make synth             # Preview CloudFormation
make destroy           # Tear down stacks

# Container Images
make build-images      # Build and push all images
make build-processor   # Build processor image only
make build-sharder     # Build sharder image only
make build-tiles       # Build tiles image only

# Pipeline
make run               # Run download → shard → process → merge → tiles
make run-us            # Run pipeline on Geofabrik US extract
make status            # List running AWS Batch jobs

# Development
make check             # Type-check CDK and Rust
make clean             # Remove build artifacts
```

## Frontend Map Viewer

```bash
cd frontend
npm install
cp .env.example .env
# Set VITE_PMTILES_URL=https://<cloudfront>/tiles/pois.pmtiles
npm run dev
```

## Documentation

- [Infrastructure Guide](infra/README.md) - CDK deployment, configuration, S3 layout
- [Workflow Details](WORKFLOW.md) - API endpoints, POI categories, cost breakdown

## Cost Estimate

| Resource | Monthly Cost |
|----------|-------------|
| Batch (Spot) | ~$5-10 per full run |
| S3 Storage | ~$0.02/GB |
| **Total** | ~$10-20/month for weekly runs |
