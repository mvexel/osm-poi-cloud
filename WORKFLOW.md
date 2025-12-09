# OSM POI Pipeline - Complete Workflow

Process all US states/territories to Parquet, query via Athena + API Gateway.

## Overview

```
┌────────────────┐     ┌─────────────────┐     ┌──────────────────┐
│  AWS Batch     │     │  S3 Parquet     │     │  API Gateway     │
│  (56 jobs)     │────▶│  (partitioned)  │────▶│  + Lambda/Athena │
│  → Parquet/S3  │     │                 │     │  /pois?bbox=...  │
└────────────────┘     └─────────────────┘     └──────────────────┘
     ~$3-5              ~$0.02/GB/mo           ~$0/mo (free tier)
```

**Architecture**: Fully serverless AWS-native solution using:
- **AWS Batch** (Spot instances) for parallel processing
- **S3** for Parquet storage with partition columns (`lon_bucket`, `lat_bucket`)
- **Athena** for serverless SQL queries with partition pruning
- **API Gateway + Lambda** for REST API

## Quick Start

```bash
# 1. Setup AWS infrastructure (one-time, ~5 min)
./setup-aws.sh

# 2. Build and push Docker image (~3 min)
./build-and-push.sh

# 3. Setup Athena + Lambda API (~2 min)
./setup-athena-api.sh

# 4. Run all 56 state jobs (~2-3 hours, ~$3-5)
./run-all-states.sh

# 5. Test the API
source .env
curl "${API_ENDPOINT}/health"
curl "${API_ENDPOINT}/pois?bbox=-122.5,37.7,-122.3,37.9"
```

## Detailed Steps

### Step 1: AWS Setup

Creates: S3 bucket, ECR repo, IAM roles, Batch compute environment, job queue, job definition.

```bash
./setup-aws.sh
```

This creates a `.env` file with configuration for subsequent scripts.

### Step 2: Build Container

```bash
./build-and-push.sh
```

Builds the Docker image and pushes to ECR.

### Step 3: Setup Athena API

```bash
./setup-athena-api.sh
```

Creates:
- Athena database and external table
- Lambda function for query handling
- API Gateway HTTP API with routes

### Step 4: Run Batch Jobs

```bash
./run-all-states.sh
```

Submits 56 jobs (50 states + DC + 5 territories). Jobs run in parallel on Spot instances.

Each job:
1. Downloads OSM PBF from Geofabrik
2. Filters to POI-relevant features
3. Converts to Parquet with partition columns
4. Uploads to S3

Monitor progress:
```bash
./monitor.sh
# or
aws batch list-jobs --job-queue osm-h3-queue --job-status RUNNING
```

### Step 5: Query the API

Once jobs complete, data is immediately queryable:

```bash
source .env

# Health check
curl "${API_ENDPOINT}/health"

# Get POIs in bounding box (San Francisco)
curl "${API_ENDPOINT}/pois?bbox=-122.5,37.7,-122.3,37.9"

# Filter by class
curl "${API_ENDPOINT}/pois?bbox=-122.5,37.7,-122.3,37.9&class=restaurant"

# List available classes
curl "${API_ENDPOINT}/classes"
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /classes` | List POI classes with counts |
| `GET /pois?bbox=minLon,minLat,maxLon,maxLat` | Query POIs in bbox |

### Query Parameters for /pois

| Parameter | Description | Default |
|-----------|-------------|---------|
| `bbox` | Bounding box (required): `minLon,minLat,maxLon,maxLat` | - |
| `class` | Filter by POI class (e.g., `restaurant`) | all |
| `limit` | Max results (1-10000) | 1000 |

### Response Format

Returns GeoJSON FeatureCollection:
```json
{
  "type": "FeatureCollection",
  "count": 42,
  "bbox": [-122.5, 37.7, -122.3, 37.9],
  "features": [
    {
      "type": "Feature",
      "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
      "properties": {
        "osm_id": "123456",
        "name": "Joe's Diner",
        "class": "restaurant",
        "cuisine": "american",
        ...
      }
    }
  ]
}
```

## How Partition Pruning Works

Each POI has partition columns:
- `lon_bucket`: floor(longitude), e.g., -123 for lon=-122.5
- `lat_bucket`: floor(latitude), e.g., 37 for lat=37.8

When you query `bbox=-122.5,37.7,-122.3,37.9`, Athena:
1. First filters to files where `lon_bucket IN (-123, -122)` and `lat_bucket IN (37)`
2. Then applies exact coordinate filtering within those files

This minimizes data scanned, keeping costs low.

## Cost Summary

| Component | Cost |
|-----------|------|
| AWS Batch (Spot) | ~$3-5 one-time per full run |
| S3 storage (~1GB Parquet) | ~$0.02/month |
| Athena queries | ~$5/TB scanned (partition pruning minimizes this) |
| API Gateway | Free tier: 1M requests/month |
| Lambda | Free tier: 1M requests/month |

**Estimated monthly cost for light usage: ~$0-1/month**

## Files

```
osm-h3/
├── batch/                    # Batch processing container
│   ├── Dockerfile
│   ├── process_region.py     # Main processing script
│   └── requirements.txt
├── athena/                   # Athena + Lambda API
│   ├── create_table.sql      # Athena table definition
│   └── lambda_handler.py     # API Lambda function
├── setup-aws.sh              # Create AWS Batch infrastructure
├── setup-athena-api.sh       # Create Athena + Lambda + API Gateway
├── build-and-push.sh         # Build & push Docker image
├── run-all-states.sh         # Submit Batch jobs
├── monitor.sh                # Watch job progress
├── README.md                 # Project overview
└── WORKFLOW.md               # This file (detailed docs)
```

## POI Classes

The pipeline extracts and classifies POIs into these categories:

| Class | Examples |
|-------|----------|
| restaurant | restaurants, diners, food courts |
| cafe_bakery | cafes, coffee shops, tea houses |
| bar_pub | bars, pubs, biergartens |
| fast_food | fast food, food trucks, ice cream |
| grocery | supermarkets, convenience stores |
| specialty_food | bakeries, butchers, delis |
| retail | malls, department stores, shops |
| personal_services | spas, salons, laundry |
| professional_services | offices, coworking spaces |
| finance | banks, ATMs |
| lodging | hotels, hostels, campgrounds |
| transport | bus stations, train stations, airports |
| auto_services | gas stations, car washes, repair |
| parking | parking lots, bike parking |
| healthcare | hospitals, clinics, pharmacies |
| education | schools, universities, libraries |
| government | town halls, police, post offices |
| community | community centers, social facilities |
| religious | places of worship |
| culture | museums, theaters, galleries |
| entertainment | cinemas, nightclubs, arcades |
| sports_fitness | gyms, sports centers, pools |
| parks_outdoors | parks, playgrounds, nature reserves |
| landmark | attractions, monuments, viewpoints |
| animal_services | veterinarians, pet shops |

## Troubleshooting

### Jobs stuck in RUNNABLE
Spot capacity may be limited. Check compute environment:
```bash
aws batch describe-compute-environments --compute-environments osm-h3-compute
```

### Job failed
View logs:
```bash
aws logs tail /aws/batch/osm-h3 --follow
```

### Athena query timeout
The Lambda has a 30-second timeout. For large bbox queries:
- Reduce bbox size (max 5 degrees per side)
- Use class filter to reduce results
- Reduce limit parameter

### Empty results
Verify data was uploaded:
```bash
aws s3 ls s3://${S3_BUCKET}/parquet/
```

Refresh Athena table metadata:
```bash
aws athena start-query-execution \
    --query-string "MSCK REPAIR TABLE osm_pois.pois" \
    --result-configuration "OutputLocation=s3://${S3_BUCKET}/athena-results/"
```
