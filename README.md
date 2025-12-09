# OSM POI Pipeline

Serverless pipeline for processing OpenStreetMap POI data for all US states/territories.

## Architecture

```
AWS Batch (Spot) → Parquet/S3 → Athena → API Gateway + Lambda
```

- **Processing**: AWS Batch with Spot instances (~$3-5 per full run)
- **Storage**: S3 with Parquet files (~$0.02/GB/month)
- **Query**: Athena with partition pruning for fast bbox queries
- **API**: API Gateway + Lambda (free tier)

## Quick Start

```bash
# 1. Setup AWS infrastructure
./setup-aws.sh

# 2. Build and push Docker image
./build-and-push.sh

# 3. Setup Athena + Lambda API
./setup-athena-api.sh

# 4. Run batch jobs for all states
./run-all-states.sh

# 5. Query the API
source .env
curl "${API_ENDPOINT}/pois?bbox=-122.5,37.7,-122.3,37.9"
```

## Documentation

See [WORKFLOW.md](WORKFLOW.md) for detailed documentation including:
- Step-by-step setup instructions
- API endpoints and query parameters
- POI categories
- Cost breakdown
- Troubleshooting

## Project Structure

```
osm-h3/
├── batch/                    # Batch processing container
│   ├── Dockerfile
│   ├── process_region.py
│   └── requirements.txt
├── athena/                   # Athena + Lambda API
│   ├── create_table.sql
│   └── lambda_handler.py
├── setup-aws.sh              # AWS Batch infrastructure
├── setup-athena-api.sh       # Athena + API Gateway setup
├── build-and-push.sh         # Build container image
├── run-all-states.sh         # Submit batch jobs
├── monitor.sh                # Monitor job progress
└── WORKFLOW.md               # Full documentation
```
