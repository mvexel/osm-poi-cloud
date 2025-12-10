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
# 1. Setup AWS infrastructure (.env is generated here)
./setup-aws.sh

# 2. Build and push Docker image
./build-and-push.sh

# 3. Setup Athena + Lambda API
./setup-athena-api.sh

# 4. Run batch jobs for all states
./run-all-states.sh

# 5. (Optional) Setup CloudFront + tiles job definition
./setup-tiles.sh

# 6. Query the API / generate PMTiles
source .env
curl "${API_ENDPOINT}/pois?bbox=-122.5,37.7,-122.3,37.9"
# Submit PMTiles Batch job after parquet files are ready
./generate-tiles.sh
```

### Frontend map viewer (optional)

```bash
cd frontend
npm install
cp .env.example .env   # set VITE_API_BASE to your API Gateway URL (optional if streaming PMTiles only)
nano .env              # set VITE_PMTILES_URL=https://<cloudfront>/pois.pmtiles to stream tiles client-side
npm run dev             # opens http://localhost:5173
```

When `VITE_PMTILES_URL` is configured, the frontend streams points directly from the PMTiles archive and
skips live API calls (counts come from the API so they stay hidden, but the class dropdown still filters locally).
Clear `VITE_PMTILES_URL` to return to API-driven mode.

## Documentation

See [WORKFLOW.md](WORKFLOW.md) for detailed documentation including:
- Step-by-step setup instructions
- API endpoints and query parameters
- POI categories
- Cost breakdown
- Troubleshooting

Planning the Infrastructure-as-Code migration? See [docs/iac-plan.md](docs/iac-plan.md).

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
├── scripts/
│   └── common.sh             # Shared helpers (env loading, AWS/Docker guards)
├── setup-aws.sh              # AWS Batch infrastructure
├── setup-athena-api.sh       # Athena + API Gateway setup
├── setup-tiles.sh            # Tiles ECR/Batch + CloudFront
├── generate-tiles.sh         # Submit PMTiles Batch job
├── build-and-push.sh         # Build container image
├── run-all-states.sh         # Submit batch jobs
├── monitor.sh                # Monitor job progress
└── WORKFLOW.md               # Full documentation
```
