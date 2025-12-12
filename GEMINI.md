# Project Overview

This project is a scalable pipeline for processing OpenStreetMap (OSM) data into a queryable and visualizable format. It uses a serverless architecture on AWS, orchestrated with Pulumi for infrastructure as code

The pipeline ingests an OSM PBF file, processes it into H3-indexed Points of Interest (POIs), stores the data in Parquet format in S3, and makes it available for querying via Athena and for visualization via PMTiles.

The main components are:
- **Data Pipeline:** A series of AWS Batch jobs that download, shard, process, merge, and generate tiles from OSM data.
- **Infrastructure as Code:** Pulumi scripts to deploy and manage all the necessary AWS resources.
- **Frontend:** A React-based web application to visualize the POI data on a map.

## Building and Running

### 1. Deploy Infrastructure

The infrastructure is managed by Pulumi.

```bash
# Navigate to the Pulumi directory
cd pulumi

# Install Python dependencies
uv pip install -r requirements.txt

# Login to the Pulumi backend (local or cloud)
pulumi login --local

# Initialize a new stack (e.g., dev)
pulumi stack init dev

# Configure the AWS region
pulumi config set aws:region us-west-2

# Preview and deploy the infrastructure
pulumi up
```

This will provision:
- S3 bucket for data storage
- ECR repositories for Docker images
- AWS Batch compute environment, job queue, and job definitions
- IAM roles
- VPC, subnets, and security groups
- Step functions and state machine to orchestrate the pipeline execution
- CloudFront distribution (optional)


### 2. Run the Data Pipeline



### 3. Run the Frontend

The frontend is a React application that can be run locally for development.

```bash
# Navigate to the frontend directory
cd frontend

# Install dependencies
npm install

# Create a .env file from the example
cp .env.example .env
```

You then need to configure the `VITE_PMTILES_URL` in the `.env` file to point to the `pois.pmtiles` file in your S3 bucket. You can get the URL from the Pulumi stack output:

```bash
# Get the tiles URL from the Pulumi output
pulumi stack output tiles_url
```

Then, start the development server:

```bash
# Start the development server
npm run dev
```

## Development Conventions

- **Infrastructure:** All infrastructure is managed by Pulumi in the `pulumi/` directory. Changes to the infrastructure should be made in the Python files in this directory and deployed with `pulumi up`.
- **Pipeline Logic:** The core data processing logic is in the `stack/` directory, with each subdirectory corresponding to a Docker image for a pipeline stage.
- **Orchestration:** The `pipeline_cli.py` and `scripts/run_pipeline.py` handle the orchestration of the AWS Batch jobs.
- **Frontend:** The frontend is a standard React/Vite application. Components are located in `frontend/src/components`.
- **Styling:** The frontend uses CSS modules for styling, with global styles in `frontend/src/styles.css`. Map styling is handled with a JSON file in `frontend/src/mapstyles`.
