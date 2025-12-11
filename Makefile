# OSM-H3 Pipeline
# Run `make help` for available commands

.PHONY: help deploy build-images synth run clean

# Default region
AWS_REGION ?= us-west-2
# Attempt to auto-detect AWS account; can be overridden via env var
AWS_ACCOUNT_ID ?= $(shell aws sts get-caller-identity --query Account --output text 2>/dev/null)
PROJECT_NAME ?= osm-h3
RUN_ID ?=
PLANET_URL ?=
START_AT ?=
PYTHON ?= python3

help:
	@echo "OSM-H3 Pipeline Commands"
	@echo ""
	@echo "Infrastructure:"
	@echo "  make deploy          Deploy CDK stacks"
	@echo "  make synth           Preview CloudFormation output"
	@echo "  make destroy         Tear down CDK stacks"
	@echo ""
	@echo "Container Images:"
	@echo "  make build-images    Build and push all container images"
	@echo "  make build-processor Build and push processor image"
	@echo "  make build-sharder   Build and push sharder image"
	@echo "  make build-tiles     Build and push tiles image"
	@echo ""
	@echo "Pipeline:"
	@echo "  make run             Run download → shard → process → merge → tiles via AWS Batch"
	@echo "  make run-us          Run pipeline against the Geofabrik US extract"
	@echo "  make status          Show running AWS Batch jobs"
	@echo ""
	@echo "Development:"
	@echo "  make check           Run type checks and linting"
	@echo "  make clean           Remove build artifacts"

# ============================================================
# Infrastructure
# ============================================================

.PHONY: ensure-aws-account
ensure-aws-account:
	@if [ -z "$(AWS_ACCOUNT_ID)" ]; then \
		echo "Error: Unable to determine AWS account. Configure AWS credentials or set AWS_ACCOUNT_ID."; \
		exit 1; \
	fi

deploy: ensure-aws-account
	cd infra/cdk && npm install && AWS_REGION=$(AWS_REGION) AWS_ACCOUNT_ID=$(AWS_ACCOUNT_ID) AWS_SDK_LOAD_CONFIG=1 npx cdk deploy --all --require-approval never

synth:
	cd infra/cdk && npm install && AWS_REGION=$(AWS_REGION) AWS_ACCOUNT_ID=$(AWS_ACCOUNT_ID) AWS_SDK_LOAD_CONFIG=1 npx cdk synth

destroy: ensure-aws-account
	cd infra/cdk && AWS_REGION=$(AWS_REGION) AWS_ACCOUNT_ID=$(AWS_ACCOUNT_ID) AWS_SDK_LOAD_CONFIG=1 npx cdk destroy --all

# ============================================================
# Container Images
# ============================================================

.PHONY: ecr-login get-uris

ecr-login:
	@aws ecr get-login-password --region $(AWS_REGION) | docker login --username AWS --password-stdin $$(aws sts get-caller-identity --query Account --output text).dkr.ecr.$(AWS_REGION).amazonaws.com

get-uris:
	$(eval PROCESSOR_URI := $(shell aws cloudformation describe-stacks --stack-name OsmH3InfraStack --query "Stacks[0].Outputs[?OutputKey=='ProcessorRepoUri'].OutputValue" --output text 2>/dev/null || echo ""))
	$(eval SHARDER_URI := $(shell aws cloudformation describe-stacks --stack-name OsmH3InfraStack --query "Stacks[0].Outputs[?OutputKey=='SharderRepoUri'].OutputValue" --output text 2>/dev/null || echo ""))
	$(eval TILES_URI := $(shell aws cloudformation describe-stacks --stack-name OsmH3InfraStack --query "Stacks[0].Outputs[?OutputKey=='TilesRepoUri'].OutputValue" --output text 2>/dev/null || echo ""))

build-processor: ecr-login get-uris
	@if [ -z "$(PROCESSOR_URI)" ]; then echo "Error: Deploy infrastructure first"; exit 1; fi
	docker buildx build --platform linux/amd64 -t $(PROCESSOR_URI):latest ./batch --push

build-sharder: ecr-login get-uris
	@if [ -z "$(SHARDER_URI)" ]; then echo "Error: Deploy infrastructure first"; exit 1; fi
	docker buildx build --platform linux/amd64 -t $(SHARDER_URI):latest ./sharding --push

build-tiles: ecr-login get-uris
	@if [ -z "$(TILES_URI)" ]; then echo "Error: Deploy infrastructure first"; exit 1; fi
	docker buildx build --platform linux/amd64 -t $(TILES_URI):latest ./tiles --push

build-images: build-processor build-sharder build-tiles

# ============================================================
# Pipeline Execution
# ============================================================

run:
	@CMD="$(PYTHON) scripts/run_pipeline.py --project-name $(PROJECT_NAME) --region $(AWS_REGION)"; \
	if [ -n "$(RUN_ID)" ]; then CMD="$$CMD --run-id $(RUN_ID)"; fi; \
	if [ -n "$(PLANET_URL)" ]; then CMD="$$CMD --planet-url $(PLANET_URL)"; fi; \
	if [ -n "$(START_AT)" ]; then CMD="$$CMD --start-at $(START_AT)"; fi; \
	echo $$CMD; \
	$$CMD

run-us:
	@RUN_ID=$$(date +us-%Y%m%d-%H%M%S); \
	$(MAKE) run RUN_ID="$$RUN_ID" PLANET_URL="https://download.geofabrik.de/north-america/us-latest.osm.pbf"

status:
	aws batch list-jobs --job-queue "$(PROJECT_NAME)-queue" --job-status RUNNING --region $(AWS_REGION)

# ============================================================
# Development
# ============================================================

check:
	cd infra/cdk && npx tsc --noEmit
	cd sharding && cargo check

clean:
	rm -rf infra/cdk/cdk.out
	rm -rf infra/cdk/node_modules
	cd sharding && cargo clean
