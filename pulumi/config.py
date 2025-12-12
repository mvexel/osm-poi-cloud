"""Shared configuration for OSM-H3 Pulumi infrastructure."""

import pulumi
import pulumi_aws as aws

# Get Pulumi config
config = pulumi.Config()
aws_config = pulumi.Config("aws")

# Environment and naming
environment = config.get("environment") or "dev"
project_name = "osm-h3"

# AWS context
region = aws_config.get("region") or "us-west-2"
account_id = aws.get_caller_identity().account_id

# Resource naming helper
def name(resource: str) -> str:
    """Generate consistent resource names."""
    return f"{project_name}-{resource}"

# Batch configuration
max_vcpus = config.get_int("max_vcpus") or 256
spot_bid_percentage = config.get_int("spot_bid_percentage") or 100

# Data source configuration
# Default to Utah for testing, use planet URL for production
planet_url = config.get("planet_url") or "https://download.geofabrik.de/north-america/us/utah-latest.osm.pbf"

# Feature flags
enable_cloudfront = config.get_bool("enable_cloudfront") or False  # Skip CloudFront in dev for faster iterations

# Instance types for Batch compute environment (cost-effective general purpose)
instance_types = [
    "c5.large", "c5.xlarge", "c5.2xlarge", "c5.4xlarge",
    "m5.large", "m5.xlarge", "m5.2xlarge", "m5.4xlarge",
    "r5.large", "r5.xlarge", "r5.2xlarge",
]

# Job resource configurations
job_configs = {
    "download": {"vcpus": 4, "memory": 8192},
    "sharder": {"vcpus": 8, "memory": 32768},
    "processor": {"vcpus": 2, "memory": 4096},
    "merger": {"vcpus": 4, "memory": 16384},
    "tiles": {"vcpus": 4, "memory": 16384},
}

# Tags applied to all resources
default_tags = {
    "Project": project_name,
    "Environment": environment,
    "ManagedBy": "pulumi",
}
