"""OSM-H3 Pipeline Infrastructure - Main Pulumi Program."""

import os
import pulumi
import pulumi_aws as aws
import json

from config import (
    name,
    project_name,
    default_tags,
    account_id,
    region,
    environment,
    planet_url,
    enable_cloudfront,
)

# Import infrastructure modules
from s3 import (
    create_data_bucket,
    create_pulumi_state_bucket,
    create_bucket_policy_for_cloudfront,
)
from ecr import create_ecr_repositories
from iam import (
    create_batch_execution_role,
    create_batch_job_role,
    create_batch_service_role,
    create_spot_fleet_role,
    create_batch_instance_role,
    create_sfn_role,
    create_lambda_role,
)
from vpc import get_default_vpc, get_default_subnets, create_batch_security_group
from batch import (
    create_compute_environment,
    create_job_queue,
    create_all_job_definitions,
    create_log_groups,
)
from cloudfront import (
    create_origin_access_control,
    create_cache_policy,
    create_origin_request_policy,
    create_response_headers_policy,
    create_distribution,
)
from images import create_all_images

# Get project root
PROJECT_ROOT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "stack"
)

# =============================================================================
# S3 Buckets
# =============================================================================

data_bucket = create_data_bucket()
# Note: Not creating pulumi state bucket since we're using local backend
# If you want to use S3 backend later, uncomment this:
# pulumi_state_bucket = create_pulumi_state_bucket()

# =============================================================================
# ECR Repositories
# =============================================================================

ecr_repositories = create_ecr_repositories()

# =============================================================================
# IAM Roles
# =============================================================================

batch_execution_role = create_batch_execution_role()
batch_job_role = create_batch_job_role(data_bucket.arn)
batch_service_role = create_batch_service_role()
spot_fleet_role = create_spot_fleet_role()
batch_instance_profile = create_batch_instance_role()

# =============================================================================
# VPC & Networking
# =============================================================================

default_vpc = get_default_vpc()
default_subnets = get_default_subnets(default_vpc.id)
batch_security_group = create_batch_security_group(default_vpc.id)

# =============================================================================
# CloudWatch Log Groups
# =============================================================================

log_groups = create_log_groups()

# =============================================================================
# Docker Images (Build & Push to ECR)
# =============================================================================

image_uris = create_all_images(
    repositories=ecr_repositories,
    project_root=PROJECT_ROOT,
)

# =============================================================================
# AWS Batch
# =============================================================================

compute_environment = create_compute_environment(
    instance_profile_arn=batch_instance_profile.arn,
    service_role_arn=batch_service_role.arn,
    security_group_ids=[batch_security_group.id],
    subnet_ids=default_subnets.ids,
)

job_queue = create_job_queue(
    compute_environment_arn=compute_environment.arn,
)

job_definitions = create_all_job_definitions(
    image_uris=image_uris,
    execution_role_arn=batch_execution_role.arn,
    job_role_arn=batch_job_role.arn,
    bucket_name=data_bucket.bucket,
)

# =============================================================================
# Step Functions & Lambda
# =============================================================================

get_manifest_lambda_role = create_lambda_role(data_bucket.arn)

get_manifest_lambda = aws.lambda_.Function(
    f"{project_name}-get-manifest",
    runtime="python3.11",
    handler="get_manifest.handler",
    role=get_manifest_lambda_role.arn,
    code=pulumi.AssetArchive(
        {
            ".": pulumi.FileArchive("./lambdas"),
        }
    ),
    environment={
        "variables": {
            "DATA_BUCKET_NAME": data_bucket.bucket,
        }
    },
)

sfn_role = create_sfn_role(
    job_queue_arn=job_queue.arn,
    job_definition_arns={
        "download": job_definitions["download"].arn,
        "sharder": job_definitions["sharder"].arn,
        "processor": job_definitions["processor"].arn,
        "merger": job_definitions["merger"].arn,
        "tiles": job_definitions["tiles"].arn,
    },
    lambda_arn=get_manifest_lambda.arn,
)

with open("statemachine.json") as f:
    statemachine_definition_template = f.read()

statemachine_definition = pulumi.Output.all(
    JobQueueArn=job_queue.arn,
    DownloadJobDefArn=job_definitions["download"].arn,
    SharderJobDefArn=job_definitions["sharder"].arn,
    ProcessorJobDefArn=job_definitions["processor"].arn,
    MergerJobDefArn=job_definitions["merger"].arn,
    TilesJobDefArn=job_definitions["tiles"].arn,
    GetManifestJobDefArn=get_manifest_lambda.arn,
).apply(
    lambda args: json.dumps(
        json.loads(
            statemachine_definition_template.replace(
                "${JobQueueArn}", args["JobQueueArn"]
            )
            .replace("${DownloadJobDefArn}", args["DownloadJobDefArn"])
            .replace("${SharderJobDefArn}", args["SharderJobDefArn"])
            .replace("${ProcessorJobDefArn}", args["ProcessorJobDefArn"])
            .replace("${MergerJobDefArn}", args["MergerJobDefArn"])
            .replace("${TilesJobDefArn}", args["TilesJobDefArn"])
            .replace("${GetManifestJobDefArn}", args["GetManifestJobDefArn"])
        )
    )
)

pipeline_sfn = aws.sfn.StateMachine(
    f"{project_name}-pipeline-sfn",
    role_arn=sfn_role.arn,
    definition=statemachine_definition,
    tags=default_tags,
)

# =============================================================================
# CloudFront Distribution (optional - slow to create, skip in dev)
# =============================================================================

if enable_cloudfront:
    oac = create_origin_access_control()
    cache_policy = create_cache_policy()
    origin_request_policy = create_origin_request_policy()
    response_headers_policy = create_response_headers_policy()

    distribution = create_distribution(
        bucket_domain_name=data_bucket.bucket_regional_domain_name,
        bucket_arn=data_bucket.arn,
        oac_id=oac.id,
        cache_policy_id=cache_policy.id,
        origin_request_policy_id=origin_request_policy.id,
        response_headers_policy_id=response_headers_policy.id,
    )

    # Create bucket policy after distribution (needs distribution ARN)
    bucket_policy = create_bucket_policy_for_cloudfront(
        bucket=data_bucket,
        cloudfront_distribution_arn=distribution.arn,
    )

# =============================================================================
# Exports
# =============================================================================

pulumi.export("project_name", project_name)
pulumi.export("environment", environment)
pulumi.export("region", region)
pulumi.export("account_id", account_id)
pulumi.export("planet_url", planet_url)

# S3
pulumi.export("data_bucket_name", data_bucket.bucket)
pulumi.export("data_bucket_arn", data_bucket.arn)
# pulumi.export("pulumi_state_bucket_name", pulumi_state_bucket.bucket)

# ECR
pulumi.export(
    "ecr_repository_urls", {k: v.repository_url for k, v in ecr_repositories.items()}
)

# Batch
pulumi.export("job_queue_name", job_queue.name)
pulumi.export("job_queue_arn", job_queue.arn)
pulumi.export("compute_environment_arn", compute_environment.arn)
pulumi.export("job_definition_arns", {k: v.arn for k, v in job_definitions.items()})

# CloudFront (if enabled)
if enable_cloudfront:
    pulumi.export("cloudfront_distribution_id", distribution.id)
    pulumi.export("cloudfront_domain", distribution.domain_name)
    pulumi.export(
        "tiles_url",
        distribution.domain_name.apply(lambda domain: f"https://{domain}/pois.pmtiles"),
    )
else:
    pulumi.export(
        "tiles_url",
        data_bucket.bucket.apply(
            lambda b: f"https://{b}.s3.{region}.amazonaws.com/pois.pmtiles"
        ),
    )

# Image URIs
pulumi.export("image_uris", image_uris)
pulumi.export("state_machine_arn", pipeline_sfn.id)
