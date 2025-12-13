"""AWS Batch compute environment, queue, and job definitions."""

import json
from typing import Sequence
import pulumi
import pulumi_aws as aws

from config import (
    name,
    default_tags,
    max_vcpus,
    spot_bid_percentage,
    instance_types,
    job_configs,
    region,
    planet_url,
    max_zoom,
    max_nodes_per_shard,
)


def create_compute_environment(
    instance_profile_arn: pulumi.Output[str],
    service_role_arn: pulumi.Output[str],
    security_group_ids: pulumi.Input[Sequence[pulumi.Input[str]]],
    subnet_ids: pulumi.Input[Sequence[pulumi.Input[str]]],
) -> aws.batch.ComputeEnvironment:
    """Create EC2 Spot compute environment for Batch."""
    args = aws.batch.ComputeEnvironmentArgs(
        name_prefix="osm-h3-",
        type="MANAGED",
        state="ENABLED",
        service_role=service_role_arn,
        compute_resources=aws.batch.ComputeEnvironmentComputeResourcesArgs(
            type="SPOT",
            allocation_strategy="SPOT_CAPACITY_OPTIMIZED",
            bid_percentage=spot_bid_percentage,
            max_vcpus=max_vcpus,
            min_vcpus=0,
            desired_vcpus=0,
            instance_types=instance_types,
            instance_role=instance_profile_arn,
            security_group_ids=security_group_ids,
            subnets=subnet_ids,
            tags=default_tags,
        ),
        tags=default_tags,
    )

    return aws.batch.ComputeEnvironment(name("compute-env"), args)


def create_job_queue(
    compute_environment_arn: pulumi.Output[str],
) -> aws.batch.JobQueue:
    """Create the job queue for the pipeline."""
    args = aws.batch.JobQueueArgs(
        name="osm-h3-queue",
        state="ENABLED",
        priority=1,
        compute_environment_orders=[
            aws.batch.JobQueueComputeEnvironmentOrderArgs(
                compute_environment=compute_environment_arn,
                order=1,
            )
        ],
        tags=default_tags,
    )

    return aws.batch.JobQueue(name("job-queue"), args)


def create_job_definition(
    job_name: str,
    image_uri: pulumi.Output[str],
    execution_role_arn: pulumi.Output[str],
    job_role_arn: pulumi.Output[str],
    bucket_name: pulumi.Output[str],
    vcpus: int,
    memory: int,
    environment_vars: dict[str, str] | None = None,
) -> aws.batch.JobDefinition:
    """Create a Batch job definition."""
    env_list: list[dict[str, str]] = []
    if environment_vars:
        env_list = [{"name": k, "value": v} for k, v in environment_vars.items()]

    container_properties = pulumi.Output.all(
        image_uri, execution_role_arn, job_role_arn, bucket_name
    ).apply(
        lambda args: json.dumps(
            {
                "image": args[0],
                "executionRoleArn": args[1],
                "jobRoleArn": args[2],
                "resourceRequirements": [
                    {"type": "VCPU", "value": str(vcpus)},
                    {"type": "MEMORY", "value": str(memory)},
                ],
                "environment": env_list + [
                    {"name": "S3_BUCKET", "value": args[3]}
                ],
                "logConfiguration": {
                    "logDriver": "awslogs",
                    "options": {
                        "awslogs-group": f"/aws/batch/osm-h3-{job_name}",
                        "awslogs-region": region,
                        "awslogs-stream-prefix": job_name,
                    },
                },
            }
        )
    )

    args = aws.batch.JobDefinitionArgs(
        name=f"osm-h3-{job_name}",
        type="container",
        platform_capabilities=["EC2"],
        container_properties=container_properties,
        tags=default_tags,
    )

    return aws.batch.JobDefinition(name(f"job-{job_name}"), args)


def create_log_groups() -> dict[str, aws.cloudwatch.LogGroup]:
    """Create CloudWatch log groups for all job types."""
    log_groups: dict[str, aws.cloudwatch.LogGroup] = {}

    for job_name in job_configs.keys():
        args = aws.cloudwatch.LogGroupArgs(
            name=f"/aws/batch/osm-h3-{job_name}",
            retention_in_days=14,
            tags=default_tags,
        )
        log_groups[job_name] = aws.cloudwatch.LogGroup(
            name(f"log-group-{job_name}"), args
        )

    return log_groups


def create_all_job_definitions(
    image_uris: dict[str, pulumi.Output[str]],
    execution_role_arn: pulumi.Output[str],
    job_role_arn: pulumi.Output[str],
    bucket_name: pulumi.Output[str],
) -> dict[str, aws.batch.JobDefinition]:
    """Create job definitions for all pipeline stages."""
    job_definitions: dict[str, aws.batch.JobDefinition] = {}

    # Map job config names to image names
    # download, processor, merger all use the shared "batch" image
    job_to_image = {
        "download": "batch",
        "sharder": "sharder",
        "processor": "batch",
        "merger": "batch",
        "tiles": "tiles",
    }

    # Map job names to STAGE env var values (for the batch image)
    job_to_stage = {
        "download": "download",
        "processor": "process",
        "merger": "merge",
    }

    for job_name, config in job_configs.items():
        image_key = job_to_image.get(job_name, job_name)

        # Skip if image not provided
        if image_key not in image_uris:
            continue

        # Build environment variables for this job
        env_vars = {
            "AWS_REGION": region,
            "PLANET_URL": planet_url,
        }

        # Sharder configuration (optional; if omitted, sharder binary defaults apply)
        if job_name == "sharder":
            if max_zoom is not None:
                env_vars["MAX_ZOOM"] = str(max_zoom)
            if max_nodes_per_shard is not None:
                env_vars["MAX_NODES_PER_SHARD"] = str(max_nodes_per_shard)

        # Add STAGE for batch image jobs
        if job_name in job_to_stage:
            env_vars["STAGE"] = job_to_stage[job_name]

        job_def = create_job_definition(
            job_name=job_name,
            image_uri=image_uris[image_key],
            execution_role_arn=execution_role_arn,
            job_role_arn=job_role_arn,
            bucket_name=bucket_name,
            vcpus=config["vcpus"],
            memory=config["memory"],
            environment_vars=env_vars,
        )
        job_definitions[job_name] = job_def

    return job_definitions
