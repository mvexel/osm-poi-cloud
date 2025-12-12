"""AWS Batch compute environment, queue, and job definitions."""

import json
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
)


def create_compute_environment(
    service_role_arn: pulumi.Output[str],
    instance_profile_arn: pulumi.Output[str],
    security_group_ids: list[str],
    subnet_ids: list[str],
) -> aws.batch.ComputeEnvironment:
    """Create EC2 Spot compute environment for Batch."""
    compute_env = aws.batch.ComputeEnvironment(
        name("compute-env"),
        compute_environment_name="osm-h3-compute-env",
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

    return compute_env


def create_job_queue(
    compute_environment_arn: pulumi.Output[str],
) -> aws.batch.JobQueue:
    """Create the job queue for the pipeline."""
    queue = aws.batch.JobQueue(
        name("job-queue"),
        name="osm-h3-queue",
        state="ENABLED",
        priority=1,
        compute_environment_orders=[
            aws.batch.JobQueueComputeEnvironmentOrderArgs(
                order=1,
                compute_environment=compute_environment_arn,
            ),
        ],
        tags=default_tags,
    )

    return queue


def create_job_definition(
    job_name: str,
    image_uri: pulumi.Output[str],
    execution_role_arn: pulumi.Output[str],
    job_role_arn: pulumi.Output[str],
    vcpus: int,
    memory: int,
    environment_vars: dict[str, str] | None = None,
) -> aws.batch.JobDefinition:
    """Create a Batch job definition."""
    env_list = []
    if environment_vars:
        env_list = [{"name": k, "value": v} for k, v in environment_vars.items()]

    container_properties = image_uri.apply(lambda uri: json.dumps({
        "image": uri,
        "resourceRequirements": [
            {"type": "VCPU", "value": str(vcpus)},
            {"type": "MEMORY", "value": str(memory)},
        ],
        "environment": env_list,
        "logConfiguration": {
            "logDriver": "awslogs",
            "options": {
                "awslogs-group": f"/aws/batch/osm-h3-{job_name}",
                "awslogs-region": region,
                "awslogs-stream-prefix": job_name,
            },
        },
    }))

    job_def = aws.batch.JobDefinition(
        name(f"job-{job_name}"),
        name=f"osm-h3-{job_name}",
        type="container",
        platform_capabilities=["EC2"],
        container_properties=container_properties,
        tags=default_tags,
    )

    return job_def


def create_log_groups() -> dict[str, aws.cloudwatch.LogGroup]:
    """Create CloudWatch log groups for all job types."""
    log_groups = {}

    for job_name in job_configs.keys():
        log_group = aws.cloudwatch.LogGroup(
            name(f"log-group-{job_name}"),
            name=f"/aws/batch/osm-h3-{job_name}",
            retention_in_days=14,
            tags=default_tags,
        )
        log_groups[job_name] = log_group

    return log_groups


def create_all_job_definitions(
    image_uris: dict[str, pulumi.Output[str]],
    execution_role_arn: pulumi.Output[str],
    job_role_arn: pulumi.Output[str],
    bucket_name: pulumi.Output[str],
) -> dict[str, aws.batch.JobDefinition]:
    """Create job definitions for all pipeline stages."""
    job_definitions = {}

    # Common environment variables
    common_env = {
        "AWS_REGION": region,
    }

    for job_name, config in job_configs.items():
        # Map job name to image key (handle naming differences)
        image_key = job_name
        if job_name == "sharder":
            image_key = "sharder"
        elif job_name == "merger":
            image_key = "merger"

        # Skip if image not provided
        if image_key not in image_uris:
            continue

        job_def = create_job_definition(
            job_name=job_name,
            image_uri=image_uris[image_key],
            execution_role_arn=execution_role_arn,
            job_role_arn=job_role_arn,
            vcpus=config["vcpus"],
            memory=config["memory"],
            environment_vars=common_env,
        )
        job_definitions[job_name] = job_def

    return job_definitions
