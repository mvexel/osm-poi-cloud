"""IAM roles and policies for OSM-H3 pipeline."""

import json
import pulumi
import pulumi_aws as aws

from config import name, default_tags, region, account_id


def create_batch_execution_role() -> aws.iam.Role:
    """Create the ECS task execution role for Batch jobs."""
    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )

    role = aws.iam.Role(
        name("batch-execution-role"),
        name="osm-h3-batch-execution-role",
        assume_role_policy=assume_role_policy,
        tags=default_tags,
    )

    # Attach the standard ECS task execution policy
    aws.iam.RolePolicyAttachment(
        name("batch-execution-role-policy"),
        role=role.name,
        policy_arn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",
    )

    return role


def create_batch_job_role(data_bucket_arn: pulumi.Output[str]) -> aws.iam.Role:
    """Create the IAM role for Batch job containers."""
    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )

    role = aws.iam.Role(
        name("batch-job-role"),
        name="osm-h3-batch-job-role",
        assume_role_policy=assume_role_policy,
        tags=default_tags,
    )

    # S3 access policy for the data bucket
    s3_policy = data_bucket_arn.apply(
        lambda arn: json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Action": [
                            "s3:GetObject",
                            "s3:PutObject",
                            "s3:DeleteObject",
                            "s3:ListBucket",
                        ],
                        "Resource": [
                            arn,
                            f"{arn}/*",
                        ],
                    }
                ],
            }
        )
    )

    aws.iam.RolePolicy(
        name("batch-job-role-s3-policy"),
        role=role.name,
        policy=s3_policy,
    )

    # CloudWatch Logs policy
    logs_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    "Resource": f"arn:aws:logs:{region}:{account_id}:log-group:/aws/batch/*",
                }
            ],
        }
    )

    aws.iam.RolePolicy(
        name("batch-job-role-logs-policy"),
        role=role.name,
        policy=logs_policy,
    )

    return role


def create_batch_service_role() -> aws.iam.Role:
    """Create the service role for AWS Batch."""
    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "batch.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )

    role = aws.iam.Role(
        name("batch-service-role"),
        name="osm-h3-batch-service-role",
        assume_role_policy=assume_role_policy,
        tags=default_tags,
    )

    # Attach the standard Batch service policy
    aws.iam.RolePolicyAttachment(
        name("batch-service-role-policy"),
        role=role.name,
        policy_arn="arn:aws:iam::aws:policy/service-role/AWSBatchServiceRole",
    )

    return role


def create_spot_fleet_role() -> aws.iam.Role:
    """Create the IAM role for EC2 Spot Fleet."""
    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "spotfleet.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )

    role = aws.iam.Role(
        name("spot-fleet-role"),
        name="osm-h3-spot-fleet-role",
        assume_role_policy=assume_role_policy,
        tags=default_tags,
    )

    aws.iam.RolePolicyAttachment(
        name("spot-fleet-role-policy"),
        role=role.name,
        policy_arn="arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetTaggingRole",
    )

    return role


def create_batch_instance_role() -> aws.iam.InstanceProfile:
    """Create the instance profile for Batch EC2 instances."""
    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )

    role = aws.iam.Role(
        name("batch-instance-role"),
        name="osm-h3-batch-instance-role",
        assume_role_policy=assume_role_policy,
        tags=default_tags,
    )

    # Attach the standard ECS instance policy
    aws.iam.RolePolicyAttachment(
        name("batch-instance-role-ecs-policy"),
        role=role.name,
        policy_arn="arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role",
    )

    # Create instance profile
    instance_profile = aws.iam.InstanceProfile(
        name("batch-instance-profile"),
        name="osm-h3-batch-instance-profile",
        role=role.name,
        tags=default_tags,
    )

    return instance_profile
