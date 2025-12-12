"""S3 bucket definitions for OSM-H3 pipeline."""

import pulumi
import pulumi_aws as aws
import json

from config import name, account_id, default_tags


def create_data_bucket() -> aws.s3.BucketV2:
    """Create the main data bucket for pipeline artifacts."""
    bucket = aws.s3.BucketV2(
        name("data-bucket"),
        bucket=f"osm-h3-data-{account_id}",
        tags=default_tags,
    )

    # Enable versioning for data protection
    aws.s3.BucketVersioningV2(
        name("data-bucket-versioning"),
        bucket=bucket.id,
        versioning_configuration=aws.s3.BucketVersioningV2VersioningConfigurationArgs(
            status="Enabled",
        ),
    )

    # Block public access
    aws.s3.BucketPublicAccessBlock(
        name("data-bucket-public-access-block"),
        bucket=bucket.id,
        block_public_acls=True,
        block_public_policy=True,
        ignore_public_acls=True,
        restrict_public_buckets=True,
    )

    # Lifecycle rule to clean up old run data (optional - keep 30 days)
    aws.s3.BucketLifecycleConfigurationV2(
        name("data-bucket-lifecycle"),
        bucket=bucket.id,
        rules=[
            aws.s3.BucketLifecycleConfigurationV2RuleArgs(
                id="cleanup-old-runs",
                status="Enabled",
                filter=aws.s3.BucketLifecycleConfigurationV2RuleFilterArgs(
                    prefix="runs/",
                ),
                expiration=aws.s3.BucketLifecycleConfigurationV2RuleExpirationArgs(
                    days=30,
                ),
            ),
        ],
    )

    return bucket


def create_pulumi_state_bucket() -> aws.s3.BucketV2:
    """Create bucket for Pulumi state storage."""
    bucket = aws.s3.BucketV2(
        name("pulumi-state-bucket"),
        bucket=f"osm-h3-pulumi-state-{account_id}",
        tags=default_tags,
    )

    # Enable versioning for state protection
    aws.s3.BucketVersioningV2(
        name("pulumi-state-bucket-versioning"),
        bucket=bucket.id,
        versioning_configuration=aws.s3.BucketVersioningV2VersioningConfigurationArgs(
            status="Enabled",
        ),
    )

    # Block public access
    aws.s3.BucketPublicAccessBlock(
        name("pulumi-state-bucket-public-access-block"),
        bucket=bucket.id,
        block_public_acls=True,
        block_public_policy=True,
        ignore_public_acls=True,
        restrict_public_buckets=True,
    )

    return bucket


def create_bucket_policy_for_cloudfront(
    bucket: aws.s3.BucketV2,
    cloudfront_oac_arn: pulumi.Output[str],
    cloudfront_distribution_arn: pulumi.Output[str],
) -> aws.s3.BucketPolicy:
    """Create bucket policy allowing CloudFront access to tiles."""
    policy_document = pulumi.Output.all(
        bucket.arn, cloudfront_distribution_arn
    ).apply(lambda args: json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowCloudFrontServicePrincipal",
                "Effect": "Allow",
                "Principal": {
                    "Service": "cloudfront.amazonaws.com"
                },
                "Action": "s3:GetObject",
                "Resource": f"{args[0]}/tiles/*",
                "Condition": {
                    "StringEquals": {
                        "AWS:SourceArn": args[1]
                    }
                }
            }
        ]
    }))

    return aws.s3.BucketPolicy(
        name("data-bucket-policy"),
        bucket=bucket.id,
        policy=policy_document,
    )
