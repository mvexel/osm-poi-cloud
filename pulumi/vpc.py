"""VPC and networking for OSM-H3 pipeline using default VPC."""

import pulumi_aws as aws

from config import name, default_tags


def get_default_vpc() -> aws.ec2.GetVpcResult:
    """Look up the default VPC."""
    return aws.ec2.get_vpc(default=True)


def get_default_subnets(vpc_id: str) -> aws.ec2.GetSubnetsResult:
    """Look up subnets in the default VPC."""
    return aws.ec2.get_subnets(
        filters=[
            aws.ec2.GetSubnetsFilterArgs(
                name="vpc-id",
                values=[vpc_id],
            ),
        ],
    )


def create_batch_security_group(vpc_id: str) -> aws.ec2.SecurityGroup:
    """Create security group for Batch compute instances."""
    sg = aws.ec2.SecurityGroup(
        name("batch-sg"),
        name="osm-h3-batch-sg",
        description="Security group for OSM-H3 Batch compute instances",
        vpc_id=vpc_id,
        # Allow all outbound traffic (needed for ECR, S3, internet)
        egress=[
            aws.ec2.SecurityGroupEgressArgs(
                from_port=0,
                to_port=0,
                protocol="-1",
                cidr_blocks=["0.0.0.0/0"],
                description="Allow all outbound traffic",
            ),
        ],
        # No inbound rules needed - instances only make outbound connections
        tags=default_tags,
    )

    return sg
