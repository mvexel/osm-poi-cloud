"""ECR repository definitions for OSM-H3 pipeline images."""

import pulumi_aws as aws

from config import name, default_tags


# Repository names for each pipeline stage
REPO_NAMES = [
    "downloader",
    "sharder",
    "processor",
    "merger",
    "tiles",
]


def create_ecr_repositories() -> dict[str, aws.ecr.Repository]:
    """Create ECR repositories for all pipeline stages."""
    repositories = {}

    for repo_name in REPO_NAMES:
        repo = aws.ecr.Repository(
            name(f"ecr-{repo_name}"),
            name=f"osm-h3-{repo_name}",
            image_tag_mutability="MUTABLE",
            image_scanning_configuration=aws.ecr.RepositoryImageScanningConfigurationArgs(
                scan_on_push=True,
            ),
            tags=default_tags,
        )

        # Lifecycle policy to limit stored images (keep last 5)
        aws.ecr.LifecyclePolicy(
            name(f"ecr-{repo_name}-lifecycle"),
            repository=repo.name,
            policy="""{
                "rules": [
                    {
                        "rulePriority": 1,
                        "description": "Keep last 5 images",
                        "selection": {
                            "tagStatus": "any",
                            "countType": "imageCountMoreThan",
                            "countNumber": 5
                        },
                        "action": {
                            "type": "expire"
                        }
                    }
                ]
            }""",
        )

        repositories[repo_name] = repo

    return repositories
