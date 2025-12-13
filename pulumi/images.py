"""Docker image builds and ECR pushes for OSM-H3 pipeline."""

import pulumi
import pulumi_aws as aws
import pulumi_docker as docker

from config import name


def get_ecr_auth_token() -> docker.RegistryArgs:
    """Get ECR authorization token for Docker provider."""
    auth = aws.ecr.get_authorization_token()
    return docker.RegistryArgs(
        server=auth.proxy_endpoint,
        username="AWS",
        password=auth.password,
    )


def build_and_push_image(
    image_name: str,
    context_path: str,
    dockerfile_path: str,
    repository_url: pulumi.Output[str],
    registry: docker.RegistryArgs,
    build_args: dict[str, str] | None = None,
) -> docker.Image:
    """Build a Docker image and push to ECR."""
    image = docker.Image(
        name(f"image-{image_name}"),
        build=docker.DockerBuildArgs(
            context=context_path,
            dockerfile=dockerfile_path,
            platform="linux/amd64",
            args=build_args or {},
        ),
        image_name=repository_url.apply(lambda url: f"{url}:latest"),
        registry=registry,
    )

    return image


def create_all_images(
    repositories: dict[str, aws.ecr.Repository],
    project_root: str,
) -> dict[str, pulumi.Output[str]]:
    """Build and push all pipeline images to ECR."""
    registry = get_ecr_auth_token()
    image_uris = {}

    # Image configurations: name -> (context_path, dockerfile_path)
    # Paths are relative to project root
    # Note: "batch" image is shared by download, processor, and merger jobs
    image_configs = {
        "batch": {
            "context": f"{project_root}/batch",
            "dockerfile": f"{project_root}/batch/Dockerfile",
        },
        "sharder": {
            "context": f"{project_root}/sharding",
            "dockerfile": f"{project_root}/sharding/Dockerfile",
        },
        "tiles": {
            "context": f"{project_root}/tiles",
            "dockerfile": f"{project_root}/tiles/Dockerfile",
        },
    }

    for image_name, config in image_configs.items():
        if image_name not in repositories:
            continue

        repo = repositories[image_name]

        image = build_and_push_image(
            image_name=image_name,
            context_path=config["context"],
            dockerfile_path=config["dockerfile"],
            repository_url=repo.repository_url,
            registry=registry,
        )

        # Return the fully qualified image URI with tag
        image_uris[image_name] = image.image_name

    return image_uris
