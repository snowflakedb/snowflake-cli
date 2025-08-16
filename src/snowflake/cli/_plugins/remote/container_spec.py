# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Generate container service specifications for remote development environments."""

from math import ceil
from pathlib import PurePath
from typing import Any, Dict, Optional, Union

import yaml
from snowflake import snowpark
from snowflake.cli._plugins.remote.constants import (
    DEFAULT_CONTAINER_NAME,
    DEFAULT_IMAGE_CPU,
    DEFAULT_IMAGE_GPU,
    DEFAULT_IMAGE_REPO,
    DEFAULT_IMAGE_TAG,
    DEFAULT_SERVER_PORT,
    DEFAULT_WEBSOCKET_PORT,
    ENABLE_HEALTH_CHECKS,
    ENABLE_REMOTE_DEV_ENV_VAR,
    MEMORY_VOLUME_NAME,
    MEMORY_VOLUME_SIZE,
    ML_RUNTIME_HEALTH_CHECK_PORT,
    RAY_DASHBOARD_ENDPOINT_NAME,
    RAY_ENDPOINTS,
    RAY_ENV_VARS,
    SERVER_UI_ENDPOINT_NAME,
    USER_VSCODE_DATA_VOLUME_MOUNT_PATH,
    USER_VSCODE_DATA_VOLUME_NAME,
    USER_WORKSPACE_VOLUME_MOUNT_PATH,
    USER_WORKSPACE_VOLUME_NAME,
    WEBSOCKET_SSH_ENDPOINT_NAME,
)
from snowflake.cli._plugins.remote.utils import (
    ImageSpec,
    format_stage_path,
    get_node_resources,
    validate_stage_path,
)


def _get_image_spec(
    session: snowpark.Session, compute_pool: str, image_tag: Optional[str] = None
) -> ImageSpec:
    """Get appropriate image specification based on compute pool."""
    # Retrieve compute pool node resources
    resources = get_node_resources(session, compute_pool=compute_pool)

    # Use MLRuntime image - select CPU or GPU based on resources
    image_repo = DEFAULT_IMAGE_REPO
    image_name = DEFAULT_IMAGE_GPU if resources.gpu > 0 else DEFAULT_IMAGE_CPU

    # Use provided image_tag or fall back to default
    if not image_tag:
        image_tag = DEFAULT_IMAGE_TAG

    return ImageSpec(
        repo=image_repo,
        image_name=image_name,
        image_tag=image_tag,
        resource_requests=resources,
        resource_limits=resources,
    )


def generate_service_spec(
    session: snowpark.Session,
    compute_pool: str,
    stage: Optional[str] = None,
    image_tag: Optional[str] = None,
    ssh_public_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate a service specification for a remote development environment.

    Args:
        session: Snowflake session
        compute_pool: Compute pool for service execution
        stage: Optional internal Snowflake stage to mount (e.g., @my_stage)
        image_tag: Optional custom image tag to use
        ssh_public_key: Optional SSH public key to inject for secure authentication

    Returns:
        Service specification dictionary
    """
    # Validate and normalize stage path if provided
    if stage is not None:
        # First normalize the stage path (adds @ if missing, removes trailing slashes)
        stage = format_stage_path(stage)
        # Then validate the normalized path (must be more than just "@" or "@" with whitespace)
        if not validate_stage_path(stage) or (stage and stage.strip() == "@"):
            raise ValueError(
                f"Invalid stage path: '{stage}'. Stage path must start with '@' and contain a stage name"
            )

    image_spec = _get_image_spec(session, compute_pool, image_tag)

    # Set resource requests/limits
    # This is a temporary fix to SPCS preprod8 bug to ensure the container has enough memory.
    resource_requests: Dict[str, Union[str, int]] = {
        "cpu": f"{int(image_spec.resource_requests.cpu * 1000)}m",
        "memory": f"{image_spec.resource_limits.memory}Gi",
    }
    resource_limits: Dict[str, Union[str, int]] = {
        "cpu": f"{int(image_spec.resource_requests.cpu * 1000)}m",
        "memory": f"{image_spec.resource_limits.memory}Gi",
    }

    # Add GPU resources if applicable
    if image_spec.resource_limits.gpu > 0:
        resource_requests["nvidia.com/gpu"] = image_spec.resource_requests.gpu
        resource_limits["nvidia.com/gpu"] = image_spec.resource_limits.gpu

    # Define volumes and volume mounts
    volumes = []
    volume_mounts = []

    # Add local volumes for ephemeral logs and artifacts
    for volume_name, mount_path in [
        ("system-logs", "/var/log/managedservices/system/remote"),
        ("user-logs", "/var/log/managedservices/user/remote"),
    ]:
        volume_mounts.append(
            {
                "name": volume_name,
                "mountPath": mount_path,
            }
        )
        volumes.append(
            {
                "name": volume_name,
                "source": "local",
            }
        )

    # Mount 30% of memory limit as a memory-backed volume
    memory_volume_size = min(
        ceil(image_spec.resource_limits.memory * MEMORY_VOLUME_SIZE),
        image_spec.resource_requests.memory,
    )
    volume_mounts.append(
        {
            "name": MEMORY_VOLUME_NAME,
            "mountPath": "/dev/shm",
        }
    )
    volumes.append(
        {
            "name": MEMORY_VOLUME_NAME,
            "source": "memory",
            "size": f"{memory_volume_size}Gi",
        }
    )

    # Mount user stage as volume if provided
    if stage:
        # Mount user workspace volume
        user_workspace_mount = PurePath(USER_WORKSPACE_VOLUME_MOUNT_PATH)
        volume_mounts.append(
            {
                "name": USER_WORKSPACE_VOLUME_NAME,
                "mountPath": user_workspace_mount.as_posix(),
            }
        )

        # Use stage with /user-default suffix for workspace
        workspace_source = f"{stage}/user-default"

        volumes.append(
            {
                "name": USER_WORKSPACE_VOLUME_NAME,
                "source": workspace_source,
            }
        )

        # Mount user vscode data volume
        user_vscode_data_mount = PurePath(USER_VSCODE_DATA_VOLUME_MOUNT_PATH)
        volume_mounts.append(
            {
                "name": USER_VSCODE_DATA_VOLUME_NAME,
                "mountPath": user_vscode_data_mount.as_posix(),
            }
        )

        # VS Code data always uses stage location
        vscode_data_source = f"{stage}/.vscode-server/data"

        volumes.append(
            {
                "name": USER_VSCODE_DATA_VOLUME_NAME,
                "source": vscode_data_source,
            }
        )

    # Setup environment variables
    env_vars = {
        ENABLE_REMOTE_DEV_ENV_VAR: "true",
        "ENABLE_HEALTH_CHECKS": ENABLE_HEALTH_CHECKS,
        "ML_RUNTIME_HEALTH_CHECK_PORT": ML_RUNTIME_HEALTH_CHECK_PORT,
    }

    # Update environment variables for multi-node job with Ray ports
    env_vars.update(RAY_ENV_VARS)

    # Inject SSH public key for secure authentication
    if ssh_public_key:
        env_vars["SSH_PUBLIC_KEY"] = ssh_public_key

    # Setup Ray configuration
    endpoints = []

    # Define Ray endpoints for intra-service instance communication
    endpoints.extend(RAY_ENDPOINTS)

    # Add VS Code endpoint
    endpoints.append(
        {
            "name": SERVER_UI_ENDPOINT_NAME,
            "port": DEFAULT_SERVER_PORT,
            "public": True,
        }
    )

    # Add websocket endpoint
    endpoints.append(
        {
            "name": WEBSOCKET_SSH_ENDPOINT_NAME,
            "port": DEFAULT_WEBSOCKET_PORT,
            "public": True,
        }
    )

    # Add ray dashboard endpoint
    endpoints.append(
        {
            "name": RAY_DASHBOARD_ENDPOINT_NAME,
            "port": 12003,
            "public": True,
        }
    )

    # Create the full service specification
    spec_dict = {
        "containers": [
            {
                "name": DEFAULT_CONTAINER_NAME,
                "image": image_spec.full_name,
                "env": env_vars,
                "volumeMounts": volume_mounts,
                "resources": {
                    "requests": resource_requests,
                    "limits": resource_limits,
                },
            },
        ],
        "volumes": volumes,
        "endpoints": endpoints,
    }

    # Assemble into service specification dict
    spec = {"spec": spec_dict}

    return spec


def generate_service_spec_yaml(
    session: snowpark.Session,
    compute_pool: str,
    stage: Optional[str] = None,
    image_tag: Optional[str] = None,
    ssh_public_key: Optional[str] = None,
) -> str:
    """
    Generate a service specification as YAML for a remote development environment.

    This is a convenience wrapper around generate_service_spec that returns YAML.

    Args:
        session: Snowflake session
        compute_pool: Compute pool for service execution
        stage: Optional internal Snowflake stage to mount (e.g., @my_stage)
        image_tag: Optional custom image tag to use
        ssh_public_key: Optional SSH public key to inject for secure authentication

    Returns:
        YAML string containing the service specification
    """
    spec = generate_service_spec(
        session=session,
        compute_pool=compute_pool,
        stage=stage,
        image_tag=image_tag,
        ssh_public_key=ssh_public_key,
    )
    return yaml.dump(spec, default_flow_style=False)
