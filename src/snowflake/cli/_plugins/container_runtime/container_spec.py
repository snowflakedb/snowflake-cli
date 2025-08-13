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

import logging
from math import ceil
from pathlib import PurePath
from typing import Any, Dict, List, Optional, Union

from snowflake import snowpark
from snowflake.cli._plugins.container_runtime import constants, utils
from snowflake.cli.api.console import cli_console as cc

# Constants for Container Service
DEFAULT_SERVER_PORT = 12020
DEFAULT_WEBSOCKET_PORT = 12021
DEFAULT_MEMORY_VOLUME_SIZE = 0.3  # as a fraction of total memory
DEFAULT_CPU_REQUEST = 1
DEFAULT_MEMORY_REQUEST = 4
DEFAULT_CPU_LIMIT = 2
DEFAULT_MEMORY_LIMIT = 8
DEFAULT_USER_STAGE_VOLUME_NAME = constants.USER_STAGE_VOLUME_NAME
DEFAULT_USER_STAGE_VOLUME_MOUNT_PATH = constants.USER_STAGE_VOLUME_MOUNT_PATH
DEFAULT_USER_WORKSPACE_VOLUME_NAME = constants.USER_WORKSPACE_VOLUME_NAME
DEFAULT_USER_WORKSPACE_VOLUME_MOUNT_PATH = constants.USER_WORKSPACE_VOLUME_MOUNT_PATH
DEFAULT_USER_VSCODE_DATA_VOLUME_NAME = constants.USER_VSCODE_DATA_VOLUME_NAME
DEFAULT_USER_VSCODE_DATA_VOLUME_MOUNT_PATH = (
    constants.USER_VSCODE_DATA_VOLUME_MOUNT_PATH
)


def _get_node_resources(
    session: snowpark.Session, compute_pool: str
) -> utils.ComputeResources:
    """Extract resource information for the specified compute pool."""
    # Get the instance family
    rows = session.sql(f"show compute pools like '{compute_pool}'").collect()
    if not rows:
        raise ValueError(f"Compute pool '{compute_pool}' not found")

    instance_family: str = rows[0]["instance_family"]

    cc.step(f"get instance family {instance_family} resources")

    # Get the cloud we're using (AWS, Azure, etc)
    region = utils.get_regions(session)[utils.get_current_region_id(session)]
    cloud = region["cloud"]

    cc.step(f"get cloud {cloud} instance family {instance_family} resources")

    return (
        constants.COMMON_INSTANCE_FAMILIES.get(instance_family)
        or constants.CLOUD_INSTANCE_FAMILIES[cloud][instance_family]
    )


def _get_image_spec(
    session: snowpark.Session, compute_pool: str, image_tag: Optional[str] = None
) -> utils.ImageSpec:
    """Get appropriate image specification based on compute pool."""
    # Retrieve compute pool node resources
    resources = _get_node_resources(session, compute_pool=compute_pool)

    # Use MLRuntime image
    image_repo = constants.DEFAULT_IMAGE_REPO
    image_name = (
        constants.DEFAULT_IMAGE_GPU
        if resources.gpu > 0
        else constants.DEFAULT_IMAGE_CPU
    )

    # Use provided image_tag or fall back to default
    if not image_tag:
        image_tag = constants.DEFAULT_IMAGE_TAG

    return utils.ImageSpec(
        repo=image_repo,
        image_name=image_name,
        image_tag=image_tag,
        resource_requests=resources,
        resource_limits=resources,
    )


def generate_spec_overrides(
    environment_vars: Optional[Dict[str, str]] = None,
    custom_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Generate a dictionary of service specification overrides.

    Args:
        environment_vars: Environment variables to set in primary container
        custom_overrides: Custom service specification overrides

    Returns:
        Resulting service specifiation patch dict. Empty if no overrides were supplied.
    """
    # Generate container level overrides
    container_spec: Dict[str, Any] = {
        "name": constants.DEFAULT_CONTAINER_NAME,
    }
    if environment_vars:
        container_spec["env"] = environment_vars

    # Build container override spec only if any overrides were supplied
    spec = {}
    if len(container_spec) > 1:
        spec = {
            "spec": {
                "containers": [container_spec],
            }
        }

    # Apply custom overrides
    if custom_overrides:
        spec = merge_patch(spec, custom_overrides, display_name="custom_overrides")

    return spec


def generate_service_spec(
    session: snowpark.Session,
    compute_pool: str,
    environment_vars: Optional[Dict[str, str]] = None,
    enable_metrics: bool = False,
    stage: Optional[str] = None,
    workspace_stage_path: Optional[str] = None,
    image_tag: Optional[str] = None,
    ssh_public_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate a service specification for a container service.

    Args:
        session: Snowflake session
        compute_pool: Compute pool for service execution
        environment_vars: Environment variables to set in the container
        enable_metrics: Enable platform metrics for the job
        stage: Optional internal Snowflake stage to mount (e.g., @my_stage)
        workspace_stage_path: Optional workspace stage path (for workspace parameter)
        image_tag: Optional custom image tag to use
        ssh_public_key: Optional SSH public key to inject for secure authentication

    Returns:
        Service specification
    """
    image_spec = _get_image_spec(session, compute_pool, image_tag)

    # Set resource requests/limits
    # TODO: This is a temporary fix to SPCS preprod8 bug to ensure the container has enough memory.
    resource_requests: Dict[str, Union[str, int]] = {
        "cpu": f"{int((image_spec.resource_requests.cpu - 1) * 1000)}m",
        "memory": f"{image_spec.resource_limits.memory * 0.9}Gi",
    }
    resource_limits: Dict[str, Union[str, int]] = {
        "cpu": f"{int((image_spec.resource_requests.cpu - 1) * 1000)}m",
        "memory": f"{image_spec.resource_limits.memory * 0.9}Gi",
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
        ("system-logs", "/var/log/managedservices/system/mlrs"),
        ("user-logs", "/var/log/managedservices/user/mlrs"),
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
        ceil(image_spec.resource_limits.memory * constants.MEMORY_VOLUME_SIZE),
        image_spec.resource_requests.memory,
    )
    volume_mounts.append(
        {
            "name": constants.MEMORY_VOLUME_NAME,
            "mountPath": "/dev/shm",
        }
    )
    volumes.append(
        {
            "name": constants.MEMORY_VOLUME_NAME,
            "source": "memory",
            "size": f"{memory_volume_size}Gi",
        }
    )

    # Mount user stage as volume if provided
    if stage or workspace_stage_path:
        # Mount user workspace volume
        user_workspace_mount = PurePath(constants.USER_WORKSPACE_VOLUME_MOUNT_PATH)
        volume_mounts.append(
            {
                "name": constants.USER_WORKSPACE_VOLUME_NAME,
                "mountPath": user_workspace_mount.as_posix(),
            }
        )

        # Determine the source path for the user workspace volume
        # Priority: workspace_stage_path > stage/user-default
        if workspace_stage_path:
            # Use workspace_stage_path
            workspace_source = workspace_stage_path
        elif stage:
            # Use stage with /user-default suffix
            workspace_source = f"{stage}/user-default"
        else:
            raise ValueError(
                "Either stage or workspace_stage_path must be provided for volume mounting"
            )

        volumes.append(
            {
                "name": constants.USER_WORKSPACE_VOLUME_NAME,
                "source": workspace_source,
            }
        )

        # Mount user vscode data volume - always uses stage location if stage is provided
        if stage:
            user_vscode_data_mount = PurePath(
                constants.USER_VSCODE_DATA_VOLUME_MOUNT_PATH
            )
            volume_mounts.append(
                {
                    "name": constants.USER_VSCODE_DATA_VOLUME_NAME,
                    "mountPath": user_vscode_data_mount.as_posix(),
                }
            )

            # VS Code data always uses stage location
            vscode_data_source = f"{stage}/.vscode-server/data"

            volumes.append(
                {
                    "name": constants.USER_VSCODE_DATA_VOLUME_NAME,
                    "source": vscode_data_source,
                }
            )

    # Setup environment variables
    env_vars = {
        constants.ENABLE_REMOTE_DEV_ENV_VAR: "true",
    }
    if environment_vars:
        env_vars.update(environment_vars)

    # Inject SSH public key for secure authentication
    if ssh_public_key:
        env_vars["SSH_PUBLIC_KEY"] = ssh_public_key

    cc.step(f"env vars: {env_vars}")

    # Setup Ray configuration if enabled
    endpoints = []

    # Define Ray ports for environment variables
    ray_ports = {
        "RAY_HEAD_GCS_PORT": "12001",
        "RAY_HEAD_CLIENT_SERVER_PORT": "10001",
        "RAY_HEAD_DASHBOARD_GRPC_PORT": "12002",
        "RAY_OBJECT_MANAGER_PORT": "12011",
        "RAY_NODE_MANAGER_PORT": "12012",
        "RAY_RUNTIME_ENV_AGENT_PORT": "12013",
        "RAY_DASHBOARD_AGENT_GRPC_PORT": "12014",
        "RAY_DASHBOARD_AGENT_LISTEN_PORT": "12015",
        "RAY_MIN_WORKER_PORT": "12031",
        "RAY_MAX_WORKER_PORT": "13000",
    }

    # Update environment variables for multi-node job
    env_vars.update(ray_ports)
    env_vars["ENABLE_HEALTH_CHECKS"] = "false"

    # Define Ray endpoints for intra-service instance communication
    ray_endpoints = [
        {"name": "ray-client-server-endpoint", "port": 10001, "protocol": "TCP"},
        {"name": "ray-gcs-endpoint", "port": 12001, "protocol": "TCP"},
        {"name": "ray-dashboard-grpc-endpoint", "port": 12002, "protocol": "TCP"},
        {"name": "ray-object-manager-endpoint", "port": 12011, "protocol": "TCP"},
        {"name": "ray-node-manager-endpoint", "port": 12012, "protocol": "TCP"},
        {"name": "ray-runtime-agent-endpoint", "port": 12013, "protocol": "TCP"},
        {"name": "ray-dashboard-agent-grpc-endpoint", "port": 12014, "protocol": "TCP"},
        {"name": "ephemeral-port-range", "portRange": "32768-60999", "protocol": "TCP"},
        {
            "name": "ray-worker-port-range",
            "portRange": "12031-13000",
            "protocol": "TCP",
        },
    ]
    endpoints.extend(ray_endpoints)

    # Add VS Code endpoint
    endpoints.append(
        {
            "name": "server-ui",
            "port": DEFAULT_SERVER_PORT,
            "public": True,
        }
    )

    # Add websocket endpoint
    endpoints.append(
        {
            "name": "websocket-ssh",
            "port": DEFAULT_WEBSOCKET_PORT,
            "public": True,
        }
    )

    # Add ray dashboard endpoint
    endpoints.append(
        {
            "name": "ray-dashboard",
            "port": int(constants.RAY_PORTS["HEAD_DASHBOARD_PORT"]),
            "public": True,
        }
    )

    metrics = []
    if enable_metrics:
        # https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services#label-spcs-available-platform-metrics
        metrics = [
            "system",
            "status",
            "network",
            "storage",
        ]

    # Create the full service specification
    spec_dict = {
        "containers": [
            {
                "name": constants.DEFAULT_CONTAINER_NAME,
                "image": image_spec.full_name,
                # "command": ["/usr/local/bin/_entrypoint.sh"],
                # "args": [
                #     (
                #         stage_mount.joinpath(v).as_posix()
                #         if isinstance(v, PurePath)
                #         else v
                #     )
                #     for v in payload.entrypoint
                # ],
                "env": env_vars,
                "volumeMounts": volume_mounts,
                "resources": {
                    "requests": resource_requests,
                    "limits": resource_limits,
                },
            },
        ],
        "volumes": volumes,
    }

    if endpoints:
        spec_dict["endpoints"] = endpoints  # type: ignore

    if metrics:
        spec_dict.update(
            {
                "platformMonitor": {  # type: ignore
                    "metricConfig": {
                        "groups": metrics,
                    },
                },
            }
        )

    # Assemble into service specification dict
    spec = {"spec": spec_dict}

    return spec


def merge_patch(base: Any, patch: Any, display_name: str = "") -> Any:
    """
    Implements a modified RFC7386 JSON Merge Patch
    https://datatracker.ietf.org/doc/html/rfc7386

    Behavior differs from the RFC in the following ways:
      1. Empty nested dictionaries resulting from the patch are treated as None and are pruned
      2. Attempts to merge lists of dicts using a merge key (default "name").
         See _merge_lists_of_dicts for details on list merge behavior.

    Args:
        base: The base object to patch.
        patch: The patch object.
        display_name: The name of the patch object for logging purposes.

    Returns:
        The patched object.
    """
    if not type(base) is type(patch):
        if base is not None:
            logging.warning(
                "Type mismatch while merging %s (base=%s, patch=%s)",
                display_name,
                type(base),
                type(patch),
            )
        return patch
    elif isinstance(patch, list) and all(isinstance(v, dict) for v in base + patch):
        # TODO: Should we prune empty lists?
        return _merge_lists_of_dicts(base, patch, display_name=display_name)
    elif not isinstance(patch, dict) or len(patch) == 0:
        return patch

    result = dict(base)  # Shallow copy
    for key, value in patch.items():
        if value is None:
            result.pop(key, None)
        else:
            merge_result = merge_patch(
                result.get(key, None), value, display_name="%s.%s" % (display_name, key)
            )
            if isinstance(merge_result, dict) and len(merge_result) == 0:
                result.pop(key, None)
            else:
                result[key] = merge_result

    return result


def _merge_lists_of_dicts(
    base: List[Dict[str, Any]],
    patch: List[Dict[str, Any]],
    merge_key: str = "name",
    display_name: str = "",
) -> List[Dict[str, Any]]:
    """
    Attempts to merge lists of dicts by matching on a merge key (default "name").
    - If the merge key is missing, the behavior falls back to overwriting the list.
    - If the merge key is present, the behavior is to match the list elements based on the
        merge key and preserving any unmatched elements from the base list.
    - Matched entries may be dropped in the following way(s):
        1. The matching patch entry has a None key entry, e.g. { "name": "foo", None: None }.

    Args:
        base: The base list of dicts.
        patch: The patch list of dicts.
        merge_key: The key to use for merging.
        display_name: The name of the patch object for logging purposes.

    Returns:
        The merged list of dicts if merging successful, else returns the patch list.
    """
    # Type safety check - ensure we're working with lists of dicts
    typed_base: List[Dict[str, Any]] = base
    typed_patch: List[Dict[str, Any]] = patch

    if any(merge_key not in d for d in typed_base + typed_patch):
        logging.warning(
            "Missing merge key %s in %s. Falling back to overwrite behavior.",
            merge_key,
            display_name,
        )
        return typed_patch

    # Build mapping of merge key values to list elements for the base list
    result: Dict[str, Dict[str, Any]] = {d[merge_key]: d for d in typed_base}
    if len(result) != len(typed_base):
        logging.warning(
            "Duplicate merge key %s in %s. Falling back to overwrite behavior.",
            merge_key,
            display_name,
        )
        return typed_patch

    # Apply patches
    for d in typed_patch:
        key = d[merge_key]

        # Removal case 1: `None` key in patch entry
        if None in d:
            result.pop(key, None)
            continue

        # Apply patch
        if key in result:
            d = merge_patch(
                result[key],
                d,
                display_name=f"{display_name}[{merge_key}={d[merge_key]}]",
            )
        result[key] = d

    return list(result.values())
