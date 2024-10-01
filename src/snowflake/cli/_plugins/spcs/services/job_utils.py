import io
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.identifiers import FQN
from snowflake.snowpark import Session


@dataclass
class _ComputeResources:
    cpu: float  # Number of vCPU cores
    memory: float  # Memory in GiB
    gpu: int = 0  # Number of GPUs
    gpu_type: Optional[str] = None


@dataclass
class _ImageSpec:
    repo: str
    arch: str
    family: str
    tag: str
    resource_requests: _ComputeResources
    resource_limits: _ComputeResources

    @property
    def full_name(self) -> str:
        return f"{self.repo}/st_plat/runtime/{self.arch}/{self.family}:{self.tag}"


# TODO: Query Snowflake for resource information instead of relying on this hardcoded
#       table from https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool
_COMMON_INSTANCE_FAMILIES = {
    "CPU_X64_XS": _ComputeResources(cpu=1, memory=6),
    "CPU_X64_S": _ComputeResources(cpu=3, memory=13),
    "CPU_X64_M": _ComputeResources(cpu=6, memory=28),
    "CPU_X64_L": _ComputeResources(cpu=28, memory=116),
    "HIGHMEM_X64_S": _ComputeResources(cpu=6, memory=58),
}
_AWS_INSTANCE_FAMILIES = {
    "HIGHMEM_X64_M": _ComputeResources(cpu=28, memory=240),
    "HIGHMEM_X64_L": _ComputeResources(cpu=124, memory=984),
    "GPU_NV_S": _ComputeResources(cpu=6, memory=27, gpu=1, gpu_type="A10G"),
    "GPU_NV_M": _ComputeResources(cpu=44, memory=178, gpu=4, gpu_type="A10G"),
    "GPU_NV_L": _ComputeResources(cpu=92, memory=1112, gpu=8, gpu_type="A100"),
}
_AZURE_INSTANCE_FAMILIES = {
    "HIGHMEM_X64_M": _ComputeResources(cpu=28, memory=244),
    "HIGHMEM_X64_L": _ComputeResources(cpu=92, memory=654),
    "GPU_NV_XS": _ComputeResources(cpu=3, memory=26, gpu=1, gpu_type="T4"),
    "GPU_NV_SM": _ComputeResources(cpu=32, memory=424, gpu=1, gpu_type="A10"),
    "GPU_NV_2M": _ComputeResources(cpu=68, memory=858, gpu=2, gpu_type="A10"),
    "GPU_NV_3M": _ComputeResources(cpu=44, memory=424, gpu=2, gpu_type="A100"),
    "GPU_NV_SL": _ComputeResources(cpu=92, memory=858, gpu=4, gpu_type="A100"),
}
_CLOUD_INSTANCE_FAMILIES = {
    "aws": _AWS_INSTANCE_FAMILIES,
    "azure": _AZURE_INSTANCE_FAMILIES,
}


def _get_node_resources(session: Session, compute_pool: str) -> _ComputeResources:
    """Extract resource information for the specified compute pool"""
    # Get the instance family
    (row,) = session.sql(f"show compute pools like '{compute_pool}'").collect()
    instance_family: str = row["instance_family"]

    # Get the cloud we're using (AWS, Azure, etc)
    (row,) = session.sql(f"select current_region()").collect()
    region: str = row[0]
    region_group, region_name = f".{region}".split(".")[
        -2:
    ]  # Prepend a period so we always get at least 2 splits
    regions = session.sql(f"show regions like '{region_name}'").collect()
    if region_group:
        regions = [r for r in regions if r["region_group"] == region_group]
    cloud = regions[0]["cloud"]

    return (
        _COMMON_INSTANCE_FAMILIES.get(instance_family)
        or _CLOUD_INSTANCE_FAMILIES[cloud][instance_family]
    )


def _get_image_spec(session: Session, compute_pool: str) -> _ImageSpec:
    # Retrieve compute pool node resources
    resources = _get_node_resources(session, compute_pool=compute_pool)

    # Use MLRuntime image
    # TODO: Build new image if needed
    image_repo = "/snowflake/images/snowflake_images"
    image_arch = "x86"
    image_family = (
        "generic_gpu/runtime_image/snowbooks"
        if resources.gpu > 0
        else "runtime_image/snowbooks"
    )
    image_tag = session.sql(
        f"SHOW PARAMETERS LIKE 'RUNTIME_BASE_IMAGE_TAG' IN ACCOUNT"
    ).collect()[0]["value"]

    # TODO: Should each instance consume the entire pod?
    return _ImageSpec(
        repo=image_repo,
        arch=image_arch,
        family=image_family,
        tag=image_tag,
        resource_requests=resources,
        resource_limits=resources,
    )


def _generate_spec(
    image_spec: _ImageSpec,
    stage_path: str,
    script_path: str,
    args: Optional[List[str]] = None,
    env_vars: Optional[Dict[str, str]] = None,
) -> dict:
    volumes: List[Dict[str, str]] = []
    volume_mounts: List[Dict[str, str]] = []

    # Set resource requests/limits, including nvidia.com/gpu quantity if applicable
    resource_requests = {
        "cpu": f"{image_spec.resource_requests.cpu * 1000}m",
        "memory": f"{image_spec.resource_limits.memory}Gi",
    }
    resource_limits = {
        "cpu": f"{image_spec.resource_requests.cpu * 1000}m",
        "memory": f"{image_spec.resource_limits.memory}Gi",
    }
    if image_spec.resource_limits.gpu > 0:
        resource_requests["nvidia.com/gpu"] = str(image_spec.resource_requests.gpu)
        resource_limits["nvidia.com/gpu"] = str(image_spec.resource_limits.gpu)

    # Create container spec
    mce_container: Dict[str, Any] = {
        "name": "main",
        "image": image_spec.full_name,
        "volumeMounts": volume_mounts,
        "resources": {
            "requests": resource_requests,
            "limits": resource_limits,
        },
    }

    # TODO: Add local volume for ephemeral artifacts

    # Mount 30% of memory limit as a memory-backed volume
    memory_volume_name = "memory-volume"
    memory_volume_size = min(
        round(image_spec.resource_limits.memory * 0.3),
        image_spec.resource_requests.memory,
    )
    volume_mounts.append(
        {
            "name": memory_volume_name,
            "mountPath": "/dev/shm",
        }
    )
    volumes.append(
        {
            "name": memory_volume_name,
            "source": "memory",
            "size": f"{memory_volume_size}Gi",
        }
    )

    # Mount payload as volume
    # TODO: Mount subPath only once that's supported for proper isolation
    stage_name, stage_subpath = stage_path.split("/", 2)
    stage_mount = "/opt/userapp"
    stage_volume_name = "stage-volume"
    volume_mounts.append(
        {
            "name": stage_volume_name,
            "mountPath": stage_mount,
        }
    )
    volumes.append(
        {
            "name": stage_volume_name,
            "source": stage_name,
        }
    )

    # TODO: Add hooks for endpoints for integration with TensorBoard, W&B, etc

    # Propagate user payload config
    commands = {
        ".py": "python",
        ".sh": "bash",
        ".rb": "ruby",
        ".pl": "perl",
        ".js": "node",
        # Add more formats as needed
    }
    _, ext = os.path.splitext(script_path)
    command = commands[ext]
    mce_container["command"] = [
        command,
        os.path.join(stage_mount, stage_subpath, script_path),
    ]
    if args:
        mce_container["args"] = args
    if env_vars:
        mce_container["env"] = env_vars

    return {
        "spec": {
            "containers": [mce_container],
            "volumes": volumes,
        }
    }


def _prepare_payload(
    stage_path: str,
    source: Path,
    entrypoint: Path,
    enable_pip: bool = False,
) -> str:
    """Load payload onto stage"""
    stage_manager = StageManager()
    stage = stage_manager.get_stage_from_path(stage_path)
    stage_manager.create(
        fqn=FQN.from_string(stage.lstrip("@")),
        comment="deployments managed by Snowflake CLI",
    )

    # TODO: Detect if source is a git repo or existing stage
    if not (source.exists() and entrypoint.exists()):
        raise FileNotFoundError(f"{source} or {entrypoint} does not exist")

    # Upload payload to stage
    if source.is_dir():
        # Filter to only files in source since Snowflake PUT can't handle directories
        for path in set(
            p.parent.joinpath(f"*{p.suffix}") if p.suffix else p
            for p in source.rglob("*")
            if p.is_file()
        ):
            stage_manager.put(
                str(path.resolve()), stage_path, overwrite=True, auto_compress=False
            )
    else:
        stage_manager.put(
            str(source.resolve()), stage_path, overwrite=True, auto_compress=False
        )
    cli_console.message(f"Uploaded payload to stage {stage_path}")

    if enable_pip and source.is_dir() and entrypoint.suffix == ".py" and enable_pip:
        # Multi-file Python payload: generate and inject a launch script
        script_content = _generate_launch_script(entrypoint.name).encode(
            encoding="utf-8"
        )
        entrypoint = Path("startup.sh")
        # TODO: Switch to stage_manager native method if/when stream support available
        stage_manager.snowpark_session.file.put_stream(
            io.BytesIO(script_content),
            f"{stage_path}/{entrypoint}",
            overwrite=True,
            auto_compress=False,
        )

    return entrypoint.name


def _generate_launch_script(entrypoint: str) -> str:
    assert entrypoint.endswith(
        ".py"
    ), f"Launch script only supports Python entrypoints! Got: {entrypoint}"
    return f"""
#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the directory of the script
SCRIPT_DIR="$( dirname "$0" )"

# Check if requirements.txt exists and install if found
if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
    pip install --no-cache-dir --quiet -r "$SCRIPT_DIR/requirements.txt"
    if [ $? -ne 0 ]; then
        echo "Failed to install requirements"
        exit 1
    fi
fi

# Execute the Python script
python "$SCRIPT_DIR/{entrypoint}"
"""


def prepare_spec(
    session: Session,
    service_name: str,
    compute_pool: str,
    stage_name: str,
    payload: Path,
    entrypoint: Path,
    enable_pip: bool = False,
    args: Optional[List[str]] = None,
    env: Optional[Dict[str, str]] = None,
) -> str:

    # Generate image spec based on compute pool
    image_spec = _get_image_spec(session, compute_pool=compute_pool)

    # Prepare payload
    stage_path = f"@{stage_name}/{service_name}"
    script_path = _prepare_payload(
        stage_path,
        source=payload,
        entrypoint=entrypoint,
        enable_pip=enable_pip,
    )

    spec = _generate_spec(
        image_spec=image_spec,
        stage_path=stage_path,
        script_path=script_path,
        args=args,
        env_vars=env,
    )
    return json.dumps(spec)
