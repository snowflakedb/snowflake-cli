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
class _ImageResources:
    cpu: float  # Number of vCPU cores
    gpu: int  # Number of GPUs
    memory: float  # Memory in GiB


@dataclass
class _ImageSpec:
    repo: str
    arch: str
    family: str
    tag: str
    resources: _ImageResources

    @property
    def full_name(self) -> str:
        return f"{self.repo}/st_plat/runtime/{self.arch}/{self.family}:{self.tag}"


def _get_image_spec(session: Session, compute_pool: str) -> _ImageSpec:
    # Derive container type and resource settings from compute pool instance family
    (row,) = session.sql(f"show compute pools like '{compute_pool}'").collect()
    instance_family: str = row["instance_family"]

    # TODO: Detect resources by instance family
    resources = _ImageResources(
        cpu=1,
        gpu=instance_family.startswith("GPU"),
        memory=1,
    )

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

    return _ImageSpec(
        repo=image_repo,
        arch=image_arch,
        family=image_family,
        tag=image_tag,
        resources=resources,
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

    # TODO: Set resource requests/limits, including nvidia.com/gpu quantity if applicable
    mce_container: Dict[str, Any] = {
        "name": "primary-container",
        "image": image_spec.full_name,
        "volumeMounts": volume_mounts,  # TODO: Verify this gets updated in-place
    }

    # TODO: Add memory volume
    #       https://docs.snowflake.com/en/developer-guide/snowpark-container-services/specification-reference#label-snowpark-containers-spec-volume

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
    cli_console.message(f"Uploading payload to stage {stage_path}")
    if not (source.exists() and entrypoint.exists()):
        raise FileNotFoundError(f"{source} or {entrypoint} does not exist")

    # Upload payload to stage
    if source.is_dir():
        # TODO: Support nested directories (or at least ignore them so PUT doesn't fail)
        source = source / "*"
    stage_manager.put(
        str(source.resolve()), stage_path, overwrite=True, auto_compress=False
    )

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
    # TODO: pip install requires EAI
    return f"""
#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Get the directory of the script
SCRIPT_DIR="$( dirname "$0" )"

# Check if requirements.txt exists and install if found
if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
    pip install -r "$SCRIPT_DIR/requirements.txt" -q
    if [ $? -ne 0 ]; then
        echo "Failed to install requirements"
        exit 1
    fi
fi

# Execute the Python script
python "$SCRIPT_DIR/{entrypoint}"
"""


def generate_name() -> str:
    raise NotImplementedError


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
