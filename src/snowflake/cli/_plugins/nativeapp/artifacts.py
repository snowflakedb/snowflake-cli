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

from __future__ import annotations

import os
from collections import namedtuple
from pathlib import Path
from typing import Any, Dict, List, Optional

from click.exceptions import ClickException
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.artifacts.common import ArtifactError, DeployRootError
from snowflake.cli.api.artifacts.utils import symlink_or_copy
from snowflake.cli.api.cli_global_context import span
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.project.util import to_identifier
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.api.utils.path_utils import delete
from yaml import safe_load


@span("bundle")
def build_bundle(
    project_root: Path,
    deploy_root: Path,
    artifacts: List[PathMapping],
) -> BundleMap:
    """
    Prepares a local folder (deploy_root) with configured app artifacts.
    This folder can then be uploaded to a stage.
    Returns a map of the copied source files, pointing to where they were copied.
    """
    resolved_root = deploy_root.resolve()
    if resolved_root.exists() and not resolved_root.is_dir():
        raise DeployRootError(
            f"Deploy root {resolved_root} exists, but is not a directory!"
        )

    if project_root.resolve() not in resolved_root.parents:
        raise DeployRootError(
            f"Deploy root {resolved_root} is not a descendent of the project directory!"
        )

    # users may have removed files or entire artifact mappings from their project
    # definition since the last time we bundled; we need to clear the deploy root first
    if resolved_root.exists():
        delete(resolved_root)

    bundle_map = bundle_artifacts(project_root, deploy_root, artifacts)
    if bundle_map.is_empty():
        raise ArtifactError(
            "No artifacts mapping found in project definition, nothing to do."
        )

    return bundle_map


def bundle_artifacts(
    project_root: Path, deploy_root: Path, artifacts: list[PathMapping]
):
    """
    Internal implementation of build_bundle that assumes
    that validation is being done by the caller.
    """
    bundle_map = BundleMap(project_root=project_root, deploy_root=deploy_root)
    for artifact in artifacts:
        bundle_map.add(artifact)

    for absolute_src, absolute_dest in bundle_map.all_mappings(
        absolute=True, expand_directories=False
    ):
        symlink_or_copy(absolute_src, absolute_dest, deploy_root=deploy_root)

    return bundle_map


def find_manifest_file(deploy_root: Path) -> Path:
    """
    Find manifest.yml file, if available, in the deploy_root of the Snowflake Native App project.
    """
    resolved_root = deploy_root.resolve()
    for root, _, files in os.walk(resolved_root):
        for file in files:
            if file.lower() == "manifest.yml":
                return Path(os.path.join(root, file))

    raise ClickException(
        "Required manifest.yml file not found in the deploy root of the Snowflake Native App project."
    )


def find_and_read_manifest_file(deploy_root: Path) -> Dict[str, Any]:
    """
    Finds the manifest file in the deploy root of the project, and reads the contents and returns them
    as a dictionary.
    """
    manifest_file = find_manifest_file(deploy_root=deploy_root)
    with SecurePath(manifest_file).open(
        "r", read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB
    ) as file:
        manifest_content = safe_load(file.read())
    return manifest_content or {}


def find_setup_script_file(deploy_root: Path) -> Path:
    """
    Find the setup script file, if available, in the deploy_root of the Snowflake Native App project.
    """
    artifacts = "artifacts"
    setup_script = "setup_script"

    manifest_content = find_and_read_manifest_file(deploy_root=deploy_root)

    if (artifacts in manifest_content) and (
        setup_script in manifest_content[artifacts]
    ):
        setup_script_rel_path = manifest_content[artifacts][setup_script]
        file_name = Path(deploy_root / setup_script_rel_path)
        if file_name.is_file():
            return file_name
        else:
            raise ClickException(f"Could not find setup script file at {file_name}.")
    else:
        raise ClickException(
            "Manifest.yml file must contain an artifacts section to specify the location of the setup script."
        )


VersionInfo = namedtuple("VersionInfo", ["version_name", "patch_number", "label"])


def find_version_info_in_manifest_file(
    deploy_root: Path,
) -> VersionInfo:
    """
    Find version and patch, if available, in the manifest.yml file.
    """
    name_field = "name"
    patch_field = "patch"
    label_field = "label"

    manifest_content = find_and_read_manifest_file(deploy_root=deploy_root)

    version_name: Optional[str] = None
    patch_number: Optional[int] = None
    label: Optional[str] = None

    version_info = manifest_content.get("version", None)
    if version_info is not None:
        if not isinstance(version_info, dict):
            raise ClickException(
                "Error occurred while reading manifest.yml. Received unexpected version format."
            )
        if version_info.get(name_field) is not None:
            version_name = to_identifier(str(version_info[name_field]))
        if version_info.get(patch_field) is not None:
            patch_number = int(version_info[patch_field])
        if version_info.get(label_field) is not None:
            label = str(version_info[label_field])

    return VersionInfo(version_name, patch_number, label)


def find_events_definitions_in_manifest_file(
    deploy_root: Path,
) -> List[Dict[str, str]]:
    """
    Find events definitions, if available, in the manifest.yml file.
    Events definitions can be found under this section in the manifest.yml file:

    configuration:
        telemetry_event_definitions:
            - type: ERRORS_AND_WARNINGS
              sharing: MANDATORY
            - type: DEBUG_LOGS
              sharing: OPTIONAL
    """
    manifest_content = find_and_read_manifest_file(deploy_root=deploy_root)

    configuration_section = manifest_content.get("configuration", None)
    events_definitions = []
    if configuration_section and isinstance(configuration_section, dict):
        telemetry_section = configuration_section.get("telemetry_event_definitions", [])
        if isinstance(telemetry_section, list):
            for event in telemetry_section:
                if isinstance(event, dict):
                    event_type = event.get("type", "")
                    events_definitions.append(
                        {
                            "name": f"SNOWFLAKE${event_type}",
                            "type": event_type,
                            "sharing": event.get("sharing", ""),
                        }
                    )

    return events_definitions
