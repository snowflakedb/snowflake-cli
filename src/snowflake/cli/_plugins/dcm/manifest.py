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
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath

MANIFEST_FILE_NAME = "manifest.yml"
DCM_PROJECT_TYPE = "dcm_project"


class ManifestNotFoundError(Exception):
    """Manifest file does not exist."""


class InvalidManifestError(Exception):
    """Manifest file is not valid (empty, wrong type, wrong version)."""


class ManifestConfigurationError(Exception):
    """Manifest is valid but has configuration issues (target not found, config doesn't exist)."""


def _is_valid_manifest_version(version: str) -> bool:
    """Check if manifest version is valid (>= 2.0 and < 3.0)."""
    try:
        v = float(version)
        return 2.0 <= v < 3.0
    except ValueError:
        return False


@dataclass
class DCMTemplating:
    """Templating configuration for DCM manifest v2."""

    defaults: Dict[str, Any] = field(default_factory=dict)
    configurations: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "DCMTemplating":
        if not data:
            return cls()
        return cls(
            defaults=data.get("defaults", {}),
            configurations=data.get("configurations", {}),
        )


@dataclass
class DCMTarget:
    """Target configuration for DCM manifest v2."""

    name: str
    project_name: str
    templating_config: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DCMTarget":
        return cls(
            name=data.get("name", ""),
            project_name=data.get("project_name", ""),
            templating_config=data.get("templating_config"),
        )


@dataclass
class DCMManifest:
    """DCM manifest v2 structure."""

    manifest_version: str
    project_type: str
    default_target: Optional[str] = None
    targets: Dict[str, DCMTarget] = field(default_factory=dict)
    templating: DCMTemplating = field(default_factory=DCMTemplating)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DCMManifest":
        targets_data = data.get("targets", {})
        targets = {
            name: DCMTarget.from_dict(target_data | {"name": name})
            for name, target_data in targets_data.items()
        }

        default_target = data.get("default_target")

        # if there's only 1 target defined we assume it's the default
        if default_target is None and len(targets) == 1:
            default_target = next(iter(targets.keys()))

        return cls(
            manifest_version=str(data.get("manifest_version", "")),
            project_type=data.get("type", ""),
            default_target=default_target,
            targets=targets,
            templating=DCMTemplating.from_dict(data.get("templating")),
        )

    @classmethod
    def load(cls, source_path: SecurePath) -> "DCMManifest":
        """Load and validate manifest from source path."""
        dcm_manifest_file = source_path / MANIFEST_FILE_NAME
        if not dcm_manifest_file.exists():
            raise ManifestNotFoundError(
                f"{MANIFEST_FILE_NAME} was not found in directory {source_path.path}."
            )

        with dcm_manifest_file.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
            data = yaml.safe_load(fd)
            if not data:
                raise InvalidManifestError("Manifest file is empty or invalid.")

            manifest = cls.from_dict(data)
            manifest.validate()
            return manifest

    def validate(self) -> None:
        """Validate the manifest structure."""
        if not self.project_type:
            raise InvalidManifestError(
                f"Manifest file type is undefined. Expected {DCM_PROJECT_TYPE}."
            )

        if self.project_type.lower() != DCM_PROJECT_TYPE:
            raise InvalidManifestError(
                f"Manifest file is defined for type {self.project_type}. Expected {DCM_PROJECT_TYPE}."
            )

        if not _is_valid_manifest_version(self.manifest_version):
            raise InvalidManifestError(
                f"Manifest version '{self.manifest_version}' is not supported. Expected version >= 2.0 and < 3.0."
            )

    def validate_target_configuration_exists(self, target: DCMTarget):
        if (
            target.templating_config
            and target.templating_config not in self.templating.configurations
        ):
            raise ManifestConfigurationError(
                f"Target '{target.name}' references unknown configuration '{target.templating_config}'."
            )

    def get_configuration_names(self) -> List[str]:
        """Return list of available configuration names."""
        return list(self.templating.configurations.keys())

    def get_target_names(self) -> List[str]:
        """Return list of available target names."""
        return list(self.targets.keys())

    def get_target(self, target_name: str) -> DCMTarget:
        """Get a specific target by name."""
        if target_name not in self.targets:
            raise ManifestConfigurationError(
                f"Target '{target_name}' not found in manifest."
            )
        target = self.targets[target_name]
        self.validate_target_configuration_exists(target)
        return target

    def get_effective_target(self, target_name: Optional[str] = None) -> DCMTarget:
        """Get effective target - specified target or default."""
        if target_name:
            return self.get_target(target_name)
        if self.default_target:
            return self.get_target(self.default_target)
        raise ManifestConfigurationError(
            "No target specified and no default_target defined in manifest."
        )


@dataclass
class TargetContext:
    """Resolved context from target configuration."""

    project_identifier: FQN
    configuration: Optional[str] = None
