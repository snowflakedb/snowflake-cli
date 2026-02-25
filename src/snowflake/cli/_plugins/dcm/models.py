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
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

import yaml
from snowflake.cli._plugins.dcm.exceptions import (
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath

MANIFEST_FILE_NAME = "manifest.yml"
DCM_PROJECT_TYPE = "dcm_project"
SUPPORTED_MANIFEST_VERSION = 2
log = logging.getLogger(__name__)


@dataclass
class DCMTemplating:
    """Templating configuration for DCM manifest v2."""

    defaults: Dict[str, Any] = field(default_factory=dict)
    configurations: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "DCMTemplating":
        if not data:
            return cls()
        configurations = data.get("configurations", {})
        return cls(
            defaults=data.get("defaults", {}),
            configurations={k.upper(): v for k, v in configurations.items()},
        )


@dataclass
class DCMTarget:
    """Target configuration for DCM manifest v2."""

    name: str
    project_name: str
    templating_config: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DCMTarget":
        templating_config = data.get("templating_config")
        return cls(
            name=data.get("name", "").upper(),
            project_name=data.get("project_name", ""),
            templating_config=templating_config.upper() if templating_config else None,
        )


@dataclass
class DCMManifest:
    """DCM manifest v2 structure."""

    manifest_version: int
    project_type: str
    default_target: Optional[str] = None
    targets: Dict[str, DCMTarget] = field(default_factory=dict)
    templating: DCMTemplating = field(default_factory=DCMTemplating)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DCMManifest":
        targets_data = data.get("targets", {})
        targets = {
            name.upper(): DCMTarget.from_dict(target_data | {"name": name.upper()})
            for name, target_data in targets_data.items()
        }

        default_target = data.get("default_target")

        # if there's only 1 target defined we assume it's the default
        if default_target is None and len(targets) == 1:
            default_target = next(iter(targets.keys()))
            log.info(
                "Derived default target from single target manifest (default_target=%s).",
                default_target,
            )

        manifest_version = data.get("manifest_version")
        if manifest_version is None:
            raise InvalidManifestError("Manifest version is undefined.")
        try:
            manifest_version = int(manifest_version)
        except (ValueError, TypeError):
            raise InvalidManifestError(
                f"Manifest version '{data.get('manifest_version')}' is not valid. Expected an integer."
            )

        manifest = cls(
            manifest_version=manifest_version,
            project_type=data.get("type", "").lower(),
            default_target=default_target.upper()
            if isinstance(default_target, str)
            else None,
            targets=targets,
            templating=DCMTemplating.from_dict(data.get("templating")),
        )
        manifest.validate()
        return manifest

    @classmethod
    def load(cls, source_path: SecurePath) -> "DCMManifest":
        """Load and validate manifest from source path."""
        dcm_manifest_file = source_path / MANIFEST_FILE_NAME
        log.info("Loading DCM manifest from %s.", dcm_manifest_file)
        if not dcm_manifest_file.exists():
            log.info("DCM manifest file not found at %s.", dcm_manifest_file)
            raise ManifestNotFoundError(
                f"{MANIFEST_FILE_NAME} was not found in directory {source_path.path}."
            )

        with dcm_manifest_file.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
            data = yaml.safe_load(fd)
            if not data:
                log.info(
                    "DCM manifest file is empty or invalid at %s.", dcm_manifest_file
                )
                raise InvalidManifestError("Manifest file is empty or invalid.")

            return cls.from_dict(data)

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

        if self.manifest_version != SUPPORTED_MANIFEST_VERSION:
            raise InvalidManifestError(
                f"Manifest version {self.manifest_version} is not supported. Expected version {SUPPORTED_MANIFEST_VERSION}."
            )

    def _validate_target_configuration_exists(self, target: DCMTarget):
        if (
            target.templating_config
            and target.templating_config not in self.templating.configurations
        ):
            log.info(
                "DCM target references unknown templating configuration (target=%s, configuration=%s).",
                target.name,
                target.templating_config,
            )
            raise ManifestConfigurationError(
                f"Target '{target.name}' references unknown configuration '{target.templating_config}'."
            )

    def get_target(self, target_name: str) -> DCMTarget:
        """Get a specific target by name."""
        target_name = target_name.upper()
        log.info("Resolving DCM target '%s'.", target_name)
        if target_name not in self.targets:
            log.info(
                "Requested DCM target '%s' was not found in manifest.", target_name
            )
            raise ManifestConfigurationError(
                f"Target '{target_name}' not found in manifest."
            )
        target = self.targets[target_name]
        self._validate_target_configuration_exists(target)
        return target

    def get_effective_target(self, target_name: Optional[str] = None) -> DCMTarget:
        """Get effective target - specified target or default."""
        if target_name:
            return self.get_target(target_name)
        if self.default_target:
            return self.get_target(self.default_target)
        log.info(
            "No DCM target specified and no default target configured in manifest."
        )
        raise ManifestConfigurationError(
            "No target specified and no default_target defined in manifest."
        )


@dataclass
class TargetContext:
    """Resolved context from target configuration."""

    project_identifier: FQN
    configuration: Optional[str] = None
