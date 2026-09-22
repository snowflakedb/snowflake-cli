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
"""Feature-store ``manifest.yml`` model — parsed the same way as DCM.

Load, ``from_dict``, target-name canonicalization, default-target
derivation, and templating-configuration lookup follow
``snowflake.cli._plugins.dcm.models.DCMManifest``. Target *fields*
differ (feature-store targets carry ``database`` / ``schema`` / ``role``
instead of DCM's ``project_name`` / ``project_owner``), and the project
type / schema version are ``feature_store`` / ``1``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml
from snowflake.cli._plugins.feature.exceptions import (
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.secure_path import SecurePath

MANIFEST_FILE_NAME = "manifest.yml"
FEATURE_STORE_PROJECT_TYPE = "feature_store"
SOURCES_FOLDER = "sources"
FEATURE_VIEWS_SUBFOLDER = "feature_views"
DATASOURCES_SUBFOLDER = "datasources"
SUPPORTED_MANIFEST_VERSION = 1
PLANS_SUBPATH = ("out", "plan")
log = logging.getLogger(__name__)


@dataclass
class FSTemplating:
    """Templating configuration — same shape as DCM ``defaults`` / ``configurations``."""

    defaults: Dict[str, Any] = field(default_factory=dict)
    configurations: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "FSTemplating":
        if not data:
            return cls()
        configurations = data.get("configurations") or {}
        return cls(
            defaults=data.get("defaults") or {},
            configurations={str(k).upper(): v for k, v in configurations.items()},
        )


@dataclass
class FSTarget:
    """Target configuration for a feature-store manifest.

    Required at resolve time: ``account_identifier``, ``database``,
    ``schema``. ``role`` and ``templating_config`` are optional.
    ``warehouse`` is not a manifest field (it comes from the active
    connection) and is rejected if declared.
    """

    name: str
    account_identifier: str
    database: str
    schema: str
    role: str = ""
    templating_config: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FSTarget":
        if not isinstance(data, dict):
            raise InvalidManifestError(
                "Manifest target must be a mapping of field name to value, "
                f"got {type(data).__name__}."
            )
        name = cls._require_str(data.get("name", ""), "name", "") or ""
        name = name.upper()
        if "warehouse" in data:
            raise ManifestConfigurationError(
                f"Target '{name}' has unsupported field 'warehouse'. "
                "Warehouse comes from the active connection."
            )
        templating_config = cls._require_str(
            data.get("templating_config"), "templating_config", name
        )
        account_identifier = cls._require_str(
            data.get("account_identifier", ""), "account_identifier", name
        )
        database = cls._require_str(data.get("database", ""), "database", name)
        schema = cls._require_str(data.get("schema", ""), "schema", name)
        role = cls._require_str(data.get("role"), "role", name)
        # Unquoted Snowflake identifiers are stored uppercase. Fold
        # database, schema, and role the same way so manifest vs
        # server/flag comparisons cannot case-diverge. Quoted mixed-case
        # names are out of scope. account_identifier is compared via
        # AccountIdentifier and is left as authored.
        return cls(
            name=name,
            account_identifier=account_identifier if account_identifier else "",
            database=database.upper() if database else "",
            schema=schema.upper() if schema else "",
            role=role.upper() if role else "",
            templating_config=templating_config.upper() if templating_config else None,
        )

    @staticmethod
    def _require_str(value: Any, field_name: str, target_name: str) -> Optional[str]:
        """Return ``value`` if it is a string (or ``None``); otherwise raise.

        A missing / null field keeps its default (``None``); a value of the
        wrong YAML type (int, list, mapping) is a malformed manifest and
        raises ``InvalidManifestError`` rather than reaching ``.upper()`` and
        leaking a raw ``AttributeError``.
        """
        if value is None or isinstance(value, str):
            return value
        label = f"Target '{target_name}' " if target_name else "Target "
        raise InvalidManifestError(
            f"{label}field '{field_name}' must be a string, "
            f"got {type(value).__name__}."
        )


@dataclass
class FSManifest:
    """Feature-store manifest. Parsed like DCM: ``SecurePath`` + ``yaml.safe_load``."""

    manifest_version: int
    project_type: str
    default_target: Optional[str] = None
    targets: Dict[str, FSTarget] = field(default_factory=dict)
    templating: FSTemplating = field(default_factory=FSTemplating)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FSManifest":
        if not isinstance(data, dict):
            raise InvalidManifestError(
                "Manifest must be a mapping (top-level YAML object), "
                f"got {type(data).__name__}."
            )
        raw_targets = data.get("targets")
        if raw_targets is None:
            targets_data: Dict[Any, Any] = {}
        elif not isinstance(raw_targets, dict):
            raise InvalidManifestError(
                "Manifest 'targets' must be a mapping of target name to its "
                f"configuration, got {type(raw_targets).__name__}."
            )
        else:
            targets_data = raw_targets

        targets = {}
        for name, target_data in targets_data.items():
            if not isinstance(name, str):
                raise InvalidManifestError(
                    "Manifest target name must be a string, "
                    f"got {type(name).__name__}."
                )
            if target_data is not None and not isinstance(target_data, dict):
                raise InvalidManifestError(
                    f"Target '{name.upper()}' must be a mapping of field name "
                    f"to value, got {type(target_data).__name__}."
                )
            targets[name.upper()] = FSTarget.from_dict(
                dict(target_data or {}) | {"name": name.upper()}
            )

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
            templating=FSTemplating.from_dict(data.get("templating")),
        )
        manifest.validate()
        return manifest

    @classmethod
    def load(cls, source_path: SecurePath) -> "FSManifest":
        """Load and validate manifest from source path (same path as DCM)."""
        manifest_file = source_path / MANIFEST_FILE_NAME
        log.info("Loading feature-store manifest from %s.", manifest_file)
        if not manifest_file.exists():
            log.info("Feature-store manifest file not found at %s.", manifest_file)
            raise ManifestNotFoundError(
                f"{MANIFEST_FILE_NAME} was not found in directory {source_path.path}."
            )

        with manifest_file.open(read_file_limit_mb=DEFAULT_SIZE_LIMIT_MB) as fd:
            data = yaml.safe_load(fd)
            if not data:
                log.info(
                    "Feature-store manifest file is empty or invalid at %s.",
                    manifest_file,
                )
                raise InvalidManifestError("Manifest file is empty or invalid.")

            return cls.from_dict(data)

    def validate(self) -> None:
        """Validate the manifest structure."""
        if not self.project_type:
            raise InvalidManifestError(
                f"Manifest file type is undefined. Expected {FEATURE_STORE_PROJECT_TYPE}."
            )

        if self.project_type.lower() != FEATURE_STORE_PROJECT_TYPE:
            raise InvalidManifestError(
                f"Manifest file is defined for type {self.project_type}. Expected {FEATURE_STORE_PROJECT_TYPE}."
            )

        if self.manifest_version != SUPPORTED_MANIFEST_VERSION:
            raise InvalidManifestError(
                f"Manifest version {self.manifest_version} is not supported. Expected version {SUPPORTED_MANIFEST_VERSION}."
            )

    def _validate_target_configuration_exists(self, target: FSTarget) -> None:
        if (
            target.templating_config
            and target.templating_config not in self.templating.configurations
        ):
            log.info(
                "Feature-store target references unknown templating configuration (target=%s, configuration=%s).",
                target.name,
                target.templating_config,
            )
            raise ManifestConfigurationError(
                f"Target '{target.name}' references unknown configuration '{target.templating_config}'."
            )

    def _validate_target_required_fields(self, target: FSTarget) -> None:
        missing = []
        if not target.account_identifier:
            missing.append("account_identifier")
        if not target.database:
            missing.append("database")
        if not target.schema:
            missing.append("schema")
        if missing:
            raise ManifestConfigurationError(
                f"Target '{target.name}' is missing required field(s): {', '.join(missing)}."
            )

    def get_target(self, target_name: str) -> FSTarget:
        """Get a specific target by name."""
        target_name = target_name.upper()
        log.info("Resolving feature-store target '%s'.", target_name)
        if target_name not in self.targets:
            log.info(
                "Requested feature-store target '%s' was not found in manifest.",
                target_name,
            )
            raise ManifestConfigurationError(
                f"Target '{target_name}' not found in manifest."
            )
        target = self.targets[target_name]
        self._validate_target_configuration_exists(target)
        self._validate_target_required_fields(target)
        return target

    def get_effective_target(self, target_name: Optional[str] = None) -> FSTarget:
        """Get effective target - specified target or default."""
        if target_name:
            return self.get_target(target_name)
        if self.default_target:
            return self.get_target(self.default_target)
        log.info(
            "No feature-store target specified and no default target configured in manifest."
        )
        raise ManifestConfigurationError(
            "No target specified and no default_target defined in manifest."
        )


@dataclass(frozen=True)
class FSProjectPaths:
    """Resolved on-disk paths for a feature-store project."""

    project_root: Path
    manifest_path: Path
    sources_dir: Path
    plans_dir: Path

    @classmethod
    def from_project_root(cls, project_root: Path) -> "FSProjectPaths":
        resolved = Path(project_root).resolve()
        return cls(
            project_root=resolved,
            manifest_path=resolved / MANIFEST_FILE_NAME,
            sources_dir=resolved / SOURCES_FOLDER,
            plans_dir=resolved / Path(*PLANS_SUBPATH),
        )

    @property
    def feature_views_dir(self) -> Path:
        """Canonical authored FeatureView specs dir (``sources/feature_views``)."""
        return self.sources_dir / FEATURE_VIEWS_SUBFOLDER

    @property
    def datasources_dir(self) -> Path:
        """Canonical authored datasource specs dir (``sources/datasources``)."""
        return self.sources_dir / DATASOURCES_SUBFOLDER

    @classmethod
    def discover(cls, start: Optional[Path] = None) -> "FSProjectPaths":
        origin = Path(start) if start is not None else Path.cwd()
        return cls.from_project_root(find_project_root(origin))


def find_project_root(start: Path) -> Path:
    """Resolve the project root: ``start`` must itself contain ``manifest.yml``.

    Matches ``DCMManifest.load`` — the given directory (``--from`` / cwd) is
    the project root only when it directly contains ``manifest.yml``. Ancestor
    directories are **not** searched, so running from a nested ``sources/``
    subdir (or an unrelated scratch dir) can never silently bind to a parent
    project's ``manifest.yml``.
    """
    origin = Path(start)
    current = (origin if origin.is_dir() else origin.parent).resolve()

    if (current / MANIFEST_FILE_NAME).is_file():
        return current
    raise ManifestNotFoundError(
        f"{MANIFEST_FILE_NAME} was not found in directory {current}."
    )


def as_path(value: Union[Path, SecurePath, str]) -> Path:
    """Normalize a CLI ``--from`` value (``SecurePath`` or ``Path``) to ``Path``."""
    if isinstance(value, SecurePath):
        return value.path
    return Path(value)
