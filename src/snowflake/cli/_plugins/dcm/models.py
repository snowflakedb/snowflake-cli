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
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import yaml
from snowflake.cli._plugins.dcm.exceptions import (
    InvalidManifestError,
    ManifestConfigurationError,
    ManifestNotFoundError,
)
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import to_identifier
from snowflake.cli.api.secure_path import SecurePath

MANIFEST_FILE_NAME = "manifest.yml"
DCM_PROJECT_TYPE = "dcm_project"
SOURCES_FOLDER = "sources"
SUPPORTED_MANIFEST_VERSION = 2
log = logging.getLogger(__name__)

# POSIX portable character set for env var names: letters, digits, and
# underscores only, not starting with a digit. The server enforces the
# same rule.
_ENV_VAR_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
# _snow is the Jinja context object injected into every template render
# (`_snow.env_var()` / `_snow.env_secret()`) -- declaring an env var with
# this exact name would shadow it in the render namespace.
_RESERVED_ENV_VAR_NAME = "_snow"


def _section_label(section_name: str) -> str:
    """Error-message-friendly identifier for a templating section, e.g.
    "manifest.yml's 'templating.env_vars' section" -- so a bare 'env_vars'
    in an error isn't mistaken for something other than a manifest.yml
    section (a shell variable, a CLI flag, etc.)."""
    return f"{MANIFEST_FILE_NAME}'s 'templating.{section_name}' section"


_ASSET_NAME_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_MAX_ASSET_NAME_LENGTH = 255
_MAX_ASSET_PATH_LENGTH = 1024


def validate_asset_name(name: Any) -> None:
    """Validate a manifest asset name against the ``assets:`` spec."""
    if not name or not isinstance(name, str):
        raise InvalidManifestError("Manifest asset is missing a non-empty 'name'.")
    if len(name) > _MAX_ASSET_NAME_LENGTH:
        raise InvalidManifestError(
            f"Asset name exceeds {_MAX_ASSET_NAME_LENGTH} characters."
        )
    if not _ASSET_NAME_PATTERN.match(name):
        raise InvalidManifestError(
            f"Asset name '{name}' is invalid; must match {_ASSET_NAME_PATTERN.pattern}."
        )


def validate_asset_path(entry: Any) -> None:
    """Validate a single ``path``/``paths`` entry stays repo-relative and in-root."""
    if not entry or not isinstance(entry, str):
        raise InvalidManifestError("Asset path entries must be non-empty strings.")
    if len(entry) > _MAX_ASSET_PATH_LENGTH:
        raise InvalidManifestError(
            f"Asset path exceeds {_MAX_ASSET_PATH_LENGTH} characters: {entry}"
        )
    if entry.startswith("/") or entry.startswith("\\"):
        raise InvalidManifestError(
            f"Asset path must be relative to the project root (no leading '/'): {entry}"
        )
    # Only a stage prefix ('@stage/...') or a URL scheme is disallowed -- an '@'
    # elsewhere is a legitimate filename character (e.g. 'img/logo@2x.png').
    if entry.startswith("@") or "://" in entry:
        raise InvalidManifestError(
            f"Asset path must be a repo-relative path (no leading '@' or '://'): {entry}"
        )
    # Split on both separators so a '.'/'..' segment can't slip through in a
    # backslash-separated path (e.g. '..\\secret') on Windows.
    segments = re.split(r"[\\/]", entry)
    if ".." in segments:
        raise InvalidManifestError(
            f"Asset path must stay within the project root (no '..'): {entry}"
        )
    # A '.' segment ('.', './', 'a/./b') reaches Path.glob unfiltered and crashes
    # with a version-dependent exception; reject it here. Use '**/*' for "everything".
    if "." in segments:
        raise InvalidManifestError(
            f"Asset path must not contain a '.' segment (use '**/*' for the whole project): {entry}"
        )
    if entry.endswith("/") or entry.endswith("\\"):
        raise InvalidManifestError(
            f"Asset path must not end with a path separator: {entry}"
        )


def validate_glob_pattern(entry: str) -> None:
    """Validate the glob metacharacters in a ``path``/``paths`` entry.

    Only ``*`` and ``**`` are wildcards. ``?``, ``{`` and ``}`` are reserved and
    rejected; ``[`` and ``]`` are matched literally (so they are allowed here).
    ``**`` is valid only as a full path segment followed by another component
    (``**/*``, ``dir/**/*``) -- a bare, trailing, or embedded ``**`` is rejected.
    """
    for char in ("?", "{", "}"):
        if char in entry:
            raise InvalidManifestError(
                f"Glob metacharacter '{char}' is not supported: {entry}"
            )
    segments = entry.split("/")
    for index, segment in enumerate(segments):
        if "**" not in segment:
            continue
        if segment != "**":
            raise InvalidManifestError(
                f"'**' must be a full path segment (write '**/*'), not '{segment}': {entry}"
            )
        if index == len(segments) - 1:
            raise InvalidManifestError(
                f"'**' must be followed by another component (write '**/*'): {entry}"
            )


@dataclass
class DCMTemplating:
    """Templating configuration for DCM manifest v2."""

    defaults: Dict[str, Any] = field(default_factory=dict)
    configurations: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    env_vars: List[str] = field(default_factory=list)
    env_secrets: List[str] = field(default_factory=list)

    @staticmethod
    def _declared_names(raw_entries: List[Any], section_name: str) -> List[str]:
        """Each entry is a single-key mapping (e.g. `- BUILD_NUMBER:` in the
        manifest) -- the key is the declared name; the value is a reserved,
        currently-empty placeholder for future per-variable properties."""
        names: List[str] = []
        for entry in raw_entries:
            if not isinstance(entry, dict):
                raise InvalidManifestError(
                    f"Entry {entry!r} in {_section_label(section_name)} must be "
                    f"a single-key mapping (e.g. `- BUILD_NUMBER:`)."
                )
            if len(entry) != 1:
                raise InvalidManifestError(
                    f"Entry {entry!r} in {_section_label(section_name)} must "
                    f"declare exactly one name (e.g. `- BUILD_NUMBER:`), not "
                    f"{len(entry)}."
                )
            for name in entry.keys():
                if not isinstance(name, str):
                    raise InvalidManifestError(
                        f"Variable name {name!r} in {_section_label(section_name)} "
                        f"must be a string."
                    )
                if name == _RESERVED_ENV_VAR_NAME:
                    raise InvalidManifestError(
                        f"'{_RESERVED_ENV_VAR_NAME}' in {_section_label(section_name)} "
                        f"is a reserved name and cannot be declared."
                    )
                if not _ENV_VAR_NAME_PATTERN.match(name):
                    raise InvalidManifestError(
                        f"Variable name '{name}' in {_section_label(section_name)} "
                        f"is not a valid environment variable name. Must follow "
                        f"the POSIX portable character set: letters, digits, and "
                        f"underscores only, and must not start with a digit."
                    )
            names.extend(entry.keys())
        return names

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]]) -> "DCMTemplating":
        if not data:
            return cls()
        configurations = data.get("configurations", {})
        env_vars = cls._declared_names(data.get("env_vars") or [], "env_vars")
        env_secrets = cls._declared_names(data.get("env_secrets") or [], "env_secrets")
        for section, names in (("env_vars", env_vars), ("env_secrets", env_secrets)):
            duplicates = {name for name in names if names.count(name) > 1}
            if duplicates:
                raise InvalidManifestError(
                    f"Duplicate name(s) in {_section_label(section)}: "
                    f"{sorted(duplicates)}."
                )
        overlap = set(env_vars) & set(env_secrets)
        if overlap:
            raise InvalidManifestError(
                f"Name(s) declared in both 'templating.env_vars' and "
                f"'templating.env_secrets' sections of {MANIFEST_FILE_NAME}: "
                f"{sorted(overlap)}."
            )
        return cls(
            defaults=data.get("defaults", {}),
            configurations={k.upper(): v for k, v in configurations.items()},
            env_vars=env_vars,
            env_secrets=env_secrets,
        )

    @property
    def declared_variable_names(self) -> Set[str]:
        """Names declared in either env_vars or env_secrets.

        The CLI does not distinguish secret vs. non-secret declarations —
        that distinction is only meaningful server-side.
        """
        return set(self.env_vars) | set(self.env_secrets)


@dataclass
class DCMTarget:
    """Target configuration for DCM manifest v2."""

    name: str
    project_name: str
    account_identifier: str
    project_owner: str
    templating_config: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DCMTarget":
        templating_config = data.get("templating_config")
        account_identifier = data.get("account_identifier", "")
        project_owner = data.get("project_owner", "")
        return cls(
            name=data.get("name", "").upper(),
            project_name=data.get("project_name", ""),
            account_identifier=account_identifier if account_identifier else "",
            project_owner=to_identifier(project_owner) if project_owner else "",
            templating_config=templating_config.upper() if templating_config else None,
        )


@dataclass
class DCMAsset:
    """A named collection of files declared in the DCM manifest v2 ``assets:``.

    Each entry declares exactly one of ``path`` (a single file/dir/glob) or
    ``paths`` (a list of them); both are normalized here to ``paths``. Entries
    are repo-relative to the project root (the directory containing
    ``manifest.yml``). This layer only models and validates the declared
    configuration -- resolving globs against the filesystem happens later,
    during upload.
    """

    name: str
    paths: List[str] = field(default_factory=list)

    @classmethod
    def from_entry(cls, name: Any, spec: Any) -> "DCMAsset":
        """Build an asset from one ``assets:`` mapping entry.

        ``name`` is the mapping key; ``spec`` is its value, a mapping declaring
        exactly one of ``path`` or ``paths``.
        """
        validate_asset_name(name)

        if not isinstance(spec, dict):
            raise InvalidManifestError(
                f"Asset '{name}' must be a mapping declaring 'path' or 'paths'."
            )

        unknown_keys = set(spec) - {"path", "paths"}
        if unknown_keys:
            # repr() every key: YAML keys need not be strings, and a raw
            # sorted()/join() over mixed types would raise TypeError instead of
            # the InvalidManifestError the user should see (also quotes names).
            raise InvalidManifestError(
                f"Unknown key(s) in asset '{name}': {', '.join(sorted(map(repr, unknown_keys)))}."
            )

        has_path = "path" in spec
        has_paths = "paths" in spec
        if has_path == has_paths:
            raise InvalidManifestError(
                f"Asset '{name}' must declare exactly one of 'path' or 'paths'."
            )

        if has_path:
            entries: List[Any] = [spec["path"]]
        else:
            entries = spec["paths"]
            if not isinstance(entries, list) or not entries:
                raise InvalidManifestError(
                    f"Asset '{name}' field 'paths' must be a non-empty list."
                )

        for entry in entries:
            validate_asset_path(entry)
            validate_glob_pattern(entry)

        return cls(name=name, paths=list(entries))


@dataclass
class DCMManifest:
    """DCM manifest v2 structure."""

    manifest_version: int
    project_type: str
    default_target: Optional[str] = None
    targets: Dict[str, DCMTarget] = field(default_factory=dict)
    templating: DCMTemplating = field(default_factory=DCMTemplating)
    assets: Dict[str, DCMAsset] = field(default_factory=dict)

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

        raw_assets = data.get("assets")
        if raw_assets is None:
            assets: Dict[str, DCMAsset] = {}
        elif not isinstance(raw_assets, dict):
            raise InvalidManifestError(
                "Manifest 'assets' must be a mapping of asset name to its path(s)."
            )
        else:
            # Name-keyed like `targets`; dict insertion order preserves
            # declaration order and gives O(1) lookup by asset name.
            assets = {
                name: DCMAsset.from_entry(name, spec)
                for name, spec in raw_assets.items()
            }

        manifest = cls(
            manifest_version=manifest_version,
            project_type=data.get("type", "").lower(),
            default_target=default_target.upper()
            if isinstance(default_target, str)
            else None,
            targets=targets,
            templating=DCMTemplating.from_dict(data.get("templating")),
            assets=assets,
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

    def _validate_target_required_fields(self, target: DCMTarget):
        if not target.project_name:
            raise ManifestConfigurationError(
                f"Target '{target.name}' is missing required field(s): project_name."
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
        self._validate_target_required_fields(target)
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
    declared_variable_names: Set[str] = field(default_factory=set)
    assets: List[DCMAsset] = field(default_factory=list)
