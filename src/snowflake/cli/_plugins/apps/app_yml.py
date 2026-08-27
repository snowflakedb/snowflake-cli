# Copyright (c) 2026 Snowflake Inc.
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

"""``app.yml`` (Snowflake App Runtime ``targets``) parsing.

``app.yml`` is the manifest uploaded to the artifact repository and consumed by
the builder service. At ``version: 2`` it also carries a ``targets`` block
describing one or more deployment *targets* (environments). When an ``app.yml``
with ``version: 2`` is present in the project root, the ``snow app`` commands
read the deployment configuration from it **instead of** ``snowflake.yml``.
This version of the CLI supports ``version: 2`` exactly; a higher version (for
example ``2.1`` or ``3``) is rejected rather than parsed against the v2 schema.

This module models the parts the CLI needs to resolve and deploy a target
(Milestone 1) plus the builder ``install`` / ``build`` / ``run`` / ``dev``
phases, which are optional and default to Node conventions when omitted. Every
root field except those four builder phases can be overridden per target.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional

import yaml
from pydantic import ConfigDict, Field, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema
from snowflake.cli._plugins.apps.snowflake_app_entity_model import (
    APP_SERVICE_COMPUTE_RESOURCE_VALUES,
)
from snowflake.cli.api.config import get_file_io_encoding
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.project.schemas.updatable_model import UpdatableModel
from snowflake.cli.api.secure_path import SecurePath

APP_YML_FILENAME = "app.yml"

# The one ``app.yml`` schema version this CLI owns. Below it (``version: 1`` or
# version-less), ``app.yml`` is the legacy build-only manifest (no ``targets``)
# and is ignored by the CLI, which falls back to ``snowflake.yml``. Above it
# (for example ``2.1`` or ``3``), the manifest is a newer schema this CLI does
# not understand and is rejected rather than parsed against the v2 model.
SUPPORTED_APP_YML_VERSION = 2


class AppYmlSecret(UpdatableModel):
    """A secret binding: an environment variable fed from a Snowflake secret.

    Mirrors the ``secrets`` shape of the inline application-service
    ``SPECIFICATION`` (a list of ``{name, secret}`` entries).
    """

    model_config = ConfigDict(extra="ignore")

    name: str = Field(title="Environment variable name")
    secret: str = Field(title="Snowflake secret object")


class AppYmlEnvVar(UpdatableModel):
    """A plain environment variable made available to the service.

    Mirrors the ``environment_variables`` shape of the inline
    application-service ``SPECIFICATION`` (a list of ``{name, value}`` entries).
    """

    model_config = ConfigDict(extra="ignore")

    name: str = Field(title="Environment variable name")
    value: str = Field(title="Environment variable value")

    @field_validator("value", mode="before")
    @classmethod
    def _coerce_scalar_value(cls, value):
        """Coerce an unquoted scalar to a string, matching the service side.

        The service accepts unquoted scalars (``value: 8080`` / ``value: true``)
        and stores them as the strings ``"8080"`` / ``"true"``. Mirror that so an
        unquoted value in ``app.yml`` parses instead of failing validation;
        booleans use their lowercase YAML/JSON spelling.
        """
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        return value


class AppYmlBuildPhase(UpdatableModel):
    """A builder phase expressed as one or more exec-form commands.

    Used by the ``install`` and ``build`` phases. Each command is an exec-form
    argv list (``["npm", "ci", "--include=dev"]``), so every argument is passed
    literally without shell parsing. These sections are consumed by the builder
    service, not the CLI; the CLI only models them so they can be omitted and
    defaulted (see :class:`AppYmlDefinition`).
    """

    model_config = ConfigDict(extra="ignore")

    commands: List[List[str]] = Field(title="Commands to run for this phase")


class AppYmlRunPhase(UpdatableModel):
    """A builder phase expressed as a single exec-form command.

    Used by the ``run`` and ``dev`` phases; the ``command`` is an exec-form argv
    list. Consumed by the builder service, not the CLI.
    """

    model_config = ConfigDict(extra="ignore")

    command: List[str] = Field(title="Command to run for this phase")


class _AppYmlServiceConfig(UpdatableModel):
    """Deployment-location and service-configuration for one service instance.

    Shared by :class:`AppYmlDefinition` (top-level *baseline*) and
    :class:`AppYmlTarget` (per-target *override*): a deploy overlays the selected
    target's set fields onto the baseline, so any of these may be declared once
    at the top level and selectively overridden per target. This covers the
    service/deployment fields plus the package-build fields (``package_name`` /
    ``artifact_repo`` / ``build_eai`` / ``build_job_location``) and the
    code-storage fields (``code_stage``
    / ``code_workspace`` / ``ignore``). Only the builder ``install`` / ``build``
    / ``run`` / ``dev`` sections are *not* part of this set — they are top-level
    only (optional, with defaults).

    Fields default to ``None`` so an unset field is distinguishable from one
    deliberately set to an empty value, and so a baseline value shows through
    wherever a target leaves the field unset. ``name`` / ``database`` /
    ``schema`` / ``query_warehouse`` locate the service; they may be set at the
    top level and/or per target, but the *resolved* target must define all four
    (there is no connection fallback). That requirement is enforced at resolve
    time (see ``_resolve_app_yml_target``) rather than on the model, so the
    values may come from either scope. ``code_stage`` / ``code_workspace`` are
    mutually exclusive and merge as a pair (see
    :func:`_merge_baseline_and_target`); the rest replace wholesale.
    """

    model_config = ConfigDict(extra="ignore")

    # ── Package build outputs (overridable per target) ────────────────────────
    package_name: Optional[str] = Field(
        title="Artifact repository package name", default=None
    )
    artifact_repo: Optional[str] = Field(
        title="Artifact repository that holds the built package", default=None
    )
    build_eai: Optional[str] = Field(
        title="External access integration used by the builder", default=None
    )
    # Location (``<database>.<schema>``) where the builder service runs the
    # ephemeral build job. When unset, the builder defaults to the current
    # user's personal database (PDB); when set, the value is forwarded to the
    # builder service, which creates the build job in that schema instead. The
    # backend gates and enforces this override (via the
    # ENABLE_APP_BUILDER_CUSTOM_JOB_LOCATION parameter plus the standard
    # privilege checks), so the CLI only passes the value through without
    # validating it.
    build_job_location: Optional[str] = Field(
        title="Location (database.schema) where the builder runs the build job",
        default=None,
    )
    # ── Code storage (where uploaded source lives; overridable per target) ─────
    # ``code_stage`` and ``code_workspace`` name the same thing (where uploaded
    # source lives) and are mutually exclusive. When neither is set the CLI picks
    # a backend based on the destination (a workspace for personal databases,
    # which do not support stages, otherwise a ``<name>_CODE`` stage), matching
    # the ``snowflake.yml`` flow. ``ignore`` lists glob patterns to exclude from
    # the upload of the (always whole) project root.
    code_stage: Optional[str] = Field(
        title="Stage that holds uploaded source", default=None
    )
    code_workspace: Optional[str] = Field(
        title="Workspace that holds uploaded source", default=None
    )
    ignore: Optional[List[str]] = Field(
        title="Glob patterns to exclude from the uploaded source", default=None
    )

    @property
    def bundle_artifacts(self) -> List[PathMapping]:
        """The effective bundle uploaded before the build.

        ``src`` / ``dest`` are fixed to the whole project root (``./*`` → ``./``);
        only the ``ignore`` exclusion list is configurable, so the entire project
        root is always uploaded minus those patterns.
        """
        return [PathMapping(src="./*", dest="./", ignore=self.ignore or [])]

    @model_validator(mode="after")
    def _reject_conflicting_code_storage(self):
        """Reject configuring both code-storage backends at once.

        ``code_stage`` and ``code_workspace`` name the same thing (where uploaded
        source lives), so setting both is rejected — matching the ``snowflake.yml``
        entity model.
        """
        if self.code_stage is not None and self.code_workspace is not None:
            raise ValueError(
                "Set only one of 'code_stage' or 'code_workspace'; they are "
                "mutually exclusive."
            )
        return self

    # ── Deployment location / routing (used by the CLI, not the SPECIFICATION) ──
    name: Optional[str] = Field(title="Application service name", default=None)
    database: Optional[str] = Field(title="Destination database", default=None)
    schema_: Optional[str] = Field(
        title="Destination schema", alias="schema", default=None
    )
    # ``account`` (per-target cross-account binding) is still parsed and drives
    # the mismatch warning, but per-target account binding is not yet supported,
    # so it is intentionally hidden/undocumented: ``SkipJsonSchema`` excludes it
    # from the generated JSON schema so editor completion and docs do not
    # advertise it.
    account: SkipJsonSchema[Optional[str]] = Field(
        title="Account the target deploys to", default=None
    )

    # ── Service configuration (passed through into the inline SPECIFICATION) ──
    query_warehouse: Optional[str] = Field(title="Query warehouse", default=None)
    label: Optional[str] = Field(title="Display label", default=None)
    description: Optional[str] = Field(title="Description", default=None)
    icon: Optional[str] = Field(title="Icon", default=None)
    execute_as_role: Optional[str] = Field(
        title="Role the service executes as", default=None
    )
    auto_resume: Optional[bool] = Field(
        title="Resume the service automatically on demand", default=None
    )
    auto_suspend_secs: Optional[int] = Field(
        title="Idle seconds before the service is suspended", default=None
    )
    min_instances: Optional[int] = Field(
        title="Minimum number of running instances", default=None
    )
    max_instances: Optional[int] = Field(
        title="Maximum number of running instances", default=None
    )
    # Backend for the application service (the write-once ``COMPUTE_RESOURCE`` DDL
    # field). Parsed unconditionally but only applied at deploy time when the
    # ``ENABLE_APP_SERVICE_COMPUTE_RESOURCE`` feature flag is on.
    compute_resource: Optional[str] = Field(
        title="Compute resource backing the service (SERVERLESS or "
        "MANAGED_COMPUTE_POOL)",
        default=None,
    )
    url_prefix: Optional[str] = Field(
        title="URL prefix for the application service", default=None
    )
    health_check: Optional[str] = Field(
        title="Health check endpoint path for the application service", default=None
    )
    external_access_integrations: Optional[List[str]] = Field(
        title="External access integrations active for the service", default=None
    )
    secrets: Optional[List[AppYmlSecret]] = Field(
        title="Environment variable to Snowflake secret bindings", default=None
    )
    environment_variables: Optional[List[AppYmlEnvVar]] = Field(
        title="Plain environment variables for the service", default=None
    )

    @field_validator("compute_resource", mode="before")
    @classmethod
    def _validate_compute_resource(cls, value):
        """Normalise ``compute_resource`` to an accepted upper-case DDL value.

        Accepts ``None``/``"null"`` (unset) and the case-insensitive values in
        :data:`APP_SERVICE_COMPUTE_RESOURCE_VALUES`, matching the
        ``snowflake.yml`` entity model.
        """
        if value is None or value == "null":
            return None
        if not isinstance(value, str):
            raise ValueError("compute_resource must be a string or null")
        normalized = value.strip().upper()
        if normalized not in APP_SERVICE_COMPUTE_RESOURCE_VALUES:
            raise ValueError(
                "compute_resource must be one of: "
                + ", ".join(APP_SERVICE_COMPUTE_RESOURCE_VALUES)
            )
        return normalized


class AppYmlTarget(_AppYmlServiceConfig):
    """A single deployment target (environment) declared under ``targets``.

    Every field is an optional *override* of the top-level baseline (see
    :class:`_AppYmlServiceConfig`); only fields the target actually sets replace
    the baseline value. Extra keys are ignored so a newer ``app.yml`` still
    parses against an older CLI.
    """


class AppYmlDefinition(_AppYmlServiceConfig):
    """The subset of ``app.yml`` the CLI reads to deploy a target.

    Deployment/service fields (inherited from :class:`_AppYmlServiceConfig`) are
    declared here as a *baseline* shared by every target. ``name`` /
    ``database`` / ``schema`` / ``query_warehouse`` locate the service and must
    be present on the *resolved* target — set them at the top level, per target,
    or a mix (there is no connection fallback); the requirement is checked at
    resolve time (see ``_resolve_app_yml_target``). ``targets`` are optional:
    with none declared the baseline is deployed directly; otherwise a command
    must select one with ``--target`` or the top-level ``default_target``, and
    the selected target's set fields override the baseline (see
    :func:`resolve_target`). A target may be an empty mapping (``{}``) to deploy
    the baseline unchanged under that target's name.

    Everything overridable per target is inherited from
    :class:`_AppYmlServiceConfig` (``package_name`` defaults to ``name`` and
    ``artifact_repo`` to ``<name>_repo`` when unset). Only the builder-owned
    ``install`` / ``build`` / ``run`` / ``dev`` sections are top-level only and
    optional, defaulting to Node conventions when omitted.
    """

    model_config = ConfigDict(extra="ignore")

    version: int = Field(title="app.yml schema version")
    # ``name`` / ``database`` / ``schema`` / ``query_warehouse`` are inherited as
    # optional from :class:`_AppYmlServiceConfig`. They may be set at the top
    # level (baseline) and/or per target; the *resolved* target (baseline
    # overlaid with the selected target) must define all four. That is enforced
    # at resolve time (see ``_resolve_app_yml_target``) rather than at parse
    # time, so the values may come from either scope with no connection fallback.
    default_target: Optional[str] = Field(
        title="Name of the target used when --target is omitted", default=None
    )
    targets: Dict[str, AppYmlTarget] = Field(
        title="Named deployment targets", default_factory=dict
    )

    # Builder phases. Consumed by the builder service (not the CLI); modeled here
    # so they can be omitted and defaulted to Node conventions.
    install: AppYmlBuildPhase = Field(
        title="Install phase commands",
        default_factory=lambda: AppYmlBuildPhase(commands=[["npm", "ci"]]),
    )
    build: AppYmlBuildPhase = Field(
        title="Build phase commands",
        default_factory=lambda: AppYmlBuildPhase(commands=[["npm", "run", "build"]]),
    )
    run: AppYmlRunPhase = Field(
        title="Run command",
        default_factory=lambda: AppYmlRunPhase(command=["npm", "start"]),
    )
    dev: AppYmlRunPhase = Field(
        title="Dev command",
        default_factory=lambda: AppYmlRunPhase(command=["npm", "run", "dev"]),
    )

    @field_validator("targets", mode="before")
    @classmethod
    def _coerce_empty_targets(cls, value):
        """Treat a body-less target (``dev:`` / ``dev: {}``) as an empty override.

        A YAML key with no value parses to ``None``; normalise it to an empty
        mapping so such a target deploys the baseline unchanged under its name.
        """
        if isinstance(value, dict):
            return {k: ({} if v is None else v) for k, v in value.items()}
        return value

    @model_validator(mode="before")
    @classmethod
    def _reject_legacy_targets_default(cls, values):
        """Reject the pre-release ``targets.default`` form with clear guidance.

        The default target is now named by a top-level ``default_target`` field;
        the reserved ``default`` key inside ``targets`` is no longer supported.
        Catch it explicitly so a stale manifest gets an actionable message
        instead of a confusing "not a valid target" error.
        """
        if isinstance(values, dict):
            targets = values.get("targets")
            if isinstance(targets, dict) and "default" in targets:
                raise ValueError(
                    "'targets.default' is no longer supported; set a top-level "
                    "'default_target' instead (e.g. 'default_target: dev')."
                )
        return values


def load_app_yml(project_root: Path) -> Optional[AppYmlDefinition]:
    """Load ``app.yml`` from *project_root* if it should drive the CLI.

    Returns the parsed :class:`AppYmlDefinition` only when the file exists and
    declares ``version`` exactly ``2`` (:data:`SUPPORTED_APP_YML_VERSION`), in
    any of its equivalent YAML forms (``2``, ``2.0``, ``"2"``). Returns ``None``
    when the file is absent or is a legacy ``version: 1`` (or version-less)
    build-only manifest, signalling the caller to fall back to ``snowflake.yml``.

    Raises :class:`CliError` when the file exists but cannot be parsed, is
    structurally invalid, or declares a version *above* 2 (for example ``2.1``
    or ``3``): such a manifest targets a newer schema this CLI does not
    understand, so it fails loudly instead of being silently ignored or parsed
    against the v2 model.
    """
    app_yml_path = SecurePath(Path(project_root) / APP_YML_FILENAME)
    if not app_yml_path.is_file():
        return None

    # ``app.yml`` is a CLI-owned project manifest read with the same encoding
    # policy as ``snowflake.yml`` (an explicit cli.encoding.file_io wins,
    # otherwise UTF-8), so a non-ASCII manifest round-trips regardless of the
    # host code page.
    encoding = get_file_io_encoding() or "utf-8"
    try:
        raw = yaml.safe_load(
            app_yml_path.read_text(
                file_size_limit_mb=DEFAULT_SIZE_LIMIT_MB, encoding=encoding
            )
        )
    except yaml.YAMLError as exc:
        raise CliError(f"Could not parse {APP_YML_FILENAME}: {exc}") from exc

    if not isinstance(raw, dict):
        raise CliError(f"{APP_YML_FILENAME} must be a mapping at the top level.")

    version = _parse_app_yml_version(raw.get("version"))
    if version is None or version < SUPPORTED_APP_YML_VERSION:
        # Absent, non-numeric, or a legacy (v1) build-only manifest: not owned
        # by the CLI — fall back to ``snowflake.yml``.
        return None
    if version != SUPPORTED_APP_YML_VERSION:
        # A newer schema (for example ``2.1`` or ``3``) this CLI does not
        # understand. Fail loudly rather than parse it against the v2 model.
        raise CliError(
            f"Unsupported {APP_YML_FILENAME} version {raw.get('version')!r}: "
            f"this version of Snowflake CLI supports {APP_YML_FILENAME} "
            f"version {SUPPORTED_APP_YML_VERSION} only."
        )

    # Normalize ``version`` to the supported int so the model does not have to
    # re-interpret a float/string form.
    raw = {**raw, "version": SUPPORTED_APP_YML_VERSION}
    try:
        return AppYmlDefinition(**raw)
    except Exception as exc:
        raise CliError(f"Invalid {APP_YML_FILENAME}: {exc}") from exc


def _parse_app_yml_version(value: object) -> Optional[float]:
    """Interpret the ``version`` field leniently as a number.

    YAML may type ``version`` as an int (``2``), a float (``2.0`` / ``2.1``), or
    a string (``"2"`` / ``"2.1"``). Return the numeric value so the caller can
    distinguish an exact ``2`` from a lower legacy version (fall back) or a
    higher unsupported one (error). Returns ``None`` for a missing or
    non-numeric value, which the caller treats as a fall-back to
    ``snowflake.yml`` — not an error — so an unrelated manifest never blocks the
    legacy flow.
    """
    # ``bool`` is an ``int`` subclass but is never a valid version.
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.strip())
        except ValueError:
            return None
    return None


# ``code_stage`` and ``code_workspace`` are mutually exclusive, so they merge as
# a pair rather than field-by-field: a target that names *either* replaces the
# baseline backend entirely. Kept out of the generic overlay in
# :func:`_merge_baseline_and_target` so an override never combines with the
# other baseline backend into an invalid pair.
_CODE_STORAGE_FIELDS = ("code_stage", "code_workspace")


def _merge_baseline_and_target(
    app_def: AppYmlDefinition, target: Optional[AppYmlTarget]
) -> AppYmlTarget:
    """Overlay a target's set fields onto the top-level baseline.

    A field the target actually sets (non-``None``) overrides the baseline;
    otherwise the baseline value shows through. Scalar and list fields replace
    wholesale (lists are not concatenated). ``code_stage`` / ``code_workspace``
    merge as a mutually-exclusive pair: a target naming either backend replaces
    both, so an override never combines with the baseline's other backend.
    """
    fields = [
        name
        for name in _AppYmlServiceConfig.model_fields
        if name not in _CODE_STORAGE_FIELDS
    ]
    merged = {name: getattr(app_def, name) for name in fields}
    if target is not None:
        merged.update(
            {
                name: value
                for name in fields
                if (value := getattr(target, name)) is not None
            }
        )
    if target is not None and (
        target.code_stage is not None or target.code_workspace is not None
    ):
        merged["code_stage"] = target.code_stage
        merged["code_workspace"] = target.code_workspace
    else:
        merged["code_stage"] = app_def.code_stage
        merged["code_workspace"] = app_def.code_workspace
    return AppYmlTarget().model_copy(update=merged)


def resolve_target(
    app_def: AppYmlDefinition, target_name: Optional[str]
) -> tuple[Optional[str], AppYmlTarget]:
    """Resolve the service configuration a command should operate on.

    ``targets`` are optional. With none declared, the top-level baseline is used
    directly and the returned name is ``None``. Once any target is declared a
    target must be selected explicitly — an explicit ``--target`` wins, then the
    top-level ``default_target`` — and its set fields override the baseline. A
    target may be an empty mapping (``{}``), which deploys the baseline unchanged
    under that name. :class:`CliError` is raised when no target is selected, or
    when the selected target is not defined.
    """
    if not app_def.targets:
        if target_name:
            raise CliError(
                f"Target '{target_name}' is not defined in {APP_YML_FILENAME}: "
                "no targets are declared."
            )
        return None, _merge_baseline_and_target(app_def, None)

    resolved_name = target_name or app_def.default_target
    available = ", ".join(sorted(app_def.targets)) or "(none)"
    if not resolved_name:
        raise CliError(
            f"No target selected. Pass --target or set 'default_target' in "
            f"{APP_YML_FILENAME}. Available targets: {available}."
        )

    target = app_def.targets.get(resolved_name)
    if target is None:
        raise CliError(
            f"Target '{resolved_name}' is not defined in {APP_YML_FILENAME}. "
            f"Available targets: {available}."
        )
    return resolved_name, _merge_baseline_and_target(app_def, target)
