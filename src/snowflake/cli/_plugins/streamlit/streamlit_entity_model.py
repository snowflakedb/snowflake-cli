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

import re
from typing import Any, List, Literal, Optional

from pydantic import Field, field_validator, model_validator
from snowflake.cli._plugins.object.common import Tag
from snowflake.cli.api.project.schemas.entities.common import (
    Artifacts,
    EntityModelBaseWithArtifacts,
    ExternalAccessBaseModel,
    GrantBaseModel,
    ImportsBaseModel,
    reject_backslashes,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
    UpdatableModel,
)

# SPCS Runtime v2 constants
SPCS_RUNTIME_V2_NAME = "SYSTEM$ST_CONTAINER_RUNTIME_PY3_11"
SPCS_CONTAINER_RUNTIME_PREFIX = "SYSTEM$ST_CONTAINER_RUNTIME"
# The family suffix is limited to what a runtime name uses (``_PY3_12``). A bare
# prefix match also accepted a trailing backslash, which hides the closing quote
# of the RUNTIME_NAME literal from the connector's statement splitter.
_SPCS_CONTAINER_RUNTIME_NAME = re.compile(
    rf"{re.escape(SPCS_CONTAINER_RUNTIME_PREFIX)}[A-Z0-9_]*"
)
WAREHOUSE_RUNTIME_NAME = "SYSTEM$WAREHOUSE_RUNTIME"

# Named runtimes this CLI documents. A typo is still rejected rather than
# dropped from the DDL. The container-runtime *family* (the prefix plus
# ``[A-Z0-9_]*``, see :func:`is_known_runtime_name`) is accepted so a newer
# ``..._PY3_12`` can deploy, restart, and stream logs without a CLI upgrade.
KNOWN_RUNTIME_NAMES = frozenset({SPCS_RUNTIME_V2_NAME, WAREHOUSE_RUNTIME_NAME})


def normalize_runtime_name(runtime_name: Optional[str]) -> str:
    """Upper-case and trim a runtime_name so it can be matched against the constants above.

    Snowflake runtime identifiers are case-insensitive, so matching is too. A
    recognized value is then stored in canonical form by
    :meth:`StreamlitEntityModel.canonicalize_runtime_name`, so the rest of the
    CLI never has to repeat this normalization.
    """
    return (runtime_name or "").strip().upper()


def is_spcs_container_runtime(runtime_name: Optional[str]) -> bool:
    """True when ``runtime_name`` is any SPCS container runtime (any Python version)."""
    return bool(
        _SPCS_CONTAINER_RUNTIME_NAME.fullmatch(normalize_runtime_name(runtime_name))
    )


def is_known_runtime_name(runtime_name: Optional[str]) -> bool:
    """True when the CLI will emit this runtime rather than reject it as unknown."""
    normalized = normalize_runtime_name(runtime_name)
    return normalized in KNOWN_RUNTIME_NAMES or is_spcs_container_runtime(normalized)


# The maximum number of tasks in SCHEDULED_TASKS.
MAX_SCHEDULES = 5


class Schedules(UpdatableModel):
    cron_exprs: List[str] = Field(
        title=f"Cron expressions that start the app. Maximum {MAX_SCHEDULES}",
        max_length=MAX_SCHEDULES,
    )
    timezone: Optional[str] = Field(
        title="Timezone of the cron expressions. The default is UTC",
        default=None,
    )

    @field_validator("cron_exprs", mode="after")
    @classmethod
    def strip_cron_exprs(cls, cron_exprs: List[str]) -> List[str]:
        """Snowflake checks the cron. This only stops a value that breaks the SQL."""
        stripped = [expr.strip() for expr in cron_exprs]
        if any(not expr for expr in stripped):
            raise ValueError("cron_exprs must not contain an empty expression")
        if any(ch in expr for expr in stripped for ch in "\r\n'\\"):
            raise ValueError(
                "cron_exprs must not contain a line break, quote, or backslash"
            )
        return stripped

    @field_validator("timezone", mode="after")
    @classmethod
    def strip_timezone(cls, timezone: Optional[str]) -> Optional[str]:
        if timezone is None:
            return None
        stripped = timezone.strip()
        if not stripped or any(ch.isspace() or ch in "'\\" for ch in stripped):
            raise ValueError(
                "timezone must be one IANA name, such as UTC or America/Los_Angeles"
            )
        return stripped


class StreamlitEntityModel(
    EntityModelBaseWithArtifacts,
    ExternalAccessBaseModel,
    ImportsBaseModel,
    GrantBaseModel,
):
    type: Literal["streamlit"] = DiscriminatorField()  # noqa: A003
    title: Optional[str] = Field(
        title="Human-readable title for the Streamlit dashboard", default=None
    )
    comment: Optional[str] = Field(title="Comment for the Streamlit app", default=None)
    query_warehouse: str = Field(
        title="Snowflake warehouse to host the app", default=None
    )
    main_file: Optional[str] = Field(
        title="Entrypoint file of the Streamlit app", default="streamlit_app.py"
    )
    pages_dir: Optional[str] = Field(title="Streamlit pages", default=None)
    stage: Optional[str] = Field(
        title="Stage in which the app’s artifacts will be stored", default="streamlit"
    )
    # Artifacts were optional, so to avoid BCR, we need to make them optional here as well
    artifacts: Optional[Artifacts] = Field(
        title="List of paths or file source/destination pairs to add to the deploy root",
        default=None,
    )
    runtime_name: Optional[str] = Field(
        title=(
            "The runtime name to run the streamlit app on. One of "
            f"{SPCS_RUNTIME_V2_NAME} (requires compute_pool) or {WAREHOUSE_RUNTIME_NAME}"
        ),
        default=None,
    )
    compute_pool: Optional[str] = Field(
        title="The compute pool name of the snowservices running the streamlit app",
        default=None,
    )
    tags: Optional[List[Tag]] = Field(title="Tags for the Streamlit app", default=None)
    include_project_definition: bool = Field(
        title=(
            "When true, include the project-root snowflake.yml in deploy artifacts "
            "if it is not already covered by an existing artifact mapping"
        ),
        default=False,
    )
    execute_as: Optional[Literal["OWNER", "RESTRICTED CALLER"]] = Field(
        title=(
            "The rights that the app's queries use: OWNER or RESTRICTED CALLER. "
            f"Only for {WAREHOUSE_RUNTIME_NAME}"
        ),
        default=None,
    )
    app_warehouse: Optional[str] = Field(
        title=(
            "Warehouse that runs the app code. It is not query_warehouse. "
            f"Only for {WAREHOUSE_RUNTIME_NAME}"
        ),
        default=None,
    )
    artifact_repositories: Optional[List[str]] = Field(
        title=(
            "Repositories that the app gets packages from. Only for "
            f"{SPCS_CONTAINER_RUNTIME_PREFIX}* runtimes"
        ),
        default=None,
    )
    idle_auto_shutdown_time_seconds: Optional[int] = Field(
        title="Seconds without viewers, before the app stops",
        default=None,
        gt=0,
    )
    schedules: Optional[Schedules] = Field(
        title="Schedules that start the app before users need it",
        default=None,
    )

    @field_validator("execute_as", mode="before")
    @classmethod
    def canonicalize_execute_as(cls, execute_as: Any) -> Any:
        """The value goes into the SQL as keywords. The ``Literal`` type stops other text."""
        if not isinstance(execute_as, str):
            return execute_as
        return " ".join(execute_as.split()).upper()

    @field_validator("app_warehouse", mode="before")
    @classmethod
    def strip_app_warehouse(cls, app_warehouse: Any) -> Any:
        if not isinstance(app_warehouse, str):
            return app_warehouse
        stripped = app_warehouse.strip()
        if not stripped:
            raise ValueError("app_warehouse must not be empty")
        reject_backslashes("app_warehouse", [stripped])
        return stripped

    @field_validator("artifact_repositories", mode="after")
    @classmethod
    def strip_artifact_repositories(
        cls, repositories: Optional[List[str]]
    ) -> Optional[List[str]]:
        if repositories is None:
            return None
        stripped = [repository.strip() for repository in repositories]
        if any(not repository for repository in stripped):
            raise ValueError("artifact_repositories must not contain an empty name")
        reject_backslashes("artifact_repositories", stripped)
        return stripped

    @field_validator("runtime_name", mode="before")
    @classmethod
    def canonicalize_runtime_name(cls, runtime_name: Any) -> Any:
        """Canonicalize a recognized runtime_name, and reject anything else.

        Matching tolerates casing and surrounding whitespace, but everything
        downstream (the emitted DDL, the ALTER property diff, log streaming)
        then sees one exact spelling instead of whatever the project file
        happened to contain. Without this, a padded value would match the
        allowlist and still reach the DDL with its padding attached.

        Both the canonicalization and the allowlist check belong here rather than
        in the ``mode="after"`` model validator, for two reasons. They concern a
        single field, and a field validator inherits UpdatableModel's
        template-skip wrap: a templated value such as
        ``<% ctx.env.RUNTIME_NAME %>`` is not a runtime name yet and has to
        survive the pre-render pass untouched. A model validator runs outside any
        field's validator chain and so never gets that protection.
        """
        if runtime_name is None or not isinstance(runtime_name, str):
            return runtime_name
        reject_backslashes("runtime_name", [runtime_name])
        normalized = normalize_runtime_name(runtime_name)
        if is_known_runtime_name(normalized):
            return normalized
        # Report the value as the user wrote it, not the normalized form.
        supported = ", ".join(sorted(KNOWN_RUNTIME_NAMES))
        raise ValueError(
            f"Unknown runtime_name '{runtime_name}'. Supported values are: {supported} "
            f"(or {SPCS_CONTAINER_RUNTIME_PREFIX} followed by letters, digits, or '_')"
        )

    @field_validator("compute_pool", mode="before")
    @classmethod
    def strip_compute_pool(cls, compute_pool: Any) -> Any:
        """Trim a compute_pool, and reject one that is present but blank.

        A pool name is an identifier, so surrounding whitespace is never
        meaningful. Trimming also stops a whitespace-only value from being
        truthy, which would otherwise emit ``COMPUTE_POOL = '   '`` and, because
        a pool implies the container runtime, infer a runtime_name from nothing.

        Like the runtime_name validator, this is a single-field rule kept in a
        field validator so it inherits the template-skip wrap.
        """
        if not isinstance(compute_pool, str):
            return compute_pool
        stripped = compute_pool.strip()
        if not stripped:
            # Reachable only from an explicitly blank value, since an omitted key
            # is None. Reported rather than treated as absent, for the same reason
            # a blank runtime_name is: the key being present expresses an intent
            # that silently dropping it would defeat.
            raise ValueError("compute_pool must not be empty")
        reject_backslashes("compute_pool", [stripped])
        return stripped

    @model_validator(mode="after")
    def validate_spcs_runtime_fields(self):
        """Infer runtime_name from a compute_pool given without one.

        Only cross-field rules belong here. Single-field rules live in the field
        validators above, where UpdatableModel's template-skip wrap reaches them.
        """
        if self.compute_pool and not self.runtime_name:
            # A compute pool is only meaningful to the container runtime, so take the
            # pool as the request it implies and name the runtime explicitly in the
            # DDL rather than emitting COMPUTE_POOL against whatever runtime the
            # account happens to default to.
            #
            # Assigning here re-enters validation (validate_assignment=True). It
            # terminates because runtime_name is set on re-entry, so this branch is
            # not taken again.
            self.runtime_name = SPCS_RUNTIME_V2_NAME

        # No runtime/compute_pool pairing rules. Snowflake supplies a default compute
        # pool when the container runtime is named without one, and ignores a pool
        # given alongside the warehouse runtime, so neither combination is an error.
        # Requiring a pool for the container runtime used to reject a valid config,
        # and rejecting one for the warehouse runtime would block the natural
        # container-to-warehouse migration where only runtime_name changes. Whether
        # COMPUTE_POOL is emitted is decided at the emission site instead.
        return self
