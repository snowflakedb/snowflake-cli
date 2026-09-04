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

from typing import Any, List, Literal, Optional

from pydantic import Field, field_validator, model_validator
from snowflake.cli._plugins.object.common import Tag
from snowflake.cli.api.project.schemas.entities.common import (
    Artifacts,
    EntityModelBaseWithArtifacts,
    ExternalAccessBaseModel,
    GrantBaseModel,
    ImportsBaseModel,
)
from snowflake.cli.api.project.schemas.updatable_model import DiscriminatorField

# SPCS Runtime v2 constants
SPCS_RUNTIME_V2_NAME = "SYSTEM$ST_CONTAINER_RUNTIME_PY3_11"
WAREHOUSE_RUNTIME_NAME = "SYSTEM$WAREHOUSE_RUNTIME"

# Runtimes this CLI knows how to deploy. An unrecognized runtime_name is rejected
# rather than dropped from the DDL, so that a typo, or a runtime newer than this
# CLI, cannot silently deploy the app onto a different runtime than was asked for.
#
# This is deliberately fail-closed, and the cost is that a runtime released after
# a given CLI version is unusable until that CLI is upgraded. Supporting a new
# runtime means adding its constant to this set; prefer that over loosening the
# check, so the failure stays a clear local error rather than a server-side one.
KNOWN_RUNTIME_NAMES = frozenset({SPCS_RUNTIME_V2_NAME, WAREHOUSE_RUNTIME_NAME})


def normalize_runtime_name(runtime_name: Optional[str]) -> str:
    """Upper-case and trim a runtime_name so it can be matched against the constants above.

    Snowflake runtime identifiers are case-insensitive, so matching is too. A
    recognized value is then stored in canonical form by
    :meth:`StreamlitEntityModel.canonicalize_runtime_name`, so the rest of the
    CLI never has to repeat this normalization.
    """
    return (runtime_name or "").strip().upper()


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
        normalized = normalize_runtime_name(runtime_name)
        if normalized in KNOWN_RUNTIME_NAMES:
            return normalized
        # Report the value as the user wrote it, not the normalized form.
        supported = ", ".join(sorted(KNOWN_RUNTIME_NAMES))
        raise ValueError(
            f"Unknown runtime_name '{runtime_name}'. Supported values are: {supported}"
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
