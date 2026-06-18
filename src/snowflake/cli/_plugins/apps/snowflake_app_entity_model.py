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

from typing import Literal, Optional, Union

from pydantic import Field, field_validator, model_validator
from pydantic.json_schema import SkipJsonSchema
from snowflake.cli.api.project.schemas.entities.common import (
    EntityModelBaseWithArtifacts,
    MetaField,
)
from snowflake.cli.api.project.schemas.updatable_model import (
    DiscriminatorField,
    IdentifierField,
    UpdatableModel,
)


class ComputePoolReference(UpdatableModel):
    """Reference to a compute pool."""

    name: Optional[str] = IdentifierField(
        title="Name of the compute pool", default=None
    )
    schema_: Optional[str] = IdentifierField(
        title="Schema of the compute pool", alias="schema", default=None
    )
    database: Optional[str] = IdentifierField(
        title="Database of the compute pool", default=None
    )


class ExternalAccessReference(UpdatableModel):
    """Reference to an external access integration.

    External access integrations are account-level objects, so only a name is
    accepted (no database/schema qualification).
    """

    name: Optional[str] = IdentifierField(
        title="Name of the external access integration", default=None
    )


class ArtifactRepositoryReference(UpdatableModel):
    """Reference to an artifact repository."""

    name: str = IdentifierField(title="Name of the artifact repository")
    schema_: Optional[str] = IdentifierField(
        title="Schema of the artifact repository", alias="schema", default=None
    )
    database: Optional[str] = IdentifierField(
        title="Database of the artifact repository", default=None
    )


class CodeStageReference(UpdatableModel):
    """Reference to a code stage.

    Supports both a fully-qualified identifier form (``DB.SCHEMA.STAGE``)
    and a bare-name form (``STAGE``).  When only a name is provided the
    app's database and schema are used implicitly at deploy time.
    """

    name: str = IdentifierField(title="Name of the code stage")
    schema_: Optional[str] = IdentifierField(
        title="Schema of the code stage", alias="schema", default=None
    )
    database: Optional[str] = IdentifierField(
        title="Database of the code stage", default=None
    )
    encryption_type: Optional[str] = Field(
        title="Encryption type for the stage", default="SNOWFLAKE_SSE"
    )


class CodeWorkspaceReference(UpdatableModel):
    """Reference to a code workspace."""

    name: str = IdentifierField(title="Name of the code workspace")
    schema_: Optional[str] = IdentifierField(
        title="Schema of the code workspace", alias="schema", default=None
    )
    database: Optional[str] = IdentifierField(
        title="Database of the code workspace", default=None
    )


class SnowflakeAppMetaField(MetaField):
    """Extended meta field for Snowflake App Runtime with title, description, icon."""

    title: Optional[str] = Field(
        title="Title of the Snowflake App Runtime", default=None
    )
    description: Optional[str] = Field(
        title="Description of the Snowflake App Runtime", default=None
    )
    icon: Optional[str] = Field(
        title="Icon for the Snowflake App Runtime", default=None
    )


class SnowflakeAppEntityModel(EntityModelBaseWithArtifacts):
    """Entity model for Snowflake App Runtime (snowflake-app) type."""

    type: Literal["snowflake-app"] = DiscriminatorField()  # noqa: A003

    meta: Optional[SnowflakeAppMetaField] = Field(title="Meta fields", default=None)

    query_warehouse: Optional[str] = IdentifierField(
        title="Warehouse to use for queries", default=None
    )

    # ``build_compute_pool`` and ``service_compute_pool`` remain fully
    # functional (still parsed from ``snowflake.yml`` and forwarded to the
    # server when present), but are intentionally hidden/undocumented:
    # ``SkipJsonSchema`` excludes them from the generated project-definition
    # JSON schema so editor completion and docs do not advertise them.
    build_compute_pool: SkipJsonSchema[Union[ComputePoolReference, None]] = Field(
        title="Compute pool for building the app", default=None
    )

    service_compute_pool: SkipJsonSchema[Union[ComputePoolReference, None]] = Field(
        title="Compute pool for running the app service", default=None
    )

    @field_validator("build_compute_pool", "service_compute_pool", mode="before")
    @classmethod
    def _validate_compute_pool(cls, value):
        """Allow null/None values for compute pool fields."""
        if value is None or value == "null":
            return None
        return value

    build_eai: Union[ExternalAccessReference, None] = Field(
        title="External access integration for build", default=None
    )

    @field_validator("build_eai", mode="before")
    @classmethod
    def _validate_eai(cls, value):
        """Accept a bare name string, a mapping with ``name``, or null/None.

        External access integrations are account-level objects, so a plain
        string is treated as the integration name (e.g. ``build_eai: MY_EAI``).
        """
        if value is None or value == "null":
            return None
        if isinstance(value, str):
            return {"name": value}
        return value

    artifact_repository: Optional[ArtifactRepositoryReference] = Field(
        title="Artifact repository for the app", default=None
    )

    code_stage: Optional[CodeStageReference] = Field(
        title="Stage for storing code artifacts", default=None
    )

    code_workspace: Optional[CodeWorkspaceReference] = Field(
        title="Workspace for storing code artifacts", default=None
    )

    @field_validator("code_stage", "code_workspace", mode="before")
    @classmethod
    def _validate_code_storage(cls, value):
        """Accept either a dict, a plain name, or a ``DB.SCHEMA.NAME`` identifier.

        When a string is provided it is parsed as an FQN.  Any missing
        database/schema components are left as ``None`` and resolved to the
        app's database/schema at deploy time.
        """
        if value is None or value == "null":
            return None
        if isinstance(value, str):
            from snowflake.cli.api.identifiers import FQN

            fqn = FQN.from_string(value)
            parsed: dict = {"name": fqn.name}
            if fqn.database:
                parsed["database"] = fqn.database
            if fqn.schema:
                parsed["schema"] = fqn.schema
            return parsed
        return value

    @model_validator(mode="after")
    def _validate_single_code_storage(self):
        """``code_stage`` and ``code_workspace`` are mutually exclusive."""
        if self.code_stage is not None and self.code_workspace is not None:
            raise ValueError("Specify either code_stage or code_workspace, not both.")
        return self

    runtime_image: str = Field(
        title="Runtime image used by SPCS artifact repo build/run",
        default="",
    )

    spcs_test_project_type: Optional[str] = Field(
        title="Project type override for SPCS_TEST builds",
        default=None,
    )

    @field_validator("spcs_test_project_type", mode="before")
    @classmethod
    def _validate_spcs_test_project_type(cls, value):
        if value is None or value == "null":
            return None
        if not isinstance(value, str):
            raise ValueError("spcs_test_project_type must be a string or null")
        return value.strip()
