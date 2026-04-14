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

from typing import List, Literal, Optional, Union

# Default port exposed by Snowflake App services
DEFAULT_APP_PORT = 3000

from pydantic import Field, field_validator
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
    """Reference to an external access integration."""

    name: Optional[str] = IdentifierField(
        title="Name of the external access integration", default=None
    )
    schema_: Optional[str] = IdentifierField(
        title="Schema of the external access integration",
        alias="schema",
        default=None,
    )
    database: Optional[str] = IdentifierField(
        title="Database of the external access integration", default=None
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


class ImageRepositoryReference(UpdatableModel):
    """Reference to an image repository used for container image storage."""

    name: str = IdentifierField(title="Name of the image repository")
    schema_: Optional[str] = IdentifierField(
        title="Schema of the image repository", alias="schema", default=None
    )
    database: Optional[str] = IdentifierField(
        title="Database of the image repository", default=None
    )


class CodeStageReference(UpdatableModel):
    """Reference to a code stage."""

    name: str = IdentifierField(title="Name of the code stage")
    encryption_type: Optional[str] = Field(
        title="Encryption type for the stage", default="SNOWFLAKE_SSE"
    )


class SnowflakeAppMetaField(MetaField):
    """Extended meta field for Snowflake Apps with title, description, icon."""

    title: Optional[str] = Field(title="Title of the Snowflake App", default=None)
    description: Optional[str] = Field(
        title="Description of the Snowflake App", default=None
    )
    icon: Optional[str] = Field(title="Icon for the Snowflake App", default=None)


class SnowflakeAppEntityModel(EntityModelBaseWithArtifacts):
    """Entity model for Snowflake App (snowflake-app) type."""

    type: Literal["snowflake-app"] = DiscriminatorField()  # noqa: A003

    meta: Optional[SnowflakeAppMetaField] = Field(title="Meta fields", default=None)

    query_warehouse: Optional[str] = IdentifierField(
        title="Warehouse to use for queries", default=None
    )

    build_compute_pool: Union[ComputePoolReference, None] = Field(
        title="Compute pool for building the app", default=None
    )

    service_compute_pool: Union[ComputePoolReference, None] = Field(
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

    service_eai: Union[ExternalAccessReference, None] = Field(
        title="External access integration for service", default=None
    )

    @field_validator("build_eai", "service_eai", mode="before")
    @classmethod
    def _validate_eai(cls, value):
        """Allow null/None values for EAI fields."""
        if value is None or value == "null":
            return None
        return value

    artifact_repository: Optional[ArtifactRepositoryReference] = Field(
        title="Artifact repository for the app", default=None
    )

    image_repository: Optional[ImageRepositoryReference] = Field(
        title="Image repository for container images", default=None
    )

    code_stage: Optional[CodeStageReference] = Field(
        title="Stage for storing code artifacts", default=None
    )

    app_port: int = Field(title="Port the app listens on", default=DEFAULT_APP_PORT)

    runtime_image: str = Field(
        title="Runtime image used by SPCS artifact repo build/run",
        default="",
    )

    build_image: Optional[str] = Field(
        title="Custom container image for building the app",
        default=None,
    )

    @field_validator("build_image", mode="before")
    @classmethod
    def _validate_build_image(cls, value):
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("build_image must be a non-empty string")
        value = value.strip()
        import re

        if re.search(r"\s", value):
            raise ValueError(f"build_image must not contain whitespace, got: {value!r}")
        _unsafe_chars = {"$", '"'}
        found = _unsafe_chars.intersection(value)
        if found:
            raise ValueError(
                f"build_image contains unsafe character(s) {found}, got: {value!r}"
            )
        return value

    execute_as_caller: bool = Field(
        title="Whether the service runs with caller privileges",
        default=True,
    )

    dev_roles: Optional[List[str]] = Field(
        title="Development roles for the app", default=None
    )
