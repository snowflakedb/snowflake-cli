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

import pytest
from snowflake.cli._plugins.apps.snowflake_app_entity_model import (
    ArtifactRepositoryReference,
    CodeStageReference,
    ComputePoolReference,
    ExternalAccessReference,
    SnowflakeAppEntityModel,
    SnowflakeAppMetaField,
)
from snowflake.cli.api.utils.definition_rendering import render_definition_template


class TestSnowflakeAppEntityModel:
    def test_minimal_model(self):
        """Model can be created with only required fields."""
        model = SnowflakeAppEntityModel(
            type="snowflake-app",
            identifier="my_app",
            artifacts=["app/*"],
        )
        assert model.type == "snowflake-app"
        assert model.query_warehouse is None
        assert model.build_compute_pool is None
        assert model.service_compute_pool is None
        assert model.build_eai is None
        assert model.service_eai is None
        assert model.artifact_repository is None
        assert model.code_stage is None
        assert model.meta is None
        assert model.dev_roles is None

    def test_full_model(self):
        """Model can be created with all fields."""
        model = SnowflakeAppEntityModel(
            type="snowflake-app",
            identifier="my_app",
            artifacts=[{"src": "app/*", "dest": "./"}],
            query_warehouse="TEST_WH",
            build_compute_pool={"name": "BUILD_POOL"},
            service_compute_pool={"name": "SERVICE_POOL"},
            build_eai={"name": "BUILD_EAI"},
            service_eai={"name": "SERVICE_EAI"},
            artifact_repository={"name": "ARTIFACT_REPO"},
            code_stage={"name": "MY_STAGE", "encryption_type": "SNOWFLAKE_SSE"},
            meta={"title": "My App", "description": "A test app", "icon": "icon.png"},
            dev_roles=["DEV_ROLE_1", "DEV_ROLE_2"],
        )
        assert model.query_warehouse == "TEST_WH"
        assert model.build_compute_pool.name == "BUILD_POOL"
        assert model.service_compute_pool.name == "SERVICE_POOL"
        assert model.build_eai.name == "BUILD_EAI"
        assert model.service_eai.name == "SERVICE_EAI"
        assert model.artifact_repository.name == "ARTIFACT_REPO"
        assert model.code_stage.name == "MY_STAGE"
        assert model.code_stage.encryption_type == "SNOWFLAKE_SSE"
        assert model.meta.title == "My App"
        assert model.meta.description == "A test app"
        assert model.meta.icon == "icon.png"
        assert model.dev_roles == ["DEV_ROLE_1", "DEV_ROLE_2"]

    @pytest.mark.parametrize("value", [None, "null"])
    def test_compute_pool_validator_none_values(self, value):
        """Compute pool validator accepts None and 'null' as None."""
        model = SnowflakeAppEntityModel(
            type="snowflake-app",
            identifier="my_app",
            artifacts=["app/*"],
            build_compute_pool=value,
            service_compute_pool=value,
        )
        assert model.build_compute_pool is None
        assert model.service_compute_pool is None

    def test_compute_pool_validator_dict_value(self):
        """Compute pool validator passes through dict values."""
        model = SnowflakeAppEntityModel(
            type="snowflake-app",
            identifier="my_app",
            artifacts=["app/*"],
            build_compute_pool={"name": "MY_POOL"},
        )
        assert model.build_compute_pool.name == "MY_POOL"

    @pytest.mark.parametrize("value", [None, "null"])
    def test_eai_validator_none_values(self, value):
        """EAI validator accepts None and 'null' as None."""
        model = SnowflakeAppEntityModel(
            type="snowflake-app",
            identifier="my_app",
            artifacts=["app/*"],
            build_eai=value,
            service_eai=value,
        )
        assert model.build_eai is None
        assert model.service_eai is None

    def test_eai_validator_dict_value(self):
        """EAI validator passes through dict values."""
        model = SnowflakeAppEntityModel(
            type="snowflake-app",
            identifier="my_app",
            artifacts=["app/*"],
            build_eai={"name": "MY_EAI"},
        )
        assert model.build_eai.name == "MY_EAI"

    def test_code_stage_defaults(self):
        """Code stage encryption_type defaults to SNOWFLAKE_SSE."""
        model = SnowflakeAppEntityModel(
            type="snowflake-app",
            identifier="my_app",
            artifacts=["app/*"],
            code_stage={"name": "MY_STAGE"},
        )
        assert model.code_stage.encryption_type == "SNOWFLAKE_SSE"

    def test_meta_field_defaults(self):
        """Meta field sub-fields default to None."""
        model = SnowflakeAppEntityModel(
            type="snowflake-app",
            identifier="my_app",
            artifacts=["app/*"],
            meta={},
        )
        assert model.meta.title is None
        assert model.meta.description is None
        assert model.meta.icon is None


class TestSnowflakeAppInProjectDefinition:
    def test_snowflake_app_entity_in_project_definition(self):
        """snowflake-app entity can be parsed from a project definition."""
        definition_input = {
            "definition_version": "2",
            "entities": {
                "my_app": {
                    "type": "snowflake-app",
                    "identifier": "MY_APP",
                    "artifacts": [{"src": "app/*", "dest": "./"}],
                    "query_warehouse": "TEST_WH",
                    "build_compute_pool": {"name": "BUILD_POOL"},
                    "service_compute_pool": {"name": "SERVICE_POOL"},
                    "code_stage": {"name": "MY_STAGE"},
                }
            },
        }
        result = render_definition_template(definition_input, {})
        project = result.project_definition
        entity = project.entities["my_app"]
        assert entity.type == "snowflake-app"
        assert entity.query_warehouse == "TEST_WH"
        assert entity.build_compute_pool.name == "BUILD_POOL"
        assert entity.service_compute_pool.name == "SERVICE_POOL"
        assert entity.code_stage.name == "MY_STAGE"

    def test_snowflake_app_minimal_project_definition(self):
        """snowflake-app with minimal config can be parsed."""
        definition_input = {
            "definition_version": "2",
            "entities": {
                "my_app": {
                    "type": "snowflake-app",
                    "identifier": "MY_APP",
                    "artifacts": ["app/*"],
                }
            },
        }
        result = render_definition_template(definition_input, {})
        project = result.project_definition
        entity = project.entities["my_app"]
        assert entity.type == "snowflake-app"
        assert entity.query_warehouse is None

    def test_snowflake_app_with_null_compute_pools(self):
        """snowflake-app handles null compute pool values in YAML."""
        definition_input = {
            "definition_version": "2",
            "entities": {
                "my_app": {
                    "type": "snowflake-app",
                    "identifier": "MY_APP",
                    "artifacts": ["app/*"],
                    "build_compute_pool": None,
                    "service_compute_pool": None,
                }
            },
        }
        result = render_definition_template(definition_input, {})
        project = result.project_definition
        entity = project.entities["my_app"]
        assert entity.build_compute_pool is None
        assert entity.service_compute_pool is None

    def test_snowflake_app_with_meta(self):
        """snowflake-app handles meta fields including title, description, icon."""
        definition_input = {
            "definition_version": "2",
            "entities": {
                "my_app": {
                    "type": "snowflake-app",
                    "identifier": "MY_APP",
                    "artifacts": ["app/*"],
                    "meta": {
                        "title": "My App Title",
                        "description": "My App Description",
                        "icon": "icon.png",
                    },
                }
            },
        }
        result = render_definition_template(definition_input, {})
        project = result.project_definition
        entity = project.entities["my_app"]
        assert entity.meta.title == "My App Title"
        assert entity.meta.description == "My App Description"
        assert entity.meta.icon == "icon.png"


class TestSubModels:
    def test_compute_pool_reference(self):
        ref = ComputePoolReference(name="MY_POOL")
        assert ref.name == "MY_POOL"

    def test_compute_pool_reference_optional_name(self):
        ref = ComputePoolReference()
        assert ref.name is None

    def test_external_access_reference(self):
        ref = ExternalAccessReference(name="MY_EAI")
        assert ref.name == "MY_EAI"

    def test_artifact_repository_reference(self):
        ref = ArtifactRepositoryReference(name="MY_REPO")
        assert ref.name == "MY_REPO"

    def test_code_stage_reference(self):
        ref = CodeStageReference(name="MY_STAGE")
        assert ref.name == "MY_STAGE"
        assert ref.encryption_type == "SNOWFLAKE_SSE"

    def test_code_stage_reference_custom_encryption(self):
        ref = CodeStageReference(name="MY_STAGE", encryption_type="SNOWFLAKE_FULL")
        assert ref.encryption_type == "SNOWFLAKE_FULL"

    def test_snowflake_app_meta_field(self):
        meta = SnowflakeAppMetaField(title="Title", description="Desc", icon="icon.png")
        assert meta.title == "Title"
        assert meta.description == "Desc"
        assert meta.icon == "icon.png"

    def test_snowflake_app_meta_field_defaults(self):
        meta = SnowflakeAppMetaField()
        assert meta.title is None
        assert meta.description is None
        assert meta.icon is None
