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

from pathlib import Path
from typing import List, Optional
from unittest import mock
from unittest.mock import PropertyMock

import pytest
from snowflake.cli.api.feature_flags import FeatureFlag
from snowflake.cli.api.project.definition import (
    generate_local_override_yml,
    load_project,
)
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.api.project.schemas.project_definition import (
    build_project_definition,
)

from tests.testing_utils.mock_config import mock_config_key


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_napp_project_1(project_definition_files):
    project = load_project(project_definition_files).project_definition
    assert project.native_app.name == "myapp"
    assert project.native_app.deploy_root == "output/deploy/"
    assert project.native_app.package.role == "accountadmin"
    assert project.native_app.application.name == "myapp_polly"
    assert project.native_app.application.role == "myapp_consumer"
    assert project.native_app.application.debug == True


@pytest.mark.parametrize("project_definition_files", ["minimal"], indirect=True)
def test_na_minimal_project(project_definition_files: List[Path]):
    project = load_project(project_definition_files).project_definition
    assert project.native_app.name == "minimal"
    assert project.native_app.artifacts == [
        PathMapping(src="setup.sql"),
        PathMapping(src="README.md"),
    ]

    from os import getenv as original_getenv

    def mock_getenv(key: str, default: Optional[str] = None) -> Optional[str]:
        if key.lower() == "user":
            return "jsmith"
        return original_getenv(key, default)

    with mock.patch(
        "snowflake.cli.api.cli_global_context._CliGlobalContextAccess.connection",
        new_callable=PropertyMock,
    ) as connection:
        connection.return_value.role = "resolved_role"
        connection.return_value.warehouse = "resolved_warehouse"
        with mock.patch("os.getenv", side_effect=mock_getenv):
            # TODO: probably a better way of going about this is to not generate
            # a definition structure for these values but directly return defaults
            # in "getter" functions (higher-level data structures).
            local = generate_local_override_yml(project)
            assert local.native_app.application.name == "minimal_jsmith"
            assert local.native_app.application.role == "resolved_role"
            assert local.native_app.application.warehouse == "resolved_warehouse"
            assert local.native_app.application.debug == True
            assert local.native_app.package.name == "minimal_pkg_jsmith"
            assert local.native_app.package.role == "resolved_role"


@pytest.mark.parametrize("project_definition_files", ["underspecified"], indirect=True)
def test_underspecified_project(project_definition_files):
    with pytest.raises(SchemaValidationError) as exc_info:
        load_project(project_definition_files).project_definition

    assert (
        "Your project definition is missing the following field: 'native_app.artifacts'"
        in str(exc_info.value)
    )


@pytest.mark.parametrize(
    "project_definition_files", ["no_definition_version"], indirect=True
)
def test_fails_without_definition_version(project_definition_files):
    with pytest.raises(SchemaValidationError) as exc_info:
        load_project(project_definition_files).project_definition

    assert (
        "Your project definition is missing the following field: 'definition_version'"
        in str(exc_info.value)
    )


@pytest.mark.parametrize("project_definition_files", ["unknown_fields"], indirect=True)
def test_does_not_accept_unknown_fields(project_definition_files):
    with pytest.raises(SchemaValidationError) as exc_info:
        load_project(project_definition_files).project_definition

    assert (
        "You provided field 'native_app.unknown_fields_accepted' with value 'true' that is not supported in given version."
        in str(exc_info)
    )


@pytest.mark.parametrize(
    "project_definition_files",
    [
        "integration",
        "integration_external",
        "minimal",
        "napp_project_1",
        "napp_project_with_pkg_warehouse",
        "snowpark_function_external_access",
        "snowpark_function_fully_qualified_name",
        "snowpark_function_secrets_without_external_access",
        "snowpark_functions",
        "snowpark_procedure_external_access",
        "snowpark_procedure_fully_qualified_name",
        "snowpark_procedure_secrets_without_external_access",
        "snowpark_procedures",
        "snowpark_procedures_coverage",
        "streamlit_full_definition",
    ],
    indirect=True,
)
def test_fields_are_parsed_correctly(project_definition_files, os_agnostic_snapshot):
    result = load_project(project_definition_files).project_definition.model_dump(
        mode="json"
    )
    assert result == os_agnostic_snapshot


@pytest.mark.parametrize(
    "data",
    [
        {"definition_version": "1", "env": {"foo": "bar"}},
        {"definition_version": "1.1", "unknown": {}},
    ],
)
def test_schema_is_validated_for_version(data):
    with pytest.raises(SchemaValidationError) as err:
        build_project_definition(**data)

    assert "is not supported in given version" in str(err.value)


def test_project_definition_v2_is_disabled():
    assert FeatureFlag.ENABLE_PROJECT_DEFINITION_V2.is_enabled() == False
    with pytest.raises(SchemaValidationError) as err:
        build_project_definition(**{"definition_version": "2", "entities": {}})
    assert "Version 2 is not supported" in str(err.value)


def test_project_definition_v2_is_enabled_with_feature_flag():
    with mock_config_key("enable_project_definition_v2", True):
        assert FeatureFlag.ENABLE_STREAMLIT_EMBEDDED_STAGE.is_enabled() == False
        assert FeatureFlag.ENABLE_STREAMLIT_NO_CHECKOUTS.is_enabled() == False
        assert FeatureFlag.ENABLE_STREAMLIT_VERSIONED_STAGE.is_enabled() == False
        assert FeatureFlag.ENABLE_PROJECT_DEFINITION_V2.is_enabled() == True
        build_project_definition(**{"definition_version": "2", "entities": {}})
