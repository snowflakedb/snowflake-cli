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

import os
from unittest import mock

import pytest
from snowflake.cli.api.project.definition import load_project
from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.cli.api.project.schemas.project_definition import (
    build_project_definition,
)

from tests.nativeapp.factories import PdfV10Factory


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_napp_project_1(project_definition_files):
    project = load_project(project_definition_files).project_definition
    assert project.native_app.name == "myapp"
    assert project.native_app.deploy_root == "output/deploy/"
    assert project.native_app.package.role == "accountadmin"
    assert project.native_app.application.name == "myapp_polly"
    assert project.native_app.application.role == "myapp_consumer"
    assert project.native_app.application.debug == True


@mock.patch.dict(os.environ, {"USER": "jsmith"})
def test_na_minimal_project(temporary_directory):
    minimal_pdf = PdfV10Factory(native_app__artifacts=["setup.sql", "README.md"])
    project = load_project([minimal_pdf.path]).project_definition
    assert project.native_app.name == minimal_pdf.yml["native_app"]["name"]
    assert project.native_app.artifacts == [
        PathMapping(src="setup.sql"),
        PathMapping(src="README.md"),
    ]


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
