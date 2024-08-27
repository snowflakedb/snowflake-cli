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

from unittest import mock

import pytest
from click import ClickException
from snowflake.cli._plugins.nativeapp.v2_conversions.v2_to_v1_decorator import (
    _pdf_v2_to_v1,
    nativeapp_definition_v2_to_v1,
)
from snowflake.cli.api.cli_global_context import (
    get_cli_context,
    get_cli_context_manager,
)
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV11,
    DefinitionV20,
)


@pytest.mark.parametrize(
    "pdfv2_input, expected_pdfv1, expected_error",
    [
        [
            {
                "definition_version": "2",
                "entities": {
                    "pkg1": {
                        "type": "application package",
                        "identifier": "pkg",
                        "artifacts": [],
                        "manifest": "",
                    },
                    "pkg2": {
                        "type": "application package",
                        "identifier": "pkg",
                        "artifacts": [],
                        "manifest": "",
                    },
                },
            },
            None,
            "More than one application package entity exists",
        ],
        [
            {
                "definition_version": "2",
                "entities": {
                    "pkg": {
                        "type": "application package",
                        "identifier": "pkg",
                        "artifacts": [],
                        "manifest": "",
                    },
                    "app1": {
                        "type": "application",
                        "identifier": "pkg",
                        "from": {"target": "pkg"},
                    },
                    "app2": {
                        "type": "application",
                        "identifier": "pkg",
                        "from": {"target": "pkg"},
                    },
                },
            },
            None,
            "More than one application entity exists",
        ],
        [
            {
                "definition_version": "2",
                "entities": {
                    "pkg": {
                        "type": "application package",
                        "identifier": "pkg_name",
                        "artifacts": [{"src": "app/*", "dest": "./"}],
                        "manifest": "",
                        "stage": "app.stage",
                        "bundle_root": "bundle_root_path",
                        "generated_root": "generated_root_path",
                        "deploy_root": "deploy_root_path",
                        "scratch_stage": "scratch_stage_path",
                        "meta": {
                            "role": "pkg_role",
                            "warehouse": "pkg_wh",
                            "post_deploy": [
                                {"sql_script": "scripts/script1.sql"},
                                {"sql_script": "scripts/script2.sql"},
                            ],
                        },
                        "distribution": "external",
                    },
                    "app": {
                        "type": "application",
                        "identifier": "app_name",
                        "from": {"target": "pkg"},
                        "debug": True,
                        "meta": {
                            "role": "app_role",
                            "warehouse": "app_wh",
                            "post_deploy": [
                                {"sql_script": "scripts/script3.sql"},
                                {"sql_script": "scripts/script4.sql"},
                            ],
                        },
                    },
                },
            },
            {
                "definition_version": "1.1",
                "native_app": {
                    "name": "app_name",
                    "artifacts": [{"src": "app/*", "dest": "./"}],
                    "source_stage": "app.stage",
                    "bundle_root": "bundle_root_path",
                    "generated_root": "generated_root_path",
                    "deploy_root": "deploy_root_path",
                    "scratch_stage": "scratch_stage_path",
                    "package": {
                        "name": "pkg_name",
                        "distribution": "external",
                        "role": "pkg_role",
                        "warehouse": "pkg_wh",
                        "post_deploy": [
                            {"sql_script": "scripts/script1.sql"},
                            {"sql_script": "scripts/script2.sql"},
                        ],
                    },
                    "application": {
                        "name": "app_name",
                        "role": "app_role",
                        "debug": True,
                        "warehouse": "app_wh",
                        "post_deploy": [
                            {"sql_script": "scripts/script3.sql"},
                            {"sql_script": "scripts/script4.sql"},
                        ],
                    },
                },
            },
            None,
        ],
    ],
)
def test_v2_to_v1_conversions(pdfv2_input, expected_pdfv1, expected_error):
    pdfv2 = DefinitionV20(**pdfv2_input)
    if expected_error:
        with pytest.raises(ClickException, match=expected_error) as err:
            _pdf_v2_to_v1(pdfv2)
    else:
        pdfv1_actual = vars(_pdf_v2_to_v1(pdfv2))
        pdfv1_expected = vars(DefinitionV11(**expected_pdfv1))

        # Assert that the expected dict is a subset of the actual dict
        assert {**pdfv1_actual, **pdfv1_expected} == pdfv1_actual


def test_decorator_error_when_no_project_exists():
    with pytest.raises(ValueError, match="Project definition could not be found"):
        nativeapp_definition_v2_to_v1(lambda *args: None)()


@pytest.mark.parametrize(
    "pdfv2_input, expected_project_name",
    [
        [
            # Using application name as project name
            {
                "definition_version": "2",
                "entities": {
                    "pkg": {
                        "type": "application package",
                        "identifier": "package_name",
                        "artifacts": [{"src": "app/*", "dest": "./"}],
                        "manifest": "",
                        "stage": "app.stage",
                    },
                    "app": {
                        "type": "application",
                        "identifier": "application_name",
                        "from": {"target": "pkg"},
                        "meta": {
                            "role": "app_role",
                            "post_deploy": [
                                {"sql_script": "scripts/script3.sql"},
                                {"sql_script": "scripts/script4.sql"},
                            ],
                        },
                    },
                },
            },
            "application_name",
        ],
        [
            # Using package name as project name
            {
                "definition_version": "2",
                "entities": {
                    "pkg": {
                        "type": "application package",
                        "identifier": "package_name",
                        "artifacts": [{"src": "app/*", "dest": "./"}],
                        "manifest": "",
                        "stage": "app.stage",
                    },
                },
            },
            "package_name",
        ],
        [
            # Using package name as project name, stripping _pkg_.*
            {
                "definition_version": "2",
                "entities": {
                    "pkg": {
                        "type": "application package",
                        "identifier": "appname_pkg_username",
                        "artifacts": [{"src": "app/*", "dest": "./"}],
                        "manifest": "",
                        "stage": "app.stage",
                    },
                },
            },
            "appname",
        ],
    ],
)
def test_project_name(pdfv2_input, expected_project_name):
    pdfv2 = DefinitionV20(**pdfv2_input)
    pdfv1 = _pdf_v2_to_v1(pdfv2)

    # Assert that the expected dict is a subset of the actual dict
    assert pdfv1.native_app.name == expected_project_name


@mock.patch(
    "snowflake.cli._plugins.nativeapp.v2_conversions.v2_to_v1_decorator._pdf_v2_to_v1"
)
def test_decorator_skips_when_project_is_not_v2(mock_pdf_v2_to_v1):
    pdfv1 = DefinitionV11(
        **{
            "definition_version": "1.1",
            "native_app": {
                "name": "test",
                "artifacts": [{"src": "*", "dest": "./"}],
            },
        },
    )
    get_cli_context_manager().set_project_definition(pdfv1)

    nativeapp_definition_v2_to_v1(lambda *args: None)()

    mock_pdf_v2_to_v1.launch.assert_not_called()
    assert get_cli_context().project_definition == pdfv1
