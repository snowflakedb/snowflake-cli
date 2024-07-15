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
from click import ClickException
from snowflake.cli.api.project.schemas.project_definition import (
    DefinitionV11,
    DefinitionV20,
)

# from snowflake.cli.api.project.errors import SchemaValidationError
from snowflake.cli.plugins.nativeapp.v2_conversions.v2_to_v1_decorator import (
    _pdf_v2_to_v1,
)

from tests.testing_utils.mock_config import mock_config_key


@pytest.mark.parametrize(
    "pdfv2_input, expected_pdfv1, expected_error",
    [
        [
            {
                "definition_version": "2",
                "entities": {
                    "pkg1": {
                        "type": "application package",
                        "name": "pkg",
                        "artifacts": [],
                        "manifest": "",
                    },
                    "pkg2": {
                        "type": "application package",
                        "name": "pkg",
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
                        "name": "pkg",
                        "artifacts": [],
                        "manifest": "",
                    },
                    "app1": {
                        "type": "application",
                        "name": "pkg",
                        "from": {"target": "pkg"},
                    },
                    "app2": {
                        "type": "application",
                        "name": "pkg",
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
                        "name": "pkg_name",
                        "artifacts": [{"src": "app/*", "dest": "./"}],
                        "manifest": "",
                        "stage": "app.stage",
                    },
                    "app": {
                        "type": "application",
                        "name": "app_name",
                        "from": {"target": "pkg"},
                        "meta": {"role": "app_role"},
                    },
                },
            },
            {
                "definition_version": "1.1",
                "native_app": {
                    "name": "Auto converted NativeApp project from V2",
                    "artifacts": [{"src": "app/*", "dest": "./"}],
                    "source_stage": "app.stage",
                    "package": {
                        "name": "pkg_name",
                    },
                    "application": {
                        "name": "app_name",
                        "role": "app_role",
                    },
                },
            },
            None,
        ],
    ],
)
def test_v2_to_v1_conversions(pdfv2_input, expected_pdfv1, expected_error):
    with mock_config_key("enable_project_definition_v2", True):
        pdfv2 = DefinitionV20(**pdfv2_input)
        if expected_error:
            with pytest.raises(ClickException) as err:
                _pdf_v2_to_v1(pdfv2)
            assert expected_error in err.value.message
        else:
            pdfv1_actual = vars(_pdf_v2_to_v1(pdfv2))
            pdfv1_expected = vars(DefinitionV11(**expected_pdfv1))

            # Assert that the expected dict is a subset of the actual dict
            assert {**pdfv1_actual, **pdfv1_expected} == pdfv1_actual
