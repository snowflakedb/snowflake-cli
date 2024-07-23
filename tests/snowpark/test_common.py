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

from typing import Tuple

import pytest
from snowflake.cli.api.constants import ObjectType
from snowflake.cli.plugins.snowpark.common import (
    _convert_resource_details_to_dict,
    _snowflake_dependencies_differ,
    _sql_to_python_return_type_mapper,
    check_if_replace_is_required,
)


def test_get_snowflake_packages_delta():
    for uploaded_packages, new_packages, expected in [
        ([], [], False),
        (
            ["package", "package_with_requirements>=2,<4"],
            ["package-with-requirements <4,>=2", "PACKAGE"],
            False,
        ),
        (
            ["different-requirements<1.1,>0.9"],
            ["different-requirements<1.0,>0.9"],
            True,
        ),
        (["different-package"], ["another-package"], True),
        (["package"], ["package", "added-package"], True),
        (["package", "removed-package"], ["package"], True),
    ]:
        assert expected == _snowflake_dependencies_differ(
            uploaded_packages, new_packages
        )


def test_convert_resource_details_to_dict():
    resource_details = [
        ("packages", "{'name': 'my-awesome-package','version': '1.2.3'}"),
        ("handler", "handler_function"),
    ]

    assert _convert_resource_details_to_dict(resource_details) == {
        "packages": {"name": "my-awesome-package", "version": "1.2.3"},
        "handler": "handler_function",
    }


@pytest.mark.parametrize(
    "argument",
    [
        ("NUMBER(38,0)", "int"),
        ("TIMESTAMP_NTZ(9)", "datetime"),
        ("TIMESTAMP_TZ(9)", "datetime"),
        ("VARCHAR(16777216)", "string"),
        ("FLOAT", "float"),
        ("ARRAY", "array"),
    ],
)
def test_sql_to_python_return_type_mapper(argument: Tuple[str, str]):
    assert _sql_to_python_return_type_mapper(argument[0]) == argument[1]


@pytest.mark.parametrize(
    "arguments, expected",
    [
        ({}, False),
        ({"handler": "app.another_procedure"}, True),
        ({"return_type": "variant"}, True),
        ({"snowflake_dependencies": ["snowflake-snowpark-python", "pandas"]}, True),
        ({"external_access_integrations": ["My_Integration"]}, True),
        ({"imports": ["@FOO.BAR.BAZ/some_project/some_package.zip"]}, True),
        ({"imports": ["@FOO.BAR.BAZ/my_snowpark_project/app.zip"]}, False),
        (
            {"stage_artifact_file": "@FOO.BAR.BAZ/my_snowpark_project/another_app.zip"},
            True,
        ),
        ({"runtime_ver": "3.9"}, True),
        ({"execute_as_caller": False}, True),
    ],
)
def test_check_if_replace_is_required(mock_procedure_description, arguments, expected):
    replace_arguments = {
        "handler": "app.hello_procedure",
        "return_type": "string",
        "snowflake_dependencies": ["snowflake-snowpark-python", "pytest<9.0.0,>=7.0.0"],
        "external_access_integrations": [],
        "imports": [],
        "stage_artifact_file": "@FOO.BAR.BAZ/my_snowpark_project/app.zip",
        "runtime_ver": "3.8",
        "execute_as_caller": True,
    }
    replace_arguments.update(arguments)

    assert (
        check_if_replace_is_required(
            ObjectType.PROCEDURE, mock_procedure_description, **replace_arguments
        )
        == expected
    )
