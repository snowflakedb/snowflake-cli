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

import pytest
from snowflake.cli._plugins.snowpark.common import (
    _check_if_replace_is_required,
    _convert_resource_details_to_dict,
    _parse_remote_signature,
    _signatures_differ,
    _snowflake_dependencies_differ,
    is_name_a_templated_one,
    same_type,
)
from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    ProcedureEntityModel,
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
    "arguments, expected",
    [
        ({}, False),
        ({"handler": "app.another_procedure"}, True),
        ({"returns": "variant"}, True),
        ({"external_access_integrations": ["My_Integration"]}, True),
        ({"imports": ["@FOO.BAR.BAZ/some_project/some_package.zip"]}, True),
        ({"imports": ["@FOO.BAR.BAZ/my_snowpark_project/app.zip"]}, False),
        ({"runtime": "3.9"}, True),
        ({"execute_as_caller": False}, True),
    ],
)
def test_check_if_replace_is_required_entity_changes(
    mock_procedure_description, arguments, expected
):
    entity_spec = {
        "type": "procedure",
        "handler": "app.hello_procedure",
        "signature": "(NAME VARCHAR)",
        "artifacts": [],
        "stage": "foo",
        "returns": "string",
        "external_access_integrations": [],
        "imports": [],
        "runtime": "3.10",
        "execute_as_caller": True,
    }
    entity_spec.update(arguments)

    entity = ProcedureEntityModel(**entity_spec)

    assert (
        _check_if_replace_is_required(
            entity=entity,
            current_state=mock_procedure_description,
            snowflake_dependencies=[
                "snowflake-snowpark-python",
                "pytest<9.0.0,>=7.0.0",
            ],
            stage_artifact_files={"@FOO.BAR.BAZ/my_snowpark_project/app.zip"},
        )
        == expected
    )


@pytest.mark.parametrize(
    "arguments, expected",
    [
        ({"snowflake_dependencies": ["snowflake-snowpark-python", "pandas"]}, True),
        (
            {
                "stage_artifact_files": [
                    "@FOO.BAR.BAZ/my_snowpark_project/another_app.zip"
                ]
            },
            True,
        ),
    ],
)
def test_check_if_replace_is_required_file_changes(
    mock_procedure_description, arguments, expected
):
    entity_spec = {
        "type": "procedure",
        "handler": "app.hello_procedure",
        "signature": "(NAME VARCHAR)",
        "artifacts": [],
        "stage": "foo",
        "returns": "string",
        "external_access_integrations": [],
        "imports": [],
        "runtime": "3.10",
        "execute_as_caller": True,
    }
    entity = ProcedureEntityModel(**entity_spec)

    kwargs = {
        "snowflake_dependencies": ["snowflake-snowpark-python", "pytest<9.0.0,>=7.0.0"],
        "stage_artifact_files": {"@FOO.BAR.BAZ/my_snowpark_project/app.zip"},
    }
    kwargs.update(arguments)
    assert (
        _check_if_replace_is_required(
            entity=entity, current_state=mock_procedure_description, **kwargs
        )
        == expected
    )


@pytest.mark.parametrize(
    "name,expected",
    [
        ("foo", False),
        ("<% ctx.env.foo %>", True),
        ("<! name | to_snowflake_identifier !>", True),
        ("app_<% ctx.env.USERNAME %>", True),
        ("<Unnecesarily_!_complicated!_name>", False),
        ("<% fn.concat_ids(ctx.native_app.name, ctx.env.pkg_suffix) %>", True),
        ("myapp_base_name_<% fn.sanitize_id(fn.get_username()) %>", True),
        ("<myapp>", False),
    ],
)
def test_is_name_is_templated_one(name: str, expected: bool):
    assert is_name_a_templated_one(name) == expected


@pytest.mark.parametrize(
    "sf_type, local_type",
    [
        ("VARCHAR", "STRING"),
        ("VARCHAR(16777216)", "STRING"),
        ("VARCHAR(16777216)", "VARCHAR"),
        ("VARCHAR(16777216)", "VARCHAR(16777216)"),
        ("NUMBER(38,0)", "int"),
        ("TIMESTAMP_NTZ", "datetime"),
        ("FLOAT", "float"),
        ("ARRAY", "array"),
    ],
)
def test_the_same_type(sf_type, local_type):
    assert same_type(sf_type, local_type)


@pytest.mark.parametrize(
    "sf_type, local_type",
    [
        ("VARCHAR(25)", "STRING"),
        ("VARCHAR(25)", "VARCHAR(16777216)"),
    ],
)
def test_is_not_the_same_type(sf_type, local_type):
    assert not same_type(sf_type, local_type)


@pytest.mark.parametrize(
    "signature, expected",
    [
        ("()", []),
        ("", []),
        ("(NAME VARCHAR)", [("NAME", "VARCHAR", None)]),
        (
            "(NAME VARCHAR, AGE NUMBER)",
            [("NAME", "VARCHAR", None), ("AGE", "NUMBER", None)],
        ),
        (
            "(NAME VARCHAR DEFAULT 'hello')",
            [("NAME", "VARCHAR", "'hello'")],
        ),
        (
            "(NAME VARCHAR DEFAULT 'with, comma')",
            [("NAME", "VARCHAR", "'with, comma'")],
        ),
        (
            "(AMOUNT NUMBER(38,0) DEFAULT 10)",
            [("AMOUNT", "NUMBER(38,0)", "10")],
        ),
        (
            "(A VARCHAR DEFAULT 'x', B NUMBER)",
            [("A", "VARCHAR", "'x'"), ("B", "NUMBER", None)],
        ),
    ],
)
def test_parse_remote_signature(signature, expected):
    assert _parse_remote_signature(signature) == expected


def _make_proc_entity(signature):
    return ProcedureEntityModel(
        **{
            "type": "procedure",
            "handler": "app.hello_procedure",
            "signature": signature,
            "artifacts": [],
            "stage": "foo",
            "returns": "string",
            "external_access_integrations": [],
            "imports": [],
            "runtime": "3.10",
            "execute_as_caller": True,
        }
    )


@pytest.mark.parametrize(
    "remote_sig, local_sig, expected_differ",
    [
        # Same single arg, no default on either side.
        ("(NAME VARCHAR)", [{"name": "name", "type": "varchar"}], False),
        # Default added locally where the remote has none.
        (
            "(NAME VARCHAR)",
            [{"name": "name", "type": "varchar", "default": "hello"}],
            True,
        ),
        # Default removed locally where the remote still has one. This is the
        # exact scenario from the bug report (#1992).
        (
            "(START_TIME VARCHAR DEFAULT 'test_default_value')",
            [{"name": "start_time", "type": "string"}],
            True,
        ),
        # Default unchanged on both sides — should NOT force a replace.
        (
            "(START_TIME VARCHAR DEFAULT 'test_default_value')",
            [
                {
                    "name": "start_time",
                    "type": "string",
                    "default": "test_default_value",
                }
            ],
            False,
        ),
        # Argument added locally.
        (
            "(A VARCHAR)",
            [
                {"name": "a", "type": "varchar"},
                {"name": "b", "type": "number"},
            ],
            True,
        ),
        # Argument removed locally.
        (
            "(A VARCHAR, B NUMBER)",
            [{"name": "a", "type": "varchar"}],
            True,
        ),
        # Argument renamed.
        (
            "(A VARCHAR)",
            [{"name": "renamed", "type": "varchar"}],
            True,
        ),
        # Argument type changed.
        (
            "(A VARCHAR)",
            [{"name": "a", "type": "number"}],
            True,
        ),
    ],
)
def test_signatures_differ(remote_sig, local_sig, expected_differ):
    entity = _make_proc_entity(local_sig)
    assert _signatures_differ(remote_sig, entity) is expected_differ


def test_check_if_replace_is_required_detects_removed_default(mock_cursor):
    """Regression test for #1992: removing a default from an argument should
    force the procedure to be re-created on ``snow snowpark deploy --replace``.
    """
    remote = mock_cursor(
        rows=[
            ("signature", "(START_TIME VARCHAR DEFAULT 'test_default_value')"),
            ("returns", "VARCHAR(16777216)"),
            ("language", "PYTHON"),
            ("execute as", "CALLER"),
            ("body", None),
            ("imports", "[@FOO.BAR.BAZ/my_snowpark_project/app.zip]"),
            ("handler", "app.hello_procedure"),
            ("runtime_version", "3.10"),
            ("packages", "['snowflake-snowpark-python','pytest<9.0.0,>=7.0.0']"),
            ("installed_packages", "['_libgcc_mutex==0.1']"),
            ("artifact_repository", None),
            ("artifact_repository_packages", None),
        ],
        columns=[
            "signature",
            "returns",
            "language",
            "execute as",
            "body",
            "imports",
            "handler",
            "runtime_version",
            "packages",
            "installed_packages",
        ],
    )
    entity = _make_proc_entity([{"name": "start_time", "type": "string"}])

    assert (
        _check_if_replace_is_required(
            entity=entity,
            current_state=remote,
            snowflake_dependencies=[
                "snowflake-snowpark-python",
                "pytest<9.0.0,>=7.0.0",
            ],
            stage_artifact_files={"@FOO.BAR.BAZ/my_snowpark_project/app.zip"},
        )
        is True
    )
