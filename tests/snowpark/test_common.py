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

from unittest import mock

import pytest
from snowflake.cli._plugins.snowpark.commands import (
    _parse_arg_types_from_show,
    drop_removed_snowpark_entities,
)
from snowflake.cli._plugins.snowpark.common import (
    _check_if_replace_is_required,
    _convert_resource_details_to_dict,
    _snowflake_dependencies_differ,
    is_name_a_templated_one,
    same_type,
)
from snowflake.cli._plugins.snowpark.snowpark_entity_model import (
    FunctionEntityModel,
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
    "arguments,expected",
    [
        ("MY_FN() RETURN VARCHAR", []),
        ("MY_FN(VARCHAR) RETURN VARCHAR", ["VARCHAR"]),
        ("MY_FN(VARCHAR, NUMBER) RETURN VARCHAR", ["VARCHAR", "NUMBER"]),
        ("MY_FN(NUMBER(38,0), VARCHAR) RETURN VARCHAR", ["NUMBER(38,0)", "VARCHAR"]),
        ("", []),
        ("no parens at all", []),
    ],
)
def test_parse_arg_types_from_show(arguments, expected):
    assert _parse_arg_types_from_show(arguments) == expected


def _make_procedure(name: str, signature):
    return ProcedureEntityModel(
        type="procedure",
        handler="app.handler",
        signature=signature,
        artifacts=[],
        stage="dev_deployment",
        returns="string",
        identifier={"name": name, "database": "MY_DB", "schema": "MY_SCHEMA"},
    )


def _make_function(name: str, signature):
    return FunctionEntityModel(
        type="function",
        handler="app.handler",
        signature=signature,
        artifacts=[],
        stage="dev_deployment",
        returns="string",
        identifier={"name": name, "database": "MY_DB", "schema": "MY_SCHEMA"},
    )


def _show_row(name: str, arguments: str, is_builtin: str = "N"):
    return {"name": name, "arguments": arguments, "is_builtin": is_builtin}


@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager")
def test_drop_removed_snowpark_entities_drops_unlisted_objects(mock_om_cls):
    om = mock.MagicMock()
    declared_proc = _make_procedure("KEEP_PROC", [])
    declared_fn = _make_function(
        "KEEP_FN",
        [{"name": "name", "type": "string"}],
    )
    entities = {"keep_proc": declared_proc, "keep_fn": declared_fn}

    om.show.side_effect = [
        # procedures in MY_DB.MY_SCHEMA
        [
            _show_row("KEEP_PROC", "KEEP_PROC() RETURN VARCHAR"),
            _show_row("STALE_PROC", "STALE_PROC(VARCHAR) RETURN VARCHAR"),
        ],
        # functions in MY_DB.MY_SCHEMA
        [
            _show_row("KEEP_FN", "KEEP_FN(VARCHAR) RETURN VARCHAR"),
            _show_row("STALE_FN", "STALE_FN(NUMBER) RETURN VARCHAR"),
            _show_row("SYSTEM_FN", "SYSTEM_FN() RETURN VARCHAR", is_builtin="Y"),
        ],
    ]

    drop_removed_snowpark_entities(om, entities)

    drop_calls = om.drop.call_args_list
    dropped = sorted(
        (call.kwargs["object_type"], call.kwargs["fqn"].identifier)
        for call in drop_calls
    )
    assert dropped == [
        ("function", "MY_DB.MY_SCHEMA.STALE_FN"),
        ("procedure", "MY_DB.MY_SCHEMA.STALE_PROC"),
    ]
    # signatures must be included so overloads are dropped safely
    proc_signatures = {
        call.kwargs["fqn"].signature
        for call in drop_calls
        if call.kwargs["object_type"] == "procedure"
    }
    assert proc_signatures == {"(VARCHAR)"}


@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager")
def test_drop_removed_snowpark_entities_keeps_matching_overloads(mock_om_cls):
    om = mock.MagicMock()
    declared = _make_procedure(
        "FN",
        [{"name": "name", "type": "string"}],
    )
    entities = {"fn_str": declared}

    # Server returns the overload with a different arg type and the string overload.
    om.show.side_effect = [
        [
            _show_row("FN", "FN(VARCHAR) RETURN VARCHAR"),  # matches declared
            _show_row("FN", "FN(NUMBER) RETURN VARCHAR"),  # overload not declared
        ],
    ]

    drop_removed_snowpark_entities(om, entities)

    drop_calls = om.drop.call_args_list
    assert len(drop_calls) == 1
    dropped_fqn = drop_calls[0].kwargs["fqn"]
    assert dropped_fqn.identifier == "MY_DB.MY_SCHEMA.FN"
    assert dropped_fqn.signature == "(NUMBER)"


@mock.patch("snowflake.cli._plugins.snowpark.commands.ObjectManager")
def test_drop_removed_snowpark_entities_does_not_touch_other_schemas(mock_om_cls):
    om = mock.MagicMock()
    entities = {
        "a": _make_procedure("PROC_A", []),
    }
    om.show.return_value = []

    drop_removed_snowpark_entities(om, entities)

    show_scopes = [call.kwargs["scope"] for call in om.show.call_args_list]
    assert show_scopes == [("schema", "MY_DB.MY_SCHEMA")]
    om.drop.assert_not_called()
