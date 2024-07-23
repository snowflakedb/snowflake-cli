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

from tests_integration.test_utils import row_from_cursor
from tests_integration.testing_utils.naming_utils import ObjectNameProvider

from contextlib import contextmanager
import json


@pytest.mark.integration
@pytest.mark.parametrize(
    "object_type,plural_object_type",
    [
        ("warehouse", "warehouses"),
        ("schema", "schemas"),
        ("external-access-integration", "external access integrations"),
    ],
)
def test_show(
    object_type, plural_object_type, runner, test_database, snowflake_session
):
    result = runner.invoke_with_connection_json(
        ["object", "list", object_type, "--format", "json"]
    )

    curr = snowflake_session.execute_string(f"show {plural_object_type}")
    expected = row_from_cursor(curr[-1])

    actual = result.json
    assert len(actual) == len(expected)
    assert actual[0].keys() == expected[0].keys()
    assert actual[0]["name"] == expected[0]["name"]


@pytest.mark.integration
def test_object_table(runner, test_database, snowflake_session):
    object_name = ObjectNameProvider(
        "Test_Object_Table"
    ).create_and_get_next_object_name()
    snowflake_session.execute_string(f"create table {object_name} (some_number NUMBER)")

    result_show = runner.invoke_with_connection_json(["object", "list", "table"])
    assert result_show.exit_code == 0

    actual_tables = row_from_cursor(
        snowflake_session.execute_string(f"show tables")[-1]
    )
    assert result_show.json[0].keys() == actual_tables[0].keys()
    assert result_show.json[0]["name"].lower() == object_name.lower()

    result_describe = runner.invoke_with_connection_json(
        ["object", "describe", "table", object_name]
    )
    assert result_describe.exit_code == 0
    assert result_describe.json[0]["name"] == "SOME_NUMBER"

    result_drop = runner.invoke_with_connection_json(
        ["object", "drop", "table", object_name]
    )
    assert result_drop.exit_code == 0
    assert (
        result_drop.json[0]["status"].lower()
        == f"{object_name.lower()} successfully dropped."
    )
    assert (
        len(row_from_cursor(snowflake_session.execute_string(f"show tables")[-1])) == 0
    )


@pytest.mark.integration
def test_list_with_scope(runner, test_database, snowflake_session):
    # create a table in a schema other than schema of the current connection
    other_schema = "other_schema"

    public_table = ObjectNameProvider("Public_Table").create_and_get_next_object_name()

    other_table = ObjectNameProvider("Other_Table").create_and_get_next_object_name()

    snowflake_session.execute_string(
        f"use schema public; create table {public_table} (some_number NUMBER);"
    )
    snowflake_session.execute_string(
        f"create schema {other_schema}; create table {other_table} (some_number NUMBER);"
    )
    result_list_public = runner.invoke_with_connection_json(
        ["object", "list", "table", "--in", "schema", "public"]
    )
    assert result_list_public.exit_code == 0, result_list_public.output
    assert result_list_public.json[0]["name"].lower() == public_table.lower()

    result_list_other = runner.invoke_with_connection_json(
        ["object", "list", "table", "--in", "schema", "other_schema"]
    )
    assert result_list_other.exit_code == 0, result_list_other.output
    assert result_list_other.json[0]["name"].lower() == other_table.lower()


@pytest.mark.integration
def test_show_drop_image_repository(runner, test_database, snowflake_session):
    repo_name = "TEST_REPO"

    result_create = runner.invoke_with_connection(
        ["sql", "-q", f"create image repository {repo_name}"]
    )
    assert result_create.exit_code == 0, result_create.output
    assert f"Image Repository {repo_name} successfully created" in result_create.output

    result_show = runner.invoke_with_connection(
        ["object", "list", "image-repository", "--format", "json"]
    )
    curr = snowflake_session.execute_string(f"show image repositories")
    expected = row_from_cursor(curr[-1])
    actual = result_show.json

    assert len(actual) == len(expected)
    assert actual[0].keys() == expected[0].keys()
    assert actual[0]["name"] == expected[0]["name"]

    result_drop = runner.invoke_with_connection(
        ["object", "drop", "image-repository", repo_name]
    )
    assert result_drop.exit_code == 0, result_drop.output
    assert f"{repo_name} successfully dropped" in result_drop.output


@pytest.mark.parametrize(
    "object_type,object_definition",
    [
        ("database", {}),
        (
            "schema",
            {"name": "test_create_schema"},
        ),
        ("image-repository", {"name": "test_create_image_repo"}),
        (
            "table",
            {
                "name": "test_create_table",
                "columns": [{"name": "col1", "datatype": "number", "nullable": False}],
                "constraints": [
                    {
                        "name": "prim_key",
                        "column_names": ["col1"],
                        "constraint_type": "PRIMARY KEY",
                    }
                ],
            },
        ),
        (
            "task",
            {
                "name": "test_create_task",
                "definition": "select 42",
                "warehouse": "xsmall",
                "schedule": {"schedule_type": "MINUTES_TYPE", "minutes": 32},
            },
        ),
        (
            "warehouse",
            {"name": "test_create_warehouse_<UUID>", "warehouse_size": "xsmall"},
        ),
    ],
)
@pytest.mark.integration
def test_create(object_type, object_definition, runner, test_database):
    if object_type == "database":
        object_definition["name"] = test_database + "_test_create_db"
    if "<UUID>" in object_definition["name"]:
        import uuid

        object_definition["name"] = object_definition["name"].replace(
            "<UUID>", str(uuid.uuid4().hex)
        )

    object_name = object_definition["name"]
    object_definition["comment"] = "created by Snowflake CLI automatic testing"

    @contextmanager
    def _cleanup_object():
        drop_cmd = ["object", "drop", object_type, object_name]
        try:
            yield
            runner.invoke_with_connection(drop_cmd, catch_exceptions=True)
        except Exception as e:
            runner.invoke_with_connection(drop_cmd, catch_exceptions=True)
            raise e

    def _test_create(params):
        # create object
        result = runner.invoke_with_connection(
            ["object", "create", object_type, *params]
        )
        assert result.exit_code == 0, result.output
        assert f"{object_name.upper()} successfully created" in result.output

        # object is visible
        result = runner.invoke_with_connection_json(["object", "list", object_type])
        assert result.exit_code == 0, result.output
        assert any(obj["name"].upper() == object_name.upper() for obj in result.json)

    # test json param
    with _cleanup_object():
        _test_create(["--json", json.dumps(object_definition)])
    # test key=value format
    with _cleanup_object():
        list_definition = [
            f"{key}={json.dumps(value)}" for key, value in object_definition.items()
        ]
        _test_create(list_definition)


@pytest.mark.integration
def test_create_error_conflict(runner, test_database, caplog):
    # conflict - an object already exists
    schema_name = "schema_noble_knight"
    result = runner.invoke_with_connection(
        ["object", "create", "schema", f"name={schema_name}"]
    )
    assert result.exit_code == 0, result.output
    result = runner.invoke_with_connection(
        ["object", "create", "schema", f"name={schema_name}", "--debug"]
    )
    assert result.exit_code == 1
    assert "An unexpected error occurred while creating the object." in result.output
    assert "object you are trying to create already exists" in result.output
    assert "409 Conflict" in caplog.text
    caplog.clear()


@pytest.mark.integration
def test_create_error_misspelled_argument(runner, test_database, caplog):
    # misspelled argument
    schema_name = "another_schema_name"
    result = runner.invoke_with_connection(
        ["object", "create", "schema", f"named={schema_name}", "--debug"]
    )
    assert result.exit_code == 1
    assert (
        "Incorrect object definition (arguments misspelled or malformatted)."
        in result.output
    )
    assert "HTTP 400: Bad Request" in caplog.text
    caplog.clear()


@pytest.mark.integration
def test_create_error_unsupported_type(runner, test_database):
    # object type that don't exist
    result = runner.invoke_with_connection(
        ["object", "create", "type_that_does_not_exist", "name=anything"]
    )
    assert result.exit_code == 1
    assert "Error" in result.output
    assert (
        "Create operation for type type_that_does_not_exist is not supported."
        in result.output
    )
    assert "using `sql -q 'CREATE ...'` command." in result.output


@pytest.mark.integration
def test_create_error_database_not_exist(runner):
    # database does not exist
    result = runner.invoke_with_connection(
        [
            "object",
            "create",
            "schema",
            "name=test_schema",
            "--database",
            "this_db_does_not_exist",
        ]
    )
    assert result.exit_code == 1, result.output
    assert "Error" in result.output
    assert "Database 'THIS_DB_DOES_NOT_EXIST' does not exist." in result.output


@pytest.mark.integration
def test_create_error_schema_not_exist(runner, test_database):
    # schema does not exist
    result = runner.invoke_with_connection(
        [
            "object",
            "create",
            "image-repository",
            "name=test_schema",
            "--schema",
            "this_schema_does_not_exist",
        ]
    )
    assert result.exit_code == 1, result.output
    assert "Error" in result.output
    assert "Schema 'THIS_SCHEMA_DOES_NOT_EXIST' does not exist." in result.output


@pytest.mark.integration
def test_create_error_undefined_database(runner):
    # undefined database
    result = runner.invoke_with_connection(
        ["object", "create", "schema", f"name=test_schema"]
    )
    assert result.exit_code == 1, result.output
    assert "Error" in result.output
    assert (
        "Database not defined in connection. Please try again with `--database` flag."
        in result.output
    )
