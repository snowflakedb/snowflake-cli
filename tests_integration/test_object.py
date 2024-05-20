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
            {
                "name": "test_schema",
                "comment": "test_schema = schema for testing stuff",
            },
        ),
        # TODO: check bad requests
        # table - bad request - to check
        # warehouse - probably not enough permissions
        # compute-pool - bad request
        # task - bad request, but locally works (permissions)?
        ("image-repository", {"name": "test_image_repo"}),
        # ("task", {"name": "test_task_for_testing", "definition": "select 5"}),
    ],
)
@pytest.mark.integration
def test_create(object_type, object_definition, runner, test_database):
    if object_type == "database":
        object_definition["name"] = test_database + "_test_create"
    object_name = object_definition["name"]

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
        _test_create([json.dumps(object_definition)])
    # test key=value format
    with _cleanup_object():
        list_definition = [f"{key}={value}" for key, value in object_definition.items()]
        _test_create(list_definition)
