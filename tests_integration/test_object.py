import pytest

from tests_integration.test_utils import row_from_cursor
from tests_integration.testing_utils.naming_utils import ObjectNameProvider


@pytest.mark.integration
@pytest.mark.parametrize("object_type", ["warehouse", "schema"])
def test_show(object_type, runner, test_database, snowflake_session):
    result = runner.invoke_with_connection_json(
        ["object", "list", object_type, "--format", "json"]
    )

    curr = snowflake_session.execute_string(f"show {object_type}s")
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
