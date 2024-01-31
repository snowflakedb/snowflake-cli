import pytest
from snowflake.cli.api.project.util import escape_like_pattern

from tests_integration.test_utils import contains_row_with, row_from_snowflake_session
from tests_integration.testing_utils.naming_utils import ObjectNameProvider

INTEGRATION_DATABASE = "SNOWCLI_DB"
INTEGRATION_SCHEMA = "PUBLIC"
INTEGRATION_REPOSITORY = "snowcli_repository"


@pytest.mark.integration
def test_list_images_tags(runner):
    _list_images(runner)
    _list_tags(runner)


def _list_images(runner):
    result = runner.invoke_with_connection_json(
        [
            "spcs",
            "image-repository",
            "list-images",
            "snowcli_repository",
            "--database",
            INTEGRATION_DATABASE,
            "--schema",
            INTEGRATION_SCHEMA,
        ]
    )
    # breakpoint()
    assert isinstance(result.json, list), result.output
    assert contains_row_with(
        result.json,
        {
            "image": f"/{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test"
        },
    )


def _list_tags(runner):
    result = runner.invoke_with_connection_json(
        [
            "spcs",
            "image-repository",
            "list-tags",
            "snowcli_repository",
            "--image_name",
            f"/{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test",
            "--database",
            INTEGRATION_DATABASE,
            "--schema",
            INTEGRATION_SCHEMA,
        ]
    )
    assert isinstance(result.json, list), result.output
    assert contains_row_with(
        result.json,
        {
            "tag": f"/{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test:1"
        },
    )


@pytest.mark.integration
def test_get_repo_url(runner, snowflake_session, test_database):
    repo_name = ObjectNameProvider("Test_Repo").create_and_get_next_object_name()
    snowflake_session.execute_string(f"create image repository {repo_name}")

    created_repo = snowflake_session.execute_string(
        f"show image repositories like '{escape_like_pattern(repo_name)}'"
    )
    created_row = row_from_snowflake_session(created_repo)[0]
    created_name = created_row["name"]
    assert created_name.lower() == repo_name.lower()

    expect_url = created_row["repository_url"]
    result = runner.invoke_with_connection(
        ["spcs", "image-repository", "url", created_name]
    )
    assert isinstance(result.output, str), result.output
    assert result.output.strip() == expect_url
