import pytest

from tests_integration.test_utils import contains_row_with, row_from_snowflake_session

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
def test_get_repo_url(runner, snowflake_session):
    # requires at least one repository to exist in the schema of snowflake_session
    expect = snowflake_session.execute_string(f"show image repositories")
    expect_row = row_from_snowflake_session(expect)[0]
    expect_name = expect_row["name"]
    expect_url = expect_row["repository_url"]

    result = runner.invoke_with_connection(
        ["spcs", "image-repository", "url", expect_name]
    )
    assert isinstance(result.output, str), result.output
    assert result.output.strip() == expect_url
