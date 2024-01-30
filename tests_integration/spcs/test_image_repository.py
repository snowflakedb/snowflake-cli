import pytest

from tests_integration.test_utils import contains_row_with

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
            "image": f"{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test"
        },
    )


@pytest.mark.integration
def _list_tags(runner):
    result = runner.invoke_with_connection_json(
        [
            "spcs",
            "image-repository",
            "list-tags",
            "snowcli_repository",
            "--image_name",
            f"{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test",
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
            "tag": f"{INTEGRATION_DATABASE}/{INTEGRATION_SCHEMA}/{INTEGRATION_REPOSITORY}/snowpark_test:1"
        },
    )
