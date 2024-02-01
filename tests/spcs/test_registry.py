import json
from tests.testing_utils.fixtures import *
from snowflake.cli.plugins.spcs.image_registry.manager import (
    RegistryManager,
    NoRepositoriesViewableError,
)
from snowflake.connector.cursor import DictCursor


@mock.patch("snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._conn")
@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._execute_query"
)
def test_registry_get_token_2(mock_execute, mock_conn, mock_cursor, runner):
    mock_execute.return_value = mock_cursor(
        ["row"], ["Statement executed successfully"]
    )
    mock_conn._rest._token_request.return_value = {
        "data": {
            "sessionToken": "token1234",
            "validityInSecondsST": 42,
        }
    }
    result = runner.invoke(["spcs", "image-registry", "token", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"token": "token1234", "expires_in": 42}


@mock.patch("snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._conn")
@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._execute_query"
)
def test_get_registry_url(mock_execute, mock_conn, mock_cursor):
    mock_rows = [
        "2023-01-01 00:00:00",
        "IMAGES",
        "DB",
        "SCHEMA",
        "orgname-alias.registry.snowflakecomputing.com/DB/SCHEMA/IMAGES",
        "TEST_ROLE",
        "ROLE",
        "",
    ]
    mock_columns = [
        "created_on",
        "name",
        "database_name",
        "schema_name",
        "repository_url",
        "owner",
        "owner_role_type",
        "comment",
    ]
    mock_execute.return_value = mock_cursor(
        rows=[{col: row for col, row in zip(mock_columns, mock_rows)}],
        columns=mock_columns,
    )
    result = RegistryManager().get_registry_url()
    expected_query = "show image repositories in account"
    mock_execute.assert_called_once_with(expected_query, cursor_class=DictCursor)
    assert result == "orgname-alias.registry.snowflakecomputing.com"


@mock.patch("snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._conn")
@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._execute_query"
)
def test_get_registry_url_no_repositories(mock_execute, mock_conn, mock_cursor):
    mock_execute.return_value = mock_cursor(
        rows=[],
        columns=[
            "created_on",
            "name",
            "database_name",
            "schema_name",
            "repository_url",
            "owner",
            "owner_role_type",
            "comment",
        ],
    )
    with pytest.raises(NoRepositoriesViewableError):
        RegistryManager().get_registry_url()

    expected_query = "show image repositories in account"
    mock_execute.assert_called_once_with(expected_query, cursor_class=DictCursor)


@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager.get_registry_url"
)
def test_get_registry_url_no_repositories_cli(mock_get_registry_url, runner, snapshot):
    mock_get_registry_url.side_effect = NoRepositoriesViewableError()
    result = runner.invoke(["spcs", "image-registry", "url"])
    assert result.exit_code == 1, result.output
    assert result.output == snapshot
