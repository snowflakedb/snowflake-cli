import json
from unittest import mock
from tests.testing_utils.fixtures import *
from snowcli.cli.snowpark.registry.manager import RegistryManager
from snowcli.cli.snowpark.registry.commands import list_images, list_tags


@mock.patch("snowcli.cli.snowpark.registry.manager.RegistryManager._conn")
@mock.patch("snowcli.cli.snowpark.registry.manager.RegistryManager._execute_query")
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
    result = runner.invoke(["snowpark", "registry", "token", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"token": "token1234", "expires_in": 42}


@mock.patch("snowcli.cli.snowpark.registry.commands.requests.get")
@mock.patch("snowcli.cli.snowpark.registry.commands.RegistryManager._execute_query")
@mock.patch("snowcli.cli.snowpark.registry.commands.RegistryManager._conn")
@mock.patch("snowcli.cli.snowpark.registry.commands.RegistryManager.login_to_registry")
def test_list_images(
    mock_login,
    mock_conn,
    mock_execute,
    mock_get_images,
    runner,
    mock_cursor,
    capsys,
):
    mock_conn.database = "DB"
    mock_conn.schema = "SCHEMA"
    mock_conn.role = "MY_ROLE"

    mock_execute.return_value = mock_cursor(
        rows=[
            [
                "2023-01-01 00:00:00",
                "IMAGES",
                "DB",
                "SCHEMA",
                "orgname-alias.registry.snowflakecomputing.com/DB/SCHEMA/IMAGES",
                "ROLE",
                "ROLE",
                "",
            ]
        ],
        columns=[
            "date",
            "name",
            "db",
            "schema",
            "registry",
            "role",
            "unknown",
            "unkown2",
        ],
    )
    mock_login.return_value = "TOKEN"

    mock_response = mock.MagicMock()
    mock_get_images.return_value.status_code = 200
    mock_get_images.return_value.text = '{"repositories":["baserepo/super-cool-repo"]}'

    list_images(repo_name="IMAGES")
    captured = capsys.readouterr()
    assert "DB/SCHEMA/IMAGES/super-cool-repo" in captured.out


@mock.patch("snowcli.cli.snowpark.registry.commands.requests.get")
@mock.patch("snowcli.cli.snowpark.registry.manager.RegistryManager._execute_query")
@mock.patch("snowcli.cli.snowpark.registry.commands.RegistryManager._conn")
@mock.patch("snowcli.cli.snowpark.registry.manager.RegistryManager.login_to_registry")
def test_list_tags(
    mock_login,
    mock_conn,
    mock_execute,
    mock_get_tags,
    runner,
    mock_cursor,
    capsys,
):
    mock_conn.database = "DB"
    mock_conn.schema = "SCHEMA"
    mock_conn.role = "MY_ROLE"

    mock_execute.return_value = mock_cursor(
        rows=[
            [
                "2023-01-01 00:00:00",
                "IMAGES",
                "DB",
                "SCHEMA",
                "orgname-alias.registry.snowflakecomputing.com/DB/SCHEMA/IMAGES",
                "ROLE",
                "ROLE",
                "",
            ]
        ],
        columns=[
            "date",
            "name",
            "db",
            "schema",
            "registry",
            "role",
            "unknown",
            "unkown2",
        ],
    )
    mock_login.return_value = "TOKEN"

    mock_response = mock.MagicMock()
    mock_get_tags.return_value.status_code = 200
    mock_get_tags.return_value.text = (
        '{"name":"baserepo/super-cool-repo","tags":["1.2.0"]}'
    )

    list_tags(repo_name="IMAGES", image_name="DB/SCHEMA/IMAGES/super-cool-repo")
    captured = capsys.readouterr()
    assert "DB/SCHEMA/IMAGES/super-cool-repo" in captured.out
