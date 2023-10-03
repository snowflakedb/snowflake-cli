import json
from unittest import mock
from tests.testing_utils.fixtures import *
from snowcli.cli.registry.manager import RegistryManager
from snowcli.cli.registry.commands import list_images, list_tags


@mock.patch("snowcli.cli.registry.manager.RegistryManager._execute_query")
def test_registry_get_token(mock_execute, mock_cursor):
    mock_execute.return_value = mock_cursor(
        ["row"], ["Statement executed successfully"]
    )
    manager = RegistryManager()
    result = manager.get_token()
    assert "token" in result
    assert "expires_in" in result


@mock.patch("snowcli.cli.registry.commands.requests.get")
@mock.patch("snowcli.cli.registry.manager.RegistryManager._execute_query")
@mock.patch("snowcli.cli.registry.manager.RegistryManager.get_schema")
@mock.patch("snowcli.cli.registry.manager.RegistryManager.get_database")
@mock.patch("snowcli.cli.registry.manager.RegistryManager.login_to_registry")
def test_list_images(
    mock_login,
    mock_database,
    mock_schema,
    mock_execute,
    mock_get_images,
    runner,
    mock_cursor,
    capsys,
):
    mock_database.return_value = "DB"
    mock_schema.return_value = "SCHEMA"
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
    print(captured.out)
    assert "DB/SCHEMA/IMAGES/super-cool-repo" in captured.out


@mock.patch("snowcli.cli.registry.commands.requests.get")
@mock.patch("snowcli.cli.registry.manager.RegistryManager._execute_query")
@mock.patch("snowcli.cli.registry.manager.RegistryManager.get_schema")
@mock.patch("snowcli.cli.registry.manager.RegistryManager.get_database")
@mock.patch("snowcli.cli.registry.manager.RegistryManager.login_to_registry")
def test_list_tags(
    mock_login,
    mock_database,
    mock_schema,
    mock_execute,
    mock_get_tags,
    runner,
    mock_cursor,
    capsys,
):
    mock_database.return_value = "DB"
    mock_schema.return_value = "SCHEMA"
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
    print(captured.out)
    assert "DB/SCHEMA/IMAGES/super-cool-repo" in captured.out
