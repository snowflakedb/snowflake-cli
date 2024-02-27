import json
from subprocess import PIPE, CalledProcessError
from unittest import mock

import pytest
from click import ClickException
from snowflake.cli.plugins.spcs.image_registry.manager import (
    NoImageRepositoriesFoundError,
    RegistryManager,
)
from snowflake.connector.cursor import DictCursor


@mock.patch("snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._conn")
@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._execute_query"
)
def test_registry_get_token(mock_execute, mock_conn, mock_cursor, runner):
    mock_execute.return_value = mock_cursor(
        ["row"], ["Statement executed successfully"]
    )
    mock_conn._rest._token_request.return_value = {  # noqa: SLF001
        "data": {
            "sessionToken": "token1234",
            "validityInSecondsST": 42,
        }
    }
    result = runner.invoke(["spcs", "image-registry", "token", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"token": "token1234", "expires_in": 42}


MOCK_REPO_COLUMNS = [
    "created_on",
    "name",
    "database_name",
    "schema_name",
    "repository_url",
    "owner",
    "owner_role_type",
    "comment",
]


@mock.patch("snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._conn")
@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager._execute_query"
)
def test_get_registry_url(mock_execute, mock_conn, mock_cursor):
    mock_row = [
        "2023-01-01 00:00:00",
        "IMAGES",
        "DB",
        "SCHEMA",
        "orgname-alias.registry.snowflakecomputing.com/DB/SCHEMA/IMAGES",
        "TEST_ROLE",
        "ROLE",
        "",
    ]

    mock_execute.return_value = mock_cursor(
        rows=[{col: row for col, row in zip(MOCK_REPO_COLUMNS, mock_row)}],
        columns=MOCK_REPO_COLUMNS,
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
        columns=MOCK_REPO_COLUMNS,
    )
    with pytest.raises(NoImageRepositoriesFoundError):
        RegistryManager().get_registry_url()

    expected_query = "show image repositories in account"
    mock_execute.assert_called_once_with(expected_query, cursor_class=DictCursor)


@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager.get_registry_url"
)
def test_get_registry_url_no_repositories_cli(mock_get_registry_url, runner, snapshot):
    mock_get_registry_url.side_effect = NoImageRepositoriesFoundError()
    result = runner.invoke(["spcs", "image-registry", "url"])
    assert result.exit_code == 1, result.output
    assert result.output == snapshot


@pytest.mark.parametrize(
    "url, expected",
    [
        ("www.google.com", False),
        ("https://www.google.com", True),
        ("//www.google.com", True),
        ("snowservices.registry.snowflakecomputing.com/db/schema/tutorial_repo", False),
        (
            "http://snowservices.registry.snowflakecomputing.com/db/schema/tutorial_repo",
            True,
        ),
    ],
)
def test_has_url_scheme(url: str, expected: bool):
    assert RegistryManager()._has_url_scheme(url) == expected  # noqa: SLF001


@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager.get_token"
)
@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager.get_registry_url"
)
@mock.patch("snowflake.cli.plugins.spcs.image_registry.manager.subprocess.check_output")
def test_docker_registry_login(mock_check_output, mock_get_url, mock_get_token):
    test_output = mock_check_output.return_value = "Login Succeeded\n"
    test_url = (
        mock_get_url.return_value
    ) = "orgname-acctname.registry.snowflakecomputing.com"
    test_token = mock_get_token.return_value = {
        "token": "ver:1-hint:abc",
        "expires_in": 3600,
    }

    expected_command = [
        "docker",
        "login",
        "--username",
        "0sessiontoken",
        "--password-stdin",
        test_url,
    ]

    result = RegistryManager().docker_registry_login()
    mock_check_output.assert_called_once_with(
        expected_command, input=json.dumps(test_token), text=True, stderr=PIPE
    )
    mock_get_url.assert_called_once_with()
    mock_get_token.assert_called_once_with()
    assert result == test_output


@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager.docker_registry_login"
)
def test_docker_registry_login_cli(mock_docker_login, runner, snapshot):
    mock_docker_login.return_value = "Login Succeeded\n"
    result = runner.invoke(["spcs", "image-registry", "login"])
    assert result.exit_code == 0, result.output
    assert result.output == snapshot


@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager.get_registry_url"
)
def test_docker_registry_login_cli_no_repositories(
    mock_get_registry_url, runner, snapshot
):
    mock_get_registry_url.side_effect = NoImageRepositoriesFoundError()
    result = runner.invoke(["spcs", "image-registry", "login"])
    assert result.exit_code == 1, result.output
    assert result.output == snapshot


@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager.get_token"
)
@mock.patch(
    "snowflake.cli.plugins.spcs.image_registry.manager.RegistryManager.get_registry_url"
)
@mock.patch("snowflake.cli.plugins.spcs.image_registry.manager.subprocess.check_output")
def test_docker_registry_login_subprocess_error(
    mock_check_output, mock_get_url, mock_get_token, snapshot
):
    test_url = (
        mock_get_url.return_value
    ) = "orgname-acctname.registry.snowflakecomputing.com"
    mock_get_token.return_value = {
        "token": "ver:1-hint:abc",
        "expires_in": 3600,
    }

    test_command = [
        "docker",
        "login",
        "--username",
        "0sessiontoken",
        "--password-stdin",
        test_url,
    ]

    mock_check_output.side_effect = CalledProcessError(
        returncode=1, cmd=test_command, stderr="Docker Failed"
    )
    with pytest.raises(ClickException) as e:
        RegistryManager().docker_registry_login()

    assert e.value.message == snapshot
