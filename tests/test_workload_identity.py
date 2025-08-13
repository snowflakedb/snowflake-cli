from textwrap import dedent
from unittest import mock


@mock.patch("snowflake.connector.connect")
def test_workload_identity_provider_parameter(mock_connect, runner):
    """Test that --workload-identity-provider parameter is recognized and passed to connection."""
    runner.invoke(
        [
            "sql",
            "-q",
            "select 1",
            "-x",  # temporary connection
            "--workload-identity-provider",
            "AWS",
            "--account",
            "test_account",
            "--user",
            "test_user",
        ]
    )

    mock_connect.assert_called_once_with(
        application="SNOWCLI.SQL",
        application_name="snowcli",
        account="test_account",
        user="test_user",
        workload_identity_provider="AWS",
        using_session_keep_alive=True,
    )


@mock.patch("snowflake.connector.connect")
def test_workload_identity_provider_in_auth_oidc_command(mock_connect, runner):
    """Test that workload_identity_provider is available in auth oidc create-user command."""
    # Mock the execute_query method to avoid actual database calls
    with mock.patch(
        "snowflake.cli._plugins.auth.oidc.manager.OidcManager.execute_query"
    ):
        result = runner.invoke(
            [
                "auth",
                "oidc",
                "create-user",
                "--user-name",
                "test_user",
                "--subject",
                "repo:owner/repo:environment:prod",
                "--issuer",
                "https://token.actions.githubusercontent.com",
                "-x",  # temporary connection
                "--workload-identity-provider",
                "AWS",
                "--account",
                "test_account",
                "--user",
                "connection_user",
            ],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    # Verify the connection was called with workload_identity_provider
    mock_connect.assert_called_once()
    call_kwargs = mock_connect.call_args[1]
    assert call_kwargs["workload_identity_provider"] == "AWS"
    assert call_kwargs["account"] == "test_account"
    assert call_kwargs["user"] == "connection_user"


def test_workload_identity_provider_visible_in_help(runner):
    """Test that --workload-identity-provider appears in help messages for commands with requires_connection=True."""
    # Test auth oidc create-user command help
    result = runner.invoke(["auth", "oidc", "create-user", "--help"])
    assert result.exit_code == 0, result.output
    assert "--workload-identity-provider" in result.output
    assert "Connection configuration" in result.output

    # Test sql command help
    result = runner.invoke(["sql", "--help"])
    assert result.exit_code == 0, result.output
    assert "--workload-identity-provider" in result.output
    assert "Connection configuration" in result.output


@mock.patch("snowflake.connector.connect")
def test_workload_identity_provider_from_config(mock_connect, runner, config_file):
    """Test that workload_identity_provider works when specified in config file."""
    config = dedent(
        """\
    default_connection_name = "test"
    
    [connections.test]
    account = "test_account"
    user = "test_user" 
    workload_identity_provider = "AZURE"
    """
    )

    with config_file(config) as config_file:
        runner.invoke_with_config_file(config_file, ["sql", "-q", "select 1"])

    mock_connect.assert_called_once_with(
        application="SNOWCLI.SQL",
        application_name="snowcli",
        account="test_account",
        user="test_user",
        workload_identity_provider="AZURE",
        using_session_keep_alive=True,
    )


def test_workload_identity_provider_invalid_value(runner):
    """Test that invalid workload identity provider values are rejected."""
    result = runner.invoke(
        [
            "sql",
            "-q",
            "select 1",
            "-x",  # temporary connection
            "--workload-identity-provider",
            "INVALID_VALUE",
            "--account",
            "test_account",
            "--user",
            "test_user",
        ]
    )

    assert result.exit_code != 0
    assert "Invalid workload identity provider 'INVALID_VALUE'" in result.output
    # Check that all valid values are mentioned (they may be on different lines due to formatting)
    assert "AWS" in result.output
    assert "AZURE" in result.output
    assert "GCP" in result.output
    assert "OIDC" in result.output
