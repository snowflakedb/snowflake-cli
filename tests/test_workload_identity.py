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


# Test removed as create-user command is being dropped


def test_workload_identity_provider_visible_in_help(runner):
    """Test that --workload-identity-provider appears in help messages for commands with requires_connection=True."""
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
