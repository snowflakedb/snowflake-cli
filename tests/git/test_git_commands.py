from pathlib import Path
from unittest import mock

import pytest
from snowflake.connector import ProgrammingError

EXAMPLE_URL = "https://github.com/an-example-repo.git"


@pytest.mark.skip(reason="Command is hidden")
def test_toplevel_help(runner):
    result = runner.invoke(["--help"])
    assert (
        result.exit_code == 0
        and "Manages git repositories in Snowflake." in result.output
    )

    result = runner.invoke(["git", "--help"])
    assert result.exit_code == 0, result.output


@mock.patch("snowflake.connector.connect")
def test_list_branches(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-branches", "repo_name"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show git branches in repo_name"


@mock.patch("snowflake.connector.connect")
def test_list_tags(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-tags", "repo_name"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "show git tags in repo_name"


@mock.patch("snowflake.connector.connect")
def test_list_files(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-files", "@repo_name/branches/main"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "ls @repo_name/branches/main"


def test_list_files_not_a_stage_error(runner):
    result = runner.invoke(["git", "list-files", "repo_name/branches/main"])
    assert result.exit_code == 1
    _assert_invalid_repo_path_error_message(result.output)


@mock.patch("snowflake.connector.connect")
def test_fetch(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "fetch", "repo_name"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "alter git repository repo_name fetch"


@mock.patch("snowflake.connector.connect")
def test_copy_to_local_file_system(mock_connector, runner, mock_ctx, temp_dir):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    local_path = Path(temp_dir) / "local_dir"
    assert not local_path.exists()
    result = runner.invoke(["git", "copy", "@repo_name/branches/main", str(local_path)])

    assert result.exit_code == 0, result.output
    assert local_path.exists()
    assert (
        ctx.get_query()
        == f"get @repo_name/branches/main file://{local_path.resolve()}/ parallel=4"
    )


@mock.patch("snowflake.connector.connect")
def test_copy_to_remote_dir(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        ["git", "copy", "@repo_name/branches/main", "@stage_path/dir_in_stage"]
    )

    assert result.exit_code == 0, result.output
    assert (
        ctx.get_query()
        == "copy files into @stage_path/dir_in_stage/ from @repo_name/branches/main"
    )


def test_copy_not_a_stage_error(runner):
    result = runner.invoke(["git", "copy", "repo_name", "@stage_path/dir_in_stage"])
    assert result.exit_code == 1
    _assert_invalid_repo_path_error_message(result.output)


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager.describe")
def test_setup_already_exists_error(mock_om_describe, mock_connector, runner, mock_ctx):
    mock_om_describe.return_value = {"object_details": "something"}
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    result = runner.invoke(["git", "setup", "repo_name"])
    assert result.exit_code == 1, result.output
    assert "Error" in result.output
    assert "Repository 'repo_name' already exists" in result.output


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager.describe")
def test_setup_no_secret_existing_api(
    mock_om_describe, mock_connector, runner, mock_ctx
):
    mock_om_describe.side_effect = ProgrammingError("does not exist or not authorized")
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join([EXAMPLE_URL, "n", "y", "existing_api_integration", ""])
    result = runner.invoke(["git", "setup", "repo_name"], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: n",
                "Use existing api integration? [y/N]: y",
                "API integration identifier: existing_api_integration",
            ]
        )
    )
    assert ctx.get_query() == (
        "create git repository repo_name"
        " api_integration = existing_api_integration"
        " origin = 'https://github.com/an-example-repo.git'"
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager.describe")
def test_setup_no_secret_create_api(mock_om_describe, mock_connector, runner, mock_ctx):
    mock_om_describe.side_effect = ProgrammingError("does not exist or not authorized")
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join([EXAMPLE_URL, "n", "n", "existing_api_integration", ""])
    result = runner.invoke(["git", "setup", "repo_name"], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: n",
                "Use existing api integration? [y/N]: n",
                "API integration 'repo_name_api_integration' successfully created.",
            ]
        )
    )
    assert ctx.get_query() == (
        "create api integration repo_name_api_integration"
        " api_provider = git_https_api"
        " api_allowed_prefixes = ('https://github.com/an-example-repo.git')"
        " allowed_authentication_secrets = ()"
        " enabled = true"
        "\n"
        "create git repository repo_name"
        " api_integration = repo_name_api_integration"
        " origin = 'https://github.com/an-example-repo.git'"
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager.describe")
def test_setup_existing_secret_existing_api(
    mock_om_describe, mock_connector, runner, mock_ctx
):
    mock_om_describe.side_effect = ProgrammingError("does not exist or not authorized")
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join(
        [EXAMPLE_URL, "y", "y", "existing_secret", "y", "existing_api_integration", ""]
    )
    result = runner.invoke(["git", "setup", "repo_name"], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: y",
                "Use existing secret? [y/N]: y",
                "Secret identifier: existing_secret",
                "Use existing api integration? [y/N]: y",
                "API integration identifier: existing_api_integration",
            ]
        )
    )
    assert ctx.get_query() == (
        "create git repository repo_name"
        " api_integration = existing_api_integration"
        " origin = 'https://github.com/an-example-repo.git'"
        " git_credentials = existing_secret"
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager.describe")
def test_setup_existing_secret_create_api(
    mock_om_describe, mock_connector, runner, mock_ctx
):
    mock_om_describe.side_effect = ProgrammingError("does not exist or not authorized")
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join([EXAMPLE_URL, "y", "y", "existing_secret", "n", ""])
    result = runner.invoke(["git", "setup", "repo_name"], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: y",
                "Use existing secret? [y/N]: y",
                "Secret identifier: existing_secret",
                "Use existing api integration? [y/N]: n",
                "API integration 'repo_name_api_integration' successfully created.",
            ]
        )
    )
    assert ctx.get_query() == (
        "create api integration repo_name_api_integration"
        " api_provider = git_https_api"
        " api_allowed_prefixes = ('https://github.com/an-example-repo.git')"
        " allowed_authentication_secrets = (existing_secret)"
        " enabled = true"
        "\n"
        "create git repository repo_name"
        " api_integration = repo_name_api_integration"
        " origin = 'https://github.com/an-example-repo.git'"
        " git_credentials = existing_secret"
    )


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.snowpark.commands.ObjectManager.describe")
def test_setup_create_secret_create_api(
    mock_om_describe, mock_connector, runner, mock_ctx
):
    mock_om_describe.side_effect = ProgrammingError("does not exist or not authorized")
    ctx = mock_ctx()
    mock_connector.return_value = ctx

    communication = "\n".join([EXAMPLE_URL, "y", "n", "john_doe", "admin123", "n", ""])
    result = runner.invoke(["git", "setup", "repo_name"], input=communication)

    assert result.exit_code == 0, result.output
    assert result.output.startswith(
        "\n".join(
            [
                "Origin url: https://github.com/an-example-repo.git",
                "Use secret for authentication? [y/N]: y",
                "Use existing secret? [y/N]: n",
                "Creating new secret",
                "username: john_doe",
                "password/token: ",
                "Secret 'repo_name_secret' successfully created",
                "Use existing api integration? [y/N]: n",
                "API integration 'repo_name_api_integration' successfully created.",
            ]
        )
    )
    assert ctx.get_query() == (
        "create secret repo_name_secret"
        " type = password"
        " username = 'john_doe'"
        " password = 'admin123'"
        "\n"
        "create api integration repo_name_api_integration"
        " api_provider = git_https_api"
        " api_allowed_prefixes = ('https://github.com/an-example-repo.git')"
        " allowed_authentication_secrets = (repo_name_secret)"
        " enabled = true"
        "\n"
        "create git repository repo_name"
        " api_integration = repo_name_api_integration"
        " origin = 'https://github.com/an-example-repo.git'"
        " git_credentials = repo_name_secret"
    )


def _assert_invalid_repo_path_error_message(output):
    assert "Error" in output
    assert (
        "REPOSITORY_PATH should be a path to git repository stage with scope" in output
    )
    assert "provided. For example: @my_repo/branches/main/" in output
