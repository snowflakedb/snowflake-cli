from unittest import mock

import pytest


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
