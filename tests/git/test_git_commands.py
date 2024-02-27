from pathlib import Path
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


@mock.patch("snowflake.connector.connect")
def test_fetch(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "fetch", "repo_name"])

    assert result.exit_code == 0, result.output
    assert ctx.get_query() == "alter git repository repo_name fetch"


@mock.patch("snowflake.connector.connect")
def test_copy(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    local_path = Path("local/path")
    result = runner.invoke(["git", "copy", "@repo_name/branches/main", str(local_path)])

    assert result.exit_code == 0, result.output
    assert (
        ctx.get_query()
        == f"get @repo_name/branches/main file://{local_path.resolve()}/ parallel=4"
    )

    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(
        ["git", "copy", "@repo_name/branches/main", "@stage_path/dir_in_stage"]
    )

    # paths in generated SQL should end with '/'
    assert result.exit_code == 0, result.output
    assert (
        ctx.get_query()
        == "copy files into @stage_path/dir_in_stage/ from @repo_name/branches/main/"
    )
