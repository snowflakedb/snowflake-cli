# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from unittest import mock

import pytest
import typer
from snowflake.cli._plugins.notebook.manager import NotebookManager
from snowflake.cli.api.identifiers import FQN


@mock.patch.object(NotebookManager, "execute")
def test_execute(mock_execute, runner):
    result = runner.invoke(["notebook", "execute", "my_notebook"])

    assert result.exit_code == 0, result.output
    assert result.output == "Notebook my_notebook executed.\n"
    mock_execute.assert_called_once_with(notebook_name=FQN.from_string("my_notebook"))


@mock.patch.object(NotebookManager, "get_url")
def test_get_url(mock_url, runner):
    mock_url.return_value = "http://my.url"
    result = runner.invoke(["notebook", "get-url", "my_notebook"])

    assert result.exit_code == 0, result.output
    assert result.output == "http://my.url\n"
    mock_url.assert_called_once_with(notebook_name=FQN.from_string("my_notebook"))


@mock.patch.object(NotebookManager, "get_url")
@mock.patch.object(typer, "launch")
def test_open(mock_launch, mock_url, runner):
    mock_url.return_value = "http://my.url"
    result = runner.invoke(["notebook", "open", "my_notebook"])

    assert result.exit_code == 0, result.output
    assert result.output == "http://my.url\n"
    mock_url.assert_called_once_with(notebook_name=FQN.from_string("my_notebook"))
    mock_launch.assert_called_once_with("http://my.url")


@mock.patch.object(NotebookManager, "create")
def test_create(mock_create, runner):
    notebook_name = "my_notebook"
    notebook_file = "@stage/notebook.ipynb"
    mock_create.return_value = "created"

    result = runner.invoke(
        ("notebook", "create", notebook_name, "--notebook-file", notebook_file)
    )
    assert result.exit_code == 0, result.output

    mock_create.assert_called_once_with(
        notebook_name=FQN.from_string("my_notebook"),
        notebook_file=notebook_file,
    )


@pytest.mark.parametrize(
    "stage_path",
    ["@db.schema.stage", "@stage/dir/subdir", "@git_repo_stage/branch/main"],
)
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.notebook.manager.make_snowsight_url")
def test_create_query(
    mock_make_snowsight_url, mock_connector, mock_ctx, runner, stage_path
):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_make_snowsight_url.return_value = "mocked_snowsight.url"
    notebook_name = "my_notebook"
    notebook_file = f"{stage_path}/notebook.ipynb"
    result = runner.invoke(
        ["notebook", "create", notebook_name, "--notebook-file", notebook_file]
    )
    assert result.exit_code == 0, result.output
    assert ctx.get_query() == (
        "\n"
        "CREATE OR REPLACE NOTEBOOK "
        f"IDENTIFIER('MockDatabase.MockSchema.{notebook_name}')\n"
        f"FROM '{stage_path}'\n"
        "QUERY_WAREHOUSE = 'MockWarehouse'\n"
        "MAIN_FILE = 'notebook.ipynb';\n"
        "// Cannot use IDENTIFIER(...)\n"
        f"ALTER NOTEBOOK MockDatabase.MockSchema.{notebook_name} ADD LIVE VERSION FROM LAST;\n"
    )


def test_deploy(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-tags", "repo_name", "--like", "PATTERN"])
    assert ctx.get_query == "q"


def test_deploy_single_notebook(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-tags", "repo_name", "--like", "PATTERN"])
    assert ctx.get_query == "q"


def test_deploy_object_exists_error(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-tags", "repo_name", "--like", "PATTERN"])
    assert ctx.get_query == "q"


def test_deploy_file_validation_error(mock_connector, runner, mock_ctx):
    # file not exists, not-ipynb file
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-tags", "repo_name", "--like", "PATTERN"])
    assert ctx.get_query == "q"


def test_deploy_replace(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-tags", "repo_name", "--like", "PATTERN"])
    assert ctx.get_query == "q"


def test_deploy_if_not_exists(mock_connector, runner, mock_ctx):
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    result = runner.invoke(["git", "list-tags", "repo_name", "--like", "PATTERN"])
    assert ctx.get_query == "q"
