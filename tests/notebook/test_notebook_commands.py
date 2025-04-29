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

STAGE_MANAGER = "snowflake.cli._plugins.stage.manager.StageManager"


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


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.notebook.notebook_entity.make_snowsight_url")
@mock.patch(f"{STAGE_MANAGER}.list_files")
@pytest.mark.parametrize("notebook_id", ["notebook1", "notebook2"])
def test_deploy_default_stage_paths(
    mock_list_files,
    mock_make_url,
    mock_connector,
    mock_ctx,
    runner,
    project_directory,
    snapshot,
    notebook_id,
):
    """Deploy two different notebooks with the same notebook file name."""
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_make_url.return_value = "http://the.notebook.url.mock"
    with project_directory("notebooks_multiple_v2") as project_path:
        result = runner.invoke(["notebook", "deploy", notebook_id, "--replace"])
        assert result.exit_code == 0, result.output
        assert f"Uploading artifacts to @notebooks/{notebook_id}" in result.output
        assert (
            "Notebook successfully deployed and available under http://the.notebook.url.mock"
            in result.output
        )
        query = "\n".join(
            line for line in ctx.get_query().split("\n") if not line.startswith("put")
        )
        assert query == snapshot(name="query")
        assert not (project_path / "output").exists()


@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli._plugins.notebook.notebook_entity.make_snowsight_url")
@mock.patch(f"{STAGE_MANAGER}.list_files")
@pytest.mark.parametrize("project_name", ["notebook_v2", "notebook_containerized_v2"])
def test_deploy_single_notebook(
    mock_list_files,
    mock_make_url,
    mock_connector,
    mock_ctx,
    runner,
    project_directory,
    project_name,
    snapshot,
):
    """Deploy single notebook with custom identifier and stage path."""
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    mock_make_url.return_value = "http://the.notebook.url.mock"
    with project_directory(project_name) as project_root:
        result = runner.invoke(["notebook", "deploy", "--replace"])
        assert result.exit_code == 0, result.output
        assert (
            "Notebook successfully deployed and available under http://the.notebook.url.mock"
            in result.output
        )
        query = "\n".join(
            line for line in ctx.get_query().split("\n") if not line.startswith("put")
        )
        assert query == snapshot(name="query")
        assert not (project_root / "output").exists()


@mock.patch("snowflake.connector.connect")
@mock.patch(f"{STAGE_MANAGER}.list_files")
def test_deploy_no_replace_error(
    mock_list_files, mock_connector, mock_ctx, runner, project_directory
):
    """Deploy two different notebooks with the same notebook file name."""
    ctx = mock_ctx()
    mock_connector.return_value = ctx
    with project_directory("notebook_v2"):
        result = runner.invoke(["notebook", "deploy"])
        assert result.exit_code == 1, result.output
        assert (
            "Notebook custom_identifier already exists. Consider using --replace."
            in result.output
        )


def test_deploy_notebook_file_not_exists_error(runner, project_directory):
    with project_directory("notebooks_multiple_v2") as project_root:
        (project_root / "notebook2" / "my_notebook.ipynb").unlink()
        result = runner.invoke(["notebook", "deploy", "notebook2", "--replace"])
        assert result.exit_code == 1, result.output
        assert "This caused: Value error, Notebook file"
        assert "notebook2/my_notebook.ipynb does not exist" in result.output.replace(
            "\\", "/"
        )


def test_deploy_notebook_definition_not_exists_error(runner, project_directory):
    with project_directory("notebook_v2"):
        result = runner.invoke(["notebook", "deploy", "not_existing_id", "--replace"])
        assert result.exit_code == 2, result.output
        assert (
            "Definition of notebook 'not_existing_id' not found in project definition"
        )
        assert "file." in result.output


def test_deploy_notebook_multiple_definitions(runner, project_directory):
    with project_directory("notebooks_multiple_v2"):
        result = runner.invoke(["notebook", "deploy", "--replace"])
        assert result.exit_code == 2, result.output
        assert (
            "Multiple entities of type notebook found. Please provide entity id for the"
        )
        assert "operation." in result.output


def test_deploy_project_definition_version_error(
    runner, project_directory, alter_snowflake_yml
):

    with project_directory("empty_project") as project_root:
        alter_snowflake_yml(project_root / "snowflake.yml", "definition_version", "1.1")
        result = runner.invoke(["notebook", "deploy", "--replace"])
        assert result.exit_code == 2, result.output
        assert (
            "This command requires project definition of version at least 2."
            in result.output
        )
