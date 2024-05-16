from unittest import mock

import typer
from snowflake.cli.plugins.notebook.manager import NotebookManager


@mock.patch.object(NotebookManager, "execute")
def test_execute(mock_execute, runner):
    result = runner.invoke(["notebook", "execute", "my_notebook"])

    assert result.exit_code == 0, result.output
    assert result.output == "Notebook my_notebook executed.\n"
    mock_execute.assert_called_once_with(notebook_name="my_notebook")


@mock.patch.object(NotebookManager, "get_url")
def test_get_url(mock_url, runner):
    mock_url.return_value = "http://my.url"
    result = runner.invoke(["notebook", "get-url", "my_notebook"])

    assert result.exit_code == 0, result.output
    assert result.output == "http://my.url\n"
    mock_url.assert_called_once_with(notebook_name="my_notebook")


@mock.patch.object(NotebookManager, "get_url")
@mock.patch.object(typer, "launch")
def test_open(mock_launch, mock_url, runner):
    mock_url.return_value = "http://my.url"
    result = runner.invoke(["notebook", "open", "my_notebook"])

    assert result.exit_code == 0, result.output
    assert result.output == "http://my.url\n"
    mock_url.assert_called_once_with(notebook_name="my_notebook")
    mock_launch.assert_called_once_with("http://my.url")


@mock.patch.object(NotebookManager, "create")
def test_create(mock_create, runner):
    notebook_name = "my_notebook"
    notebook_file = "@stage/notebook.ipynb"

    result = runner.invoke(
        ("notebook", "create", notebook_name, "--notebook-file", notebook_file)
    )
    assert result.exit_code == 0, result.output

    mock_create.assert_called_once_with(
        notebook_name=notebook_name,
        notebook_file=notebook_file,
    )
