from textwrap import dedent
from unittest import mock
from unittest.mock import MagicMock, PropertyMock

import pytest
from snowflake.cli.plugins.notebook.exceptions import NotebookStagePathError
from snowflake.cli.plugins.notebook.manager import NotebookManager


@mock.patch.object(NotebookManager, "_execute_query")
def test_execute(mock_execute):
    _ = NotebookManager().execute(notebook_name="MY_NOTEBOOK")
    mock_execute.assert_called_once_with(query="EXECUTE NOTEBOOK MY_NOTEBOOK()")


@mock.patch("snowflake.cli.plugins.notebook.manager.make_snowsight_url")
def test_get_url(mock_url):
    mock_url.return_value = "my_url"
    conn_mock = MagicMock(database="nb_database", schema="nb_schema")
    with mock.patch.object(NotebookManager, "_conn", conn_mock):
        result = NotebookManager().get_url(notebook_name="MY_NOTEBOOK")

    assert result == "my_url"
    mock_url.assert_called_once_with(
        conn_mock, f"/#/notebooks/NB_DATABASE.NB_SCHEMA.MY_NOTEBOOK"
    )


@mock.patch.object(NotebookManager, "_execute_queries")
@mock.patch("snowflake.cli.plugins.notebook.manager.cli_context")
def test_create(mock_ctx, mock_execute):
    type(mock_ctx.connection).warehouse = PropertyMock(return_value="MY_WH")

    _ = NotebookManager().create(
        notebook_name="my_notebook",
        notebook_file="@stage/nb file.ipynb",
    )
    expected_query = dedent(
        """
        CREATE OR REPLACE NOTEBOOK my_notebook
        FROM '@stage'
        QUERY_WAREHOUSE = 'MY_WH'
        MAIN_FILE = 'nb file.ipynb';

        ALTER NOTEBOOK my_notebook ADD LIVE VERSION FROM LAST;
        """
    )
    mock_execute.assert_called_once_with(queries=expected_query)


@pytest.mark.parametrize(
    "stage_path",
    (
        pytest.param("@stage/", id="no file name"),
        pytest.param("@stage/with/path", id="stage with path no file"),
    ),
)
@mock.patch("snowflake.cli.plugins.notebook.manager.cli_context")
def test_error_parsing_stage(mock_ctx, stage_path):
    type(mock_ctx.connection).warehouse = PropertyMock(return_value="my_wh")

    with pytest.raises(NotebookStagePathError):
        NotebookManager().create(notebook_name="my_notebook", notebook_file=stage_path)
