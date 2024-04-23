from unittest import mock
from unittest.mock import MagicMock

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
