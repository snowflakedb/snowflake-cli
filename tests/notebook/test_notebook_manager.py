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

from textwrap import dedent
from unittest import mock
from unittest.mock import MagicMock, PropertyMock

import pytest
from snowflake.cli._plugins.notebook.exceptions import NotebookStagePathError
from snowflake.cli._plugins.notebook.manager import NotebookManager
from snowflake.cli.api.identifiers import FQN


@mock.patch.object(NotebookManager, "_execute_query")
def test_execute(mock_execute):
    _ = NotebookManager().execute(notebook_name=FQN.from_string("MY_NOTEBOOK"))
    mock_execute.assert_called_once_with(
        query="EXECUTE NOTEBOOK IDENTIFIER('MY_NOTEBOOK')()"
    )


@mock.patch("snowflake.cli._plugins.notebook.manager.make_snowsight_url")
def test_get_url(mock_url):
    mock_url.return_value = "my_url"
    conn_mock = MagicMock(database="nb_database", schema="nb_schema")
    with mock.patch.object(NotebookManager, "_conn", conn_mock):
        result = NotebookManager().get_url(notebook_name=FQN.from_string("MY_NOTEBOOK"))

    assert result == "my_url"
    mock_url.assert_called_once_with(
        conn_mock, f"/#/notebooks/NB_DATABASE.NB_SCHEMA.MY_NOTEBOOK"
    )


@mock.patch("snowflake.cli._plugins.notebook.manager.make_snowsight_url")
@mock.patch.object(NotebookManager, "_execute_queries")
@mock.patch("snowflake.cli._plugins.notebook.manager.get_cli_context")
def test_create(mock_ctx, mock_execute, mock_url):
    type(mock_ctx().connection).warehouse = PropertyMock(return_value="MY_WH")
    mock_url.return_value = "nb_url"
    cn_mock = MagicMock(database="nb_db", schema="nb_schema")

    with mock.patch.object(NotebookManager, "_conn", cn_mock):
        _ = NotebookManager().create(
            notebook_name=FQN.from_string("MY_NOTEBOOK"),
            notebook_file="@stage/nb file.ipynb",
        )
        expected_query = dedent(
            """
            CREATE OR REPLACE NOTEBOOK IDENTIFIER('nb_db.nb_schema.MY_NOTEBOOK')
            FROM '@stage'
            QUERY_WAREHOUSE = 'MY_WH'
            MAIN_FILE = 'nb file.ipynb';
            // Cannot use IDENTIFIER(...)
            ALTER NOTEBOOK nb_db.nb_schema.MY_NOTEBOOK ADD LIVE VERSION FROM LAST;
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
def test_error_parsing_stage(stage_path):
    with pytest.raises(NotebookStagePathError):
        NotebookManager.parse_stage_as_path(stage_path)
