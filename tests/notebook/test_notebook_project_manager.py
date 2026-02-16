# Copyright (c) 2026 Snowflake Inc.
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

from snowflake.cli._plugins.notebook.project.manager import NotebookProjectManager

PROJECT_MANAGER = (
    "snowflake.cli._plugins.notebook.project.manager.NotebookProjectManager"
)


class TestNotebookProjectManager:
    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_list_projects(self, mock_execute_query, mock_cursor):
        mock_execute_query.return_value = mock_cursor(
            rows=[("test_project",), ("test_project_2",)], columns=["name"]
        )
        result = NotebookProjectManager().list_projects()
        assert result.fetchall() == [("test_project",), ("test_project_2",)]
        assert [col.name for col in result.description] == ["name"]
        mock_execute_query.assert_called_once_with("SHOW NOTEBOOK PROJECTS")

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_create_project(self, mock_execute_query, mock_cursor):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully created.",)], columns=["value"]
        )
        result = NotebookProjectManager().create(
            name="test_project",
            source='snow://workspace/"test_workspace"',
            comment="test comment",
        )
        assert result == "Project successfully created."
        mock_execute_query.assert_called_once_with(
            """CREATE NOTEBOOK PROJECT test_project FROM 'snow://workspace/"test_workspace"' COMMENT = 'test comment'"""
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_create_project_without_comment(self, mock_execute_query, mock_cursor):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully created.",)], columns=["value"]
        )
        result = NotebookProjectManager().create(
            name="test_project",
            source='snow://workspace/"test_workspace"',
            comment=None,
        )
        assert result == "Project successfully created."
        mock_execute_query.assert_called_once_with(
            """CREATE NOTEBOOK PROJECT test_project FROM 'snow://workspace/"test_workspace"'"""
        )
