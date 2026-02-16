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

PROJECT_MANAGER = (
    "snowflake.cli._plugins.notebook.project.commands.NotebookProjectManager"
)


class TestNotebookProjectCommands:
    @mock.patch(PROJECT_MANAGER)
    def test_list_projects(self, mock_project_manager, mock_cursor, runner):
        mock_project_manager.return_value.list_projects.return_value = mock_cursor(
            rows=[("test_project",), ("test_project_2",)], columns=["name"]
        )
        result = runner.invoke(["notebook", "project", "list"])
        assert result.exit_code == 0, result.output
        assert "test_project" in result.output
        assert "test_project_2" in result.output
        mock_project_manager.return_value.list_projects.assert_called_once_with()

    @mock.patch(PROJECT_MANAGER)
    def test_create_project(self, mock_project_manager, runner):
        mock_project_manager.return_value.create.return_value = (
            "Project successfully created."
        )
        result = runner.invoke(
            [
                "notebook",
                "project",
                "create",
                "test_project",
                "--source",
                'snow://workspace/"test_workspace"',
                "--comment",
                "test comment",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully created." in result.output
        mock_project_manager.return_value.create.assert_called_once_with(
            "test_project", 'snow://workspace/"test_workspace"', "test comment"
        )

    @mock.patch(PROJECT_MANAGER)
    def test_create_project_without_comment(self, mock_project_manager, runner):
        mock_project_manager.return_value.create.return_value = (
            "Project successfully created."
        )
        result = runner.invoke(
            [
                "notebook",
                "project",
                "create",
                "test_project",
                "--source",
                'snow://workspace/"test_workspace"',
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully created." in result.output
        mock_project_manager.return_value.create.assert_called_once_with(
            "test_project", 'snow://workspace/"test_workspace"', None
        )
