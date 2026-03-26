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
        mock_project_manager.return_value.process_source.return_value = (
            'snow://workspace/"test_workspace"'
        )
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
        mock_project_manager.return_value.process_source.assert_called_once_with(
            'snow://workspace/"test_workspace"'
        )
        mock_project_manager.return_value.create.assert_called_once_with(
            "test_project",
            'snow://workspace/"test_workspace"',
            "test comment",
            False,
            False,
        )

    @mock.patch(PROJECT_MANAGER)
    def test_create_project_without_comment(self, mock_project_manager, runner):
        mock_project_manager.return_value.process_source.return_value = (
            'snow://workspace/"test_workspace"'
        )
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
        mock_project_manager.return_value.process_source.assert_called_once_with(
            'snow://workspace/"test_workspace"'
        )
        mock_project_manager.return_value.create.assert_called_once_with(
            "test_project", 'snow://workspace/"test_workspace"', None, False, False
        )

    @mock.patch(PROJECT_MANAGER)
    def test_drop_project(self, mock_project_manager, runner):
        mock_project_manager.return_value.drop.return_value = (
            "Project successfully dropped."
        )
        result = runner.invoke(["notebook", "project", "drop", "test_project"])
        assert result.exit_code == 0, result.output
        assert "Project successfully dropped." in result.output
        mock_project_manager.return_value.drop.assert_called_once_with("test_project")

    @mock.patch(PROJECT_MANAGER)
    def test_create_project_with_overwrite_and_skip_if_exists(
        self, mock_project_manager, runner
    ):
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
                "--overwrite",
                "--skip-if-exists",
            ]
        )
        assert result.exit_code == 1
        assert "overwrite and skip_if_exists cannot be used together" in result.output

    @mock.patch(PROJECT_MANAGER)
    def test_create_project_with_overwrite(self, mock_project_manager, runner):
        mock_project_manager.return_value.process_source.return_value = (
            'snow://workspace/"test_workspace"'
        )
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
                "--overwrite",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully created." in result.output
        mock_project_manager.return_value.process_source.assert_called_once_with(
            'snow://workspace/"test_workspace"'
        )
        mock_project_manager.return_value.create.assert_called_once_with(
            "test_project",
            'snow://workspace/"test_workspace"',
            "test comment",
            True,
            False,
        )

    @mock.patch(PROJECT_MANAGER)
    def test_create_project_with_skip_if_exists(self, mock_project_manager, runner):
        mock_project_manager.return_value.process_source.return_value = (
            'snow://workspace/"test_workspace"'
        )
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
                "--skip-if-exists",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully created." in result.output
        mock_project_manager.return_value.process_source.assert_called_once_with(
            'snow://workspace/"test_workspace"'
        )
        mock_project_manager.return_value.create.assert_called_once_with(
            "test_project",
            'snow://workspace/"test_workspace"',
            "test comment",
            False,
            True,
        )

    @mock.patch(PROJECT_MANAGER)
    def test_execute_project_with_all_params(self, mock_project_manager, runner):
        mock_project_manager.return_value.execute.return_value = (
            "Project successfully executed."
        )
        result = runner.invoke(
            [
                "notebook",
                "project",
                "execute",
                "test_project",
                "arg1",
                "arg2",
                "--main-file",
                "main.ipynb",
                "--compute-pool",
                "my_pool",
                "--query-warehouse",
                "my_warehouse",
                "--runtime",
                "my_runtime",
                "--requirements-file",
                "requirements.txt",
                "--external-access-integrations",
                "integration1",
                "--external-access-integrations",
                "integration2",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully executed." in result.output
        mock_project_manager.return_value.execute.assert_called_once_with(
            "test_project",
            ["arg1", "arg2"],
            "main.ipynb",
            "my_pool",
            "my_warehouse",
            "my_runtime",
            "requirements.txt",
            ["integration1", "integration2"],
        )

    @mock.patch(PROJECT_MANAGER)
    def test_execute_project_with_only_required_params(
        self, mock_project_manager, runner
    ):
        mock_project_manager.return_value.execute.return_value = (
            "Project successfully executed."
        )
        result = runner.invoke(
            [
                "notebook",
                "project",
                "execute",
                "test_project",
                "--main-file",
                "main.ipynb",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully executed." in result.output
        mock_project_manager.return_value.execute.assert_called_once_with(
            "test_project",
            None,
            "main.ipynb",
            None,
            None,
            None,
            None,
            None,
        )

    @mock.patch(PROJECT_MANAGER)
    def test_execute_project_with_some_optional_params(
        self, mock_project_manager, runner
    ):
        mock_project_manager.return_value.execute.return_value = (
            "Project successfully executed."
        )
        result = runner.invoke(
            [
                "notebook",
                "project",
                "execute",
                "test_project",
                "--main-file",
                "main.ipynb",
                "--compute-pool",
                "my_pool",
                "--runtime",
                "my_runtime",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully executed." in result.output
        mock_project_manager.return_value.execute.assert_called_once_with(
            "test_project",
            None,
            "main.ipynb",
            "my_pool",
            None,
            "my_runtime",
            None,
            None,
        )

    @mock.patch(PROJECT_MANAGER)
    def test_execute_project_with_arguments_only(self, mock_project_manager, runner):
        mock_project_manager.return_value.execute.return_value = (
            "Project successfully executed."
        )
        result = runner.invoke(
            [
                "notebook",
                "project",
                "execute",
                "test_project",
                "arg1",
                "arg2",
                "arg3",
                "--main-file",
                "main.ipynb",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully executed." in result.output
        mock_project_manager.return_value.execute.assert_called_once_with(
            "test_project",
            ["arg1", "arg2", "arg3"],
            "main.ipynb",
            None,
            None,
            None,
            None,
            None,
        )

    def test_execute_project_missing_name(self, runner):
        result = runner.invoke(
            [
                "notebook",
                "project",
                "execute",
                "--main-file",
                "main.ipynb",
            ]
        )
        assert result.exit_code == 1
        assert "Name is required" in result.output

    def test_execute_project_missing_main_file(self, runner):
        result = runner.invoke(
            [
                "notebook",
                "project",
                "execute",
                "test_project",
            ]
        )
        assert result.exit_code == 1
        assert "Main file is required" in result.output

    @mock.patch(PROJECT_MANAGER)
    def test_create_project_with_stage_source(self, mock_project_manager, runner):
        mock_project_manager.return_value.process_source.return_value = "@my_stage/path"
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
                "@my_stage/path",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully created." in result.output
        mock_project_manager.return_value.process_source.assert_called_once_with(
            "@my_stage/path"
        )
        mock_project_manager.return_value.create.assert_called_once_with(
            "test_project", "@my_stage/path", None, False, False
        )

    @mock.patch(PROJECT_MANAGER)
    def test_create_project_with_local_source(self, mock_project_manager, runner):
        mock_project_manager.return_value.process_source.return_value = (
            "@tmp_npo_stage_1234567"
        )
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
                "/local/path/to/project",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully created." in result.output
        mock_project_manager.return_value.process_source.assert_called_once_with(
            "/local/path/to/project"
        )
        mock_project_manager.return_value.create.assert_called_once_with(
            "test_project", "@tmp_npo_stage_1234567", None, False, False
        )

    @mock.patch(PROJECT_MANAGER)
    def test_create_project_with_file_protocol_source(
        self, mock_project_manager, runner
    ):
        mock_project_manager.return_value.process_source.return_value = (
            "@tmp_npo_stage_7654321"
        )
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
                "file:///local/path/to/project",
                "--comment",
                "local project",
            ]
        )
        assert result.exit_code == 0, result.output
        assert "Project successfully created." in result.output
        mock_project_manager.return_value.process_source.assert_called_once_with(
            "file:///local/path/to/project"
        )
        mock_project_manager.return_value.create.assert_called_once_with(
            "test_project", "@tmp_npo_stage_7654321", "local project", False, False
        )

    def test_create_project_missing_name(self, runner):
        result = runner.invoke(
            [
                "notebook",
                "project",
                "create",
                "--source",
                "@my_stage/path",
            ]
        )
        assert result.exit_code == 1
        assert "Name is required" in result.output

    def test_create_project_missing_source(self, runner):
        result = runner.invoke(
            [
                "notebook",
                "project",
                "create",
                "test_project",
            ]
        )
        assert result.exit_code == 1
        assert "Source is required" in result.output
