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

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_drop_project(self, mock_execute_query, mock_cursor):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully dropped.",)], columns=["value"]
        )
        result = NotebookProjectManager().drop(name="test_project")
        assert result == "Project successfully dropped."
        mock_execute_query.assert_called_once_with("DROP NOTEBOOK PROJECT test_project")

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_execute_project_with_all_params(self, mock_execute_query, mock_cursor):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully executed.",)], columns=["value"]
        )
        result = NotebookProjectManager().execute(
            name="test_project",
            arguments=["arg1", "arg2"],
            main_file="main_file.ipynb",
            compute_pool="compute_pool",
            query_warehouse="query_warehouse",
            runtime="runtime",
            requirements_file="requirements.txt",
            external_access_integrations=["integration1", "integration2"],
        )
        assert result == "Project successfully executed."
        mock_execute_query.assert_called_once_with(
            "EXECUTE NOTEBOOK PROJECT test_project "
            "MAIN_FILE = 'main_file.ipynb' "
            "COMPUTE_POOL = 'compute_pool' "
            "QUERY_WAREHOUSE = 'query_warehouse' "
            "RUNTIME = 'runtime' "
            "REQUIREMENTS_FILE = 'requirements.txt' "
            "EXTERNAL_ACCESS_INTEGRATIONS = ('integration1','integration2') "
            "ARGUMENTS = 'arg1 arg2'"
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_execute_project_without_arguments(self, mock_execute_query, mock_cursor):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully executed.",)], columns=["value"]
        )
        result = NotebookProjectManager().execute(
            name="test_project",
            arguments=None,
            main_file="main_file.ipynb",
            compute_pool="compute_pool",
            query_warehouse="query_warehouse",
            runtime="runtime",
            requirements_file="requirements.txt",
            external_access_integrations=["integration1"],
        )
        assert result == "Project successfully executed."
        mock_execute_query.assert_called_once_with(
            "EXECUTE NOTEBOOK PROJECT test_project "
            "MAIN_FILE = 'main_file.ipynb' "
            "COMPUTE_POOL = 'compute_pool' "
            "QUERY_WAREHOUSE = 'query_warehouse' "
            "RUNTIME = 'runtime' "
            "REQUIREMENTS_FILE = 'requirements.txt' "
            "EXTERNAL_ACCESS_INTEGRATIONS = ('integration1')"
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_execute_project_without_requirements_file(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully executed.",)], columns=["value"]
        )
        result = NotebookProjectManager().execute(
            name="test_project",
            arguments=["arg1"],
            main_file="main_file.ipynb",
            compute_pool="compute_pool",
            query_warehouse="query_warehouse",
            runtime="runtime",
            requirements_file=None,
            external_access_integrations=["integration1"],
        )
        assert result == "Project successfully executed."
        mock_execute_query.assert_called_once_with(
            "EXECUTE NOTEBOOK PROJECT test_project "
            "MAIN_FILE = 'main_file.ipynb' "
            "COMPUTE_POOL = 'compute_pool' "
            "QUERY_WAREHOUSE = 'query_warehouse' "
            "RUNTIME = 'runtime' "
            "EXTERNAL_ACCESS_INTEGRATIONS = ('integration1') "
            "ARGUMENTS = 'arg1'"
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_execute_project_without_external_access_integrations(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully executed.",)], columns=["value"]
        )
        result = NotebookProjectManager().execute(
            name="test_project",
            arguments=["arg1", "arg2"],
            main_file="main_file.ipynb",
            compute_pool="compute_pool",
            query_warehouse="query_warehouse",
            runtime="runtime",
            requirements_file="requirements.txt",
            external_access_integrations=None,
        )
        assert result == "Project successfully executed."
        mock_execute_query.assert_called_once_with(
            "EXECUTE NOTEBOOK PROJECT test_project "
            "MAIN_FILE = 'main_file.ipynb' "
            "COMPUTE_POOL = 'compute_pool' "
            "QUERY_WAREHOUSE = 'query_warehouse' "
            "RUNTIME = 'runtime' "
            "REQUIREMENTS_FILE = 'requirements.txt' "
            "ARGUMENTS = 'arg1 arg2'"
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_execute_project_with_only_required_params(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully executed.",)], columns=["value"]
        )
        result = NotebookProjectManager().execute(
            name="test_project",
            arguments=None,
            main_file="main_file.ipynb",
            compute_pool="compute_pool",
            query_warehouse="query_warehouse",
            runtime="runtime",
            requirements_file=None,
            external_access_integrations=None,
        )
        assert result == "Project successfully executed."
        mock_execute_query.assert_called_once_with(
            "EXECUTE NOTEBOOK PROJECT test_project "
            "MAIN_FILE = 'main_file.ipynb' "
            "COMPUTE_POOL = 'compute_pool' "
            "QUERY_WAREHOUSE = 'query_warehouse' "
            "RUNTIME = 'runtime'"
        )
