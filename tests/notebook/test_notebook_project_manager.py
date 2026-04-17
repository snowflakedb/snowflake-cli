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

import pytest
from click import ClickException
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
    def test_create_project_with_overwrite_and_skip_if_exists(
        self, mock_execute_query, mock_cursor
    ):
        with pytest.raises(ValueError):
            NotebookProjectManager().create(
                name="test_project",
                source='snow://workspace/"test_workspace"',
                comment="test comment",
                overwrite=True,
                skip_if_exists=True,
            )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_create_project_with_overwrite(self, mock_execute_query, mock_cursor):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully created.",)], columns=["value"]
        )
        result = NotebookProjectManager().create(
            name="test_project",
            source='snow://workspace/"test_workspace"',
            comment="test comment",
            overwrite=True,
            skip_if_exists=False,
        )
        assert result == "Project successfully created."
        mock_execute_query.assert_called_once_with(
            """CREATE OR REPLACE NOTEBOOK PROJECT test_project FROM 'snow://workspace/"test_workspace"' COMMENT = 'test comment'"""
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_create_project_with_skip_if_exists(self, mock_execute_query, mock_cursor):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully created.",)], columns=["value"]
        )
        result = NotebookProjectManager().create(
            name="test_project",
            source='snow://workspace/"test_workspace"',
            comment="test comment",
            overwrite=False,
            skip_if_exists=True,
        )
        assert result == "Project successfully created."
        mock_execute_query.assert_called_once_with(
            """CREATE NOTEBOOK PROJECT IF NOT EXISTS test_project FROM 'snow://workspace/"test_workspace"' COMMENT = 'test comment'"""
        )

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
            compute_pool=None,
            query_warehouse=None,
            runtime=None,
            requirements_file=None,
            external_access_integrations=None,
        )
        assert result == "Project successfully executed."
        mock_execute_query.assert_called_once_with(
            "EXECUTE NOTEBOOK PROJECT test_project MAIN_FILE = 'main_file.ipynb'"
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
    def test_execute_project_without_compute_pool(
        self, mock_execute_query, mock_cursor
    ):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully executed.",)], columns=["value"]
        )
        result = NotebookProjectManager().execute(
            name="test_project",
            arguments=["arg1"],
            main_file="main_file.ipynb",
            compute_pool=None,
            query_warehouse="query_warehouse",
            runtime="runtime",
            requirements_file=None,
            external_access_integrations=None,
        )
        assert result == "Project successfully executed."
        mock_execute_query.assert_called_once_with(
            "EXECUTE NOTEBOOK PROJECT test_project "
            "MAIN_FILE = 'main_file.ipynb' "
            "QUERY_WAREHOUSE = 'query_warehouse' "
            "RUNTIME = 'runtime' "
            "ARGUMENTS = 'arg1'"
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_execute_project_without_query_warehouse(
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
            query_warehouse=None,
            runtime="runtime",
            requirements_file=None,
            external_access_integrations=None,
        )
        assert result == "Project successfully executed."
        mock_execute_query.assert_called_once_with(
            "EXECUTE NOTEBOOK PROJECT test_project "
            "MAIN_FILE = 'main_file.ipynb' "
            "COMPUTE_POOL = 'compute_pool' "
            "RUNTIME = 'runtime' "
            "ARGUMENTS = 'arg1'"
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_execute_project_without_runtime(self, mock_execute_query, mock_cursor):
        mock_execute_query.return_value = mock_cursor(
            rows=[("Project successfully executed.",)], columns=["value"]
        )
        result = NotebookProjectManager().execute(
            name="test_project",
            arguments=["arg1"],
            main_file="main_file.ipynb",
            compute_pool="compute_pool",
            query_warehouse="query_warehouse",
            runtime=None,
            requirements_file=None,
            external_access_integrations=None,
        )
        assert result == "Project successfully executed."
        mock_execute_query.assert_called_once_with(
            "EXECUTE NOTEBOOK PROJECT test_project "
            "MAIN_FILE = 'main_file.ipynb' "
            "COMPUTE_POOL = 'compute_pool' "
            "QUERY_WAREHOUSE = 'query_warehouse' "
            "ARGUMENTS = 'arg1'"
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
    def test_process_source_with_stage_path(self, mock_execute_query):
        manager = NotebookProjectManager()
        result = manager.process_source("@my_stage/path")
        assert result == "@my_stage/path"
        mock_execute_query.assert_not_called()

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_process_source_with_snow_protocol(self, mock_execute_query):
        manager = NotebookProjectManager()
        result = manager.process_source('snow://workspace/"test_workspace"')
        assert result == 'snow://workspace/"test_workspace"'
        mock_execute_query.assert_not_called()

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_process_source_with_snow_protocol_uppercase(self, mock_execute_query):
        manager = NotebookProjectManager()
        result = manager.process_source('SNOW://workspace/"test_workspace"')
        assert result == 'SNOW://workspace/"test_workspace"'
        mock_execute_query.assert_not_called()

    @mock.patch(f"{PROJECT_MANAGER}._upload_directory_recursive")
    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    @mock.patch("snowflake.cli._plugins.notebook.project.manager.random.randint")
    def test_process_source_with_file_protocol(
        self, mock_randint, mock_execute_query, mock_upload
    ):
        mock_randint.return_value = 1234567
        manager = NotebookProjectManager()
        result = manager.process_source("file:///path/to/local/dir")
        assert result == "@tmp_npo_stage_1234567"
        mock_execute_query.assert_called_once_with(
            "CREATE OR REPLACE TEMPORARY STAGE tmp_npo_stage_1234567"
        )
        mock_upload.assert_called_once_with(
            "/path/to/local/dir", "tmp_npo_stage_1234567"
        )

    @mock.patch(f"{PROJECT_MANAGER}._upload_directory_recursive")
    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    @mock.patch("snowflake.cli._plugins.notebook.project.manager.random.randint")
    def test_process_source_with_file_protocol_uppercase(
        self, mock_randint, mock_execute_query, mock_upload
    ):
        mock_randint.return_value = 7654321
        manager = NotebookProjectManager()
        result = manager.process_source("FILE:///path/to/local/dir")
        assert result == "@tmp_npo_stage_7654321"
        mock_execute_query.assert_called_once_with(
            "CREATE OR REPLACE TEMPORARY STAGE tmp_npo_stage_7654321"
        )
        mock_upload.assert_called_once_with(
            "/path/to/local/dir", "tmp_npo_stage_7654321"
        )

    @mock.patch(f"{PROJECT_MANAGER}._upload_directory_recursive")
    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    @mock.patch("snowflake.cli._plugins.notebook.project.manager.random.randint")
    def test_process_source_with_local_path_no_protocol(
        self, mock_randint, mock_execute_query, mock_upload
    ):
        mock_randint.return_value = 9999999
        manager = NotebookProjectManager()
        result = manager.process_source("/path/to/local/dir")
        assert result == "@tmp_npo_stage_9999999"
        mock_execute_query.assert_called_once_with(
            "CREATE OR REPLACE TEMPORARY STAGE tmp_npo_stage_9999999"
        )
        mock_upload.assert_called_once_with(
            "/path/to/local/dir", "tmp_npo_stage_9999999"
        )

    @mock.patch(f"{PROJECT_MANAGER}._upload_directory_recursive")
    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    @mock.patch("snowflake.cli._plugins.notebook.project.manager.random.randint")
    def test_process_source_with_relative_local_path(
        self, mock_randint, mock_execute_query, mock_upload
    ):
        mock_randint.return_value = 1111111
        manager = NotebookProjectManager()
        result = manager.process_source("relative/path/to/dir")
        assert result == "@tmp_npo_stage_1111111"
        mock_execute_query.assert_called_once_with(
            "CREATE OR REPLACE TEMPORARY STAGE tmp_npo_stage_1111111"
        )
        mock_upload.assert_called_once_with(
            "relative/path/to/dir", "tmp_npo_stage_1111111"
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_process_source_with_invalid_protocol(self, mock_execute_query):
        manager = NotebookProjectManager()
        with pytest.raises(ValueError) as exc_info:
            manager.process_source("http://invalid/path")
        assert "Invalid source: 'http://invalid/path'" in str(exc_info.value)
        assert "Snowflake stage path" in str(exc_info.value)
        mock_execute_query.assert_not_called()

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_process_source_with_https_protocol(self, mock_execute_query):
        manager = NotebookProjectManager()
        with pytest.raises(ValueError) as exc_info:
            manager.process_source("https://example.com/path")
        assert "Invalid source: 'https://example.com/path'" in str(exc_info.value)
        mock_execute_query.assert_not_called()

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_upload_directory_recursive_with_flat_structure(
        self, mock_execute_query, tmp_path
    ):
        (tmp_path / "file1.py").write_text("content1")
        (tmp_path / "file2.py").write_text("content2")

        manager = NotebookProjectManager()
        manager._upload_directory_recursive(str(tmp_path), "test_stage")  # noqa: SLF001

        assert mock_execute_query.call_count == 2
        calls = [str(call) for call in mock_execute_query.call_args_list]
        assert any(
            "PUT file://" in call and "file1.py" in call and "@test_stage" in call
            for call in calls
        )
        assert any(
            "PUT file://" in call and "file2.py" in call and "@test_stage" in call
            for call in calls
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_upload_directory_recursive_with_nested_structure(
        self, mock_execute_query, tmp_path
    ):
        (tmp_path / "root_file.py").write_text("root content")
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "nested_file.py").write_text("nested content")
        deep_subdir = subdir / "deep"
        deep_subdir.mkdir()
        (deep_subdir / "deep_file.py").write_text("deep content")

        manager = NotebookProjectManager()
        manager._upload_directory_recursive(str(tmp_path), "test_stage")  # noqa: SLF001

        assert mock_execute_query.call_count == 3
        calls = [str(call) for call in mock_execute_query.call_args_list]
        assert any(
            "@test_stage auto_compress=false" in call and "root_file.py" in call
            for call in calls
        )
        assert any(
            "@test_stage/subdir" in call and "nested_file.py" in call for call in calls
        )
        assert any(
            "@test_stage/subdir/deep" in call and "deep_file.py" in call
            for call in calls
        )

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_upload_directory_recursive_not_a_directory(
        self, mock_execute_query, tmp_path
    ):
        file_path = tmp_path / "not_a_dir.txt"
        file_path.write_text("content")

        manager = NotebookProjectManager()
        with pytest.raises(ClickException) as exc_info:
            manager._upload_directory_recursive(  # noqa: SLF001
                str(file_path), "test_stage"
            )
        assert "is not a directory" in str(exc_info.value)
        mock_execute_query.assert_not_called()

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_upload_directory_recursive_nonexistent_path(
        self, mock_execute_query, tmp_path
    ):
        nonexistent_path = tmp_path / "nonexistent"

        manager = NotebookProjectManager()
        with pytest.raises(ClickException) as exc_info:
            manager._upload_directory_recursive(  # noqa: SLF001
                str(nonexistent_path), "test_stage"
            )
        assert "is not a directory" in str(exc_info.value)
        mock_execute_query.assert_not_called()

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_upload_directory_recursive_empty_directory(
        self, mock_execute_query, tmp_path
    ):
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        manager = NotebookProjectManager()
        manager._upload_directory_recursive(  # noqa: SLF001
            str(empty_dir), "test_stage"
        )

        mock_execute_query.assert_not_called()

    @mock.patch(f"{PROJECT_MANAGER}.execute_query")
    def test_upload_directory_recursive_skips_subdirectories(
        self, mock_execute_query, tmp_path
    ):
        (tmp_path / "file.py").write_text("content")
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        manager = NotebookProjectManager()
        manager._upload_directory_recursive(str(tmp_path), "test_stage")  # noqa: SLF001

        assert mock_execute_query.call_count == 1
        call_str = str(mock_execute_query.call_args)
        assert "file.py" in call_str
        assert "subdir" not in call_str or "file.py" in call_str
