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

import io
from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.stage.diff import DiffResult
from snowflake.cli._plugins.streamlit.streamlit_entity_model import (
    SPCS_RUNTIME_V2_NAME,
    WAREHOUSE_RUNTIME_NAME,
)
from snowflake.cli.api.artifacts.bundle_map import BundleMap
from snowflake.cli.api.errno import INSUFFICIENT_PRIVILEGES, SQL_COMPILATION_ERROR
from snowflake.cli.api.exceptions import CliError, SnowflakeSQLExecutionError
from snowflake.cli.api.project.schemas.entities.common import PathMapping
from snowflake.connector.errors import ProgrammingError
from snowflake.connector.util_text import split_statements

from tests.streamlit.streamlit_test_class import (
    STREAMLIT_NAME,
    StreamlitTestClass,
    stage_diff_unchanged,
    stage_diff_with_changes,
)


class TestStreamlitMainFile(StreamlitTestClass):
    @pytest.mark.parametrize("prune", [False, True])
    def test_changed_directory_entrypoint_is_rejected_before_mutation(
        self, tmp_path, prune
    ):
        self._write_file(tmp_path / "new_app.py" / "support.py")
        entity = self._create_entity(tmp_path, "new_app.py", ["new_app.py"])
        self.mock_describe.return_value.fetchone.return_value = {
            "main_file": "old_app.py",
            "live_version_location_uri": f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/",
        }
        with (
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
            ) as sync,
            pytest.raises(CliError, match="not a file in the deployment bundle"),
        ):
            entity._deploy_versioned(  # noqa: SLF001 -- isolate the deployment phase
                entity.bundle(), object_exists=True, prune=prune
            )
        sync.assert_not_called()
        self.mock_execute.assert_not_called()

    @pytest.mark.parametrize(
        "current",
        [
            {},
            {"main_file": "old_app.py", "live_version_location_uri": ""},
            {
                "main_file": "old_app.py",
                "live_version_location_uri": None,
                "default_version_location_uri": "snow://streamlit/DB.PUBLIC.test_streamlit/versions/1/",
                "root_location": None,
            },
        ],
    )
    def test_missing_live_stage_never_authorizes_recreation(self, tmp_path, current):
        entity = self._create_entity(tmp_path, "new_app.py", [])
        self.mock_describe.return_value.fetchone.return_value = current
        assert entity._is_legacy_deployment() is False  # noqa: SLF001
        with (
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
            ) as sync,
            pytest.raises(CliError, match="Restore its live version"),
        ):
            entity._deploy_versioned(  # noqa: SLF001 -- isolate the deployment phase
                mock.Mock(spec=BundleMap), object_exists=True
            )
        self.mock_execute.assert_not_called()
        sync.assert_not_called()

    @pytest.mark.parametrize("active_main", ["old_app.py", None])
    def test_prune_with_unspecified_unbundled_entrypoint_is_rejected(
        self, tmp_path, active_main
    ):
        self._write_file(tmp_path / "support.py")
        entity = self._create_entity(tmp_path, None, ["support.py"])
        self.mock_describe.return_value.fetchone.return_value = {
            "main_file": active_main,
            "live_version_location_uri": f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/",
        }
        with (
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
            ) as sync,
            pytest.raises(
                CliError, match="Include the existing entrypoint in artifacts"
            ),
        ):
            entity._deploy_versioned(  # noqa: SLF001 -- isolate the deployment phase
                entity.bundle(), object_exists=True, prune=True
            )
        sync.assert_not_called()
        self.mock_execute.assert_not_called()

    def test_directory_mapping_is_not_a_bundled_entrypoint(self, tmp_path):
        self._write_file(tmp_path / "assets" / "support.py")
        entity = self._create_entity(
            tmp_path, None, [PathMapping(src="assets", dest="old_app.py")]
        )
        self.mock_describe.return_value.fetchone.return_value = {
            "main_file": "old_app.py",
            "live_version_location_uri": f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/",
        }
        bundle = entity.bundle()
        assert bundle.to_project_path(Path("old_app.py")) is not None
        assert (bundle.deploy_root() / "old_app.py").is_dir()
        with (
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
            ) as sync,
            pytest.raises(
                CliError, match="Include the existing entrypoint in artifacts"
            ),
        ):
            entity._deploy_versioned(  # noqa: SLF001 -- isolate the deployment phase
                bundle, object_exists=True, prune=True
            )
        sync.assert_not_called()
        self.mock_execute.assert_not_called()

    @pytest.mark.parametrize(
        "include_entrypoint,prune", [(False, False), (True, False), (True, True)]
    )
    def test_unspecified_entrypoint_allows_safe_content_updates(
        self, tmp_path, include_entrypoint, prune
    ):
        self._write_file(tmp_path / "support.py")
        artifacts = ["support.py"]
        if include_entrypoint:
            self._write_file(tmp_path / "old_app.py")
            artifacts.append("old_app.py")
        entity = self._create_entity(tmp_path, None, artifacts)
        self.mock_describe.return_value.fetchone.return_value = {
            "main_file": "old_app.py",
            "live_version_location_uri": f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/",
        }
        with (
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage",
                return_value=stage_diff_unchanged(),
            ) as sync,
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager"
            ),
        ):
            entity._deploy_versioned(  # noqa: SLF001 -- isolate the deployment phase
                entity.bundle(), object_exists=True, prune=prune
            )
        assert sync.call_count == 1
        assert sync.call_args.kwargs["prune"] == prune
        self.mock_execute.assert_not_called()

    @pytest.mark.parametrize("prune", [False, True])
    @pytest.mark.parametrize("files_changed", [False, True])
    @pytest.mark.parametrize("runtime", [WAREHOUSE_RUNTIME_NAME, SPCS_RUNTIME_V2_NAME])
    def test_versioned_entrypoint_change_uploads_then_alters_then_prunes(
        self, tmp_path, prune, files_changed, runtime
    ):
        self._write_file(tmp_path / "new_app.py")
        entity = self._create_entity(tmp_path, "new_app.py", ["new_app.py"])
        live_uri = f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/"
        self.mock_describe.return_value.fetchone.return_value = {
            "main_file": "old_app.py",
            "live_version_location_uri": live_uri,
            "query_warehouse": "LIVE_WH",
            "runtime_name": runtime,
        }
        events = []
        upload_diff = (
            stage_diff_with_changes() if files_changed else stage_diff_unchanged()
        )
        prune_diff = DiffResult(only_on_stage=[Path("old_app.py")])

        def sync_stage(**kwargs):
            events.append("prune" if kwargs["prune"] else "upload")
            assert kwargs["stage_path_parts"].full_path.rstrip("/") == live_uri.rstrip(
                "/"
            )
            return prune_diff if kwargs["prune"] else upload_diff

        self.mock_execute.side_effect = lambda sql, **kwargs: events.append("alter")
        with (
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage",
                side_effect=sync_stage,
            ) as sync,
            mock.patch.object(
                entity, "_sync_tags", side_effect=lambda: events.append("tags")
            ),
            mock.patch.object(entity, "_restart_running_app") as restart,
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager"
            ) as manager,
        ):
            entity._deploy_versioned(  # noqa: SLF001 -- isolate the deployment phase
                entity.bundle(), object_exists=True, prune=prune
            )

        assert events == ["upload", "alter", "tags"] + (["prune"] if prune else [])
        self.mock_execute.assert_called_once_with(
            f"ALTER STREAMLIT IDENTIFIER('{STREAMLIT_NAME}') SET\nMAIN_FILE = 'new_app.py';"
        )
        assert sync.call_args_list[0].kwargs["prune"] is False
        assert sync.call_args_list[0].kwargs["warn_on_preserved_files"] is not prune
        manager.return_value.grant_privileges.assert_called_once_with(entity.model)
        assert restart.called == (
            runtime == SPCS_RUNTIME_V2_NAME and (files_changed or prune)
        )

    @pytest.mark.parametrize("prune", [False, True])
    def test_versioned_entrypoint_upload_failure_keeps_old_configuration(
        self, tmp_path, prune
    ):
        self._write_file(tmp_path / "new_app.py")
        entity = self._create_entity(tmp_path, "new_app.py", ["new_app.py"])
        self.mock_describe.return_value.fetchone.return_value = {
            "main_file": "old_app.py",
            "live_version_location_uri": f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/",
        }
        with (
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage",
                side_effect=SnowflakeSQLExecutionError(),
            ) as sync,
            mock.patch.object(entity, "_sync_tags") as tags,
            mock.patch.object(entity, "_restart_running_app") as restart,
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager"
            ) as manager,
            pytest.raises(SnowflakeSQLExecutionError),
        ):
            entity._deploy_versioned(  # noqa: SLF001 -- isolate the deployment phase
                entity.bundle(), object_exists=True, prune=prune
            )

        assert sync.call_count == 1
        assert sync.call_args.kwargs["prune"] is False
        self.mock_execute.assert_not_called()
        tags.assert_not_called()
        restart.assert_not_called()
        manager.return_value.grant_privileges.assert_not_called()

    @pytest.mark.parametrize(
        "errno", [92824, INSUFFICIENT_PRIVILEGES, SQL_COMPILATION_ERROR, None]
    )
    def test_versioned_entrypoint_alter_failure_never_prunes_or_recreates(
        self, tmp_path, errno
    ):
        self._write_file(tmp_path / "new_app.py")
        entity = self._create_entity(tmp_path, "new_app.py", ["new_app.py"])
        self.mock_describe.return_value.fetchone.return_value = {
            "main_file": "old_app.py",
            "live_version_location_uri": f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/",
        }
        error = ProgrammingError(msg="ALTER failed", errno=errno)
        self.mock_execute.side_effect = error
        with (
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage",
                return_value=stage_diff_with_changes(),
            ) as sync,
            mock.patch.object(entity, "_sync_tags") as tags,
            mock.patch.object(entity, "_restart_running_app") as restart,
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.StreamlitManager"
            ) as manager,
            pytest.raises(CliError if errno == 92824 else ProgrammingError) as raised,
        ):
            entity._deploy_versioned(  # noqa: SLF001 -- isolate the deployment phase
                entity.bundle(), object_exists=True, prune=True
            )

        assert sync.call_count == 1
        assert sync.call_args.kwargs["prune"] is False
        self.mock_execute.assert_called_once_with(
            f"ALTER STREAMLIT IDENTIFIER('{STREAMLIT_NAME}') SET\nMAIN_FILE = 'new_app.py';"
        )
        if errno == 92824:
            assert "different identifier" in str(raised.value)
            assert "not recreated" in str(raised.value)
            assert raised.value.__cause__ is error
        else:
            assert raised.value is error
        tags.assert_not_called()
        restart.assert_not_called()
        manager.return_value.grant_privileges.assert_not_called()

    @pytest.mark.parametrize("legacy", [False, True])
    @pytest.mark.parametrize(
        "filename,literal",
        [
            ("app's main.py", "'app''s main.py'"),
            ("nested/main.py", "'nested/main.py'"),
            (" app.py ", "' app.py '"),
        ],
    )
    def test_main_file_change_quotes_and_preserves_the_exact_filename(
        self, tmp_path, legacy, filename, literal
    ):
        entity = self._create_entity(tmp_path, filename, [])
        sql = entity.get_alter_sql(current={"main_file": "app.py"}, legacy=legacy)
        assert f"MAIN_FILE = {literal}" in sql
        assert len(list(split_statements(io.StringIO(sql)))) == 1

    @pytest.mark.parametrize("main_file", [None, "app.py"])
    @pytest.mark.parametrize("legacy", [False, True])
    def test_unchanged_or_unspecified_main_file_does_not_alter(
        self, tmp_path, main_file, legacy
    ):
        entity = self._create_entity(tmp_path, main_file, [])
        assert (
            entity.get_alter_sql(current={"main_file": "app.py"}, legacy=legacy) is None
        )

    def test_empty_main_file_fails_before_upload_or_alter(self, tmp_path):
        entity = self._create_entity(tmp_path, "", [])
        self.mock_describe.return_value.fetchone.return_value = {
            "main_file": "app.py",
            "live_version_location_uri": f"snow://streamlit/DB.PUBLIC.{STREAMLIT_NAME}/versions/live/",
        }
        with (
            mock.patch(
                "snowflake.cli._plugins.streamlit.streamlit_entity.sync_deploy_root_with_stage"
            ) as sync,
            pytest.raises(CliError, match="main_file cannot be empty"),
        ):
            entity._deploy_versioned(  # noqa: SLF001 -- isolate the deployment phase
                mock.Mock(spec=BundleMap), object_exists=True, prune=True
            )
        sync.assert_not_called()
        self.mock_execute.assert_not_called()
