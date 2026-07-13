import json
from pathlib import Path
from unittest import mock

import pytest
from snowflake.cli._plugins.dcm.utils import (
    OUTPUT_FOLDER,
    RENDERED_FOLDER,
    RENDERED_METADATA_FILE,
    announce_rendered_definitions,
    clear_command_artifacts,
    collect_output,
    save_command_response,
    save_error_result_on_failure,
)
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.utils.path_utils import change_directory


class TestClearCommandArtifacts:
    def test_clears_result_json_file(self, tmp_path):
        with change_directory(tmp_path):
            out_dir = tmp_path / OUTPUT_FOLDER
            out_dir.mkdir()
            json_file = out_dir / "plan_result.json"
            json_file.write_text('{"old": "data"}')

            clear_command_artifacts("plan")

            assert not json_file.exists()

    def test_clears_markdown_file(self, tmp_path):
        with change_directory(tmp_path):
            out_dir = tmp_path / OUTPUT_FOLDER
            out_dir.mkdir()
            md_file = out_dir / "dependencies.md"
            md_file.write_text("# old")

            clear_command_artifacts("dependencies")

            assert not md_file.exists()

    def test_does_not_touch_other_commands_files(self, tmp_path):
        with change_directory(tmp_path):
            out_dir = tmp_path / OUTPUT_FOLDER
            out_dir.mkdir()
            deploy_json = out_dir / "deploy_result.json"
            deploy_json.write_text('{"deploy": "data"}')
            plan_json = out_dir / "plan_result.json"
            plan_json.write_text('{"plan": "data"}')

            clear_command_artifacts("plan")

            assert deploy_json.exists()
            assert deploy_json.read_text() == '{"deploy": "data"}'
            assert not plan_json.exists()

    def test_does_not_touch_rendered_folder(self, tmp_path):
        with change_directory(tmp_path):
            rendered = tmp_path / OUTPUT_FOLDER / RENDERED_FOLDER
            rendered.mkdir(parents=True)
            (rendered / "manifest.yml").write_text("name: x")

            clear_command_artifacts("plan")

            assert (rendered / "manifest.yml").exists()

    def test_noop_when_out_dir_missing(self, tmp_path):
        with change_directory(tmp_path):
            clear_command_artifacts("plan")


class TestAnnounceRenderedDefinitions:
    def test_prints_path_when_folder_exists(self, tmp_path, capsys):
        with change_directory(tmp_path):
            rendered_dir = tmp_path / OUTPUT_FOLDER / RENDERED_FOLDER
            rendered_dir.mkdir(parents=True)

            announce_rendered_definitions()

            out = capsys.readouterr().out
            assert "Rendered definitions saved to:" in out
            assert RENDERED_FOLDER in out

    def test_noop_when_folder_missing(self, tmp_path, capsys):
        with change_directory(tmp_path):
            announce_rendered_definitions()

            assert "Rendered definitions saved to:" not in capsys.readouterr().out

    def test_prints_timestamp_from_marker(self, tmp_path, capsys):
        with change_directory(tmp_path):
            rendered_dir = tmp_path / OUTPUT_FOLDER / RENDERED_FOLDER
            rendered_dir.mkdir(parents=True)
            (rendered_dir / RENDERED_METADATA_FILE).write_text(
                json.dumps({"rendered_at": "2026-07-13T22:16:03+02:00"})
            )

            announce_rendered_definitions()

            out = capsys.readouterr().out
            assert "Rendered at: 2026-07-13T22:16:03+02:00" in out

    def test_omits_timestamp_when_marker_missing(self, tmp_path, capsys):
        with change_directory(tmp_path):
            rendered_dir = tmp_path / OUTPUT_FOLDER / RENDERED_FOLDER
            rendered_dir.mkdir(parents=True)

            announce_rendered_definitions()

            out = capsys.readouterr().out
            assert "Rendered definitions saved to:" in out
            assert "Rendered at:" not in out


class TestSaveCommandResponse:
    def test_saves_json_file_from_string_payload(self, tmp_path):
        with change_directory(tmp_path):
            raw_data = '{"version": 2, "changeset": []}'

            save_command_response("plan", raw_data)

            json_file = tmp_path / OUTPUT_FOLDER / "plan_result.json"
            assert json_file.exists()
            assert json_file.read_text() == raw_data

    def test_saves_json_file_from_dict_payload(self, tmp_path):
        with change_directory(tmp_path):
            raw_data = {"expectations": []}

            save_command_response("test", raw_data)

            json_file = tmp_path / OUTPUT_FOLDER / "test_result.json"
            assert json.loads(json_file.read_text()) == raw_data

    def test_creates_out_directory(self, tmp_path):
        with change_directory(tmp_path):
            assert not (tmp_path / OUTPUT_FOLDER).exists()

            save_command_response("refresh", {"refreshed_tables": []})

            assert (tmp_path / OUTPUT_FOLDER).exists()
            assert (tmp_path / OUTPUT_FOLDER / "refresh_result.json").exists()

    def test_saves_response_under_result_filename(self, tmp_path):
        with change_directory(tmp_path):
            save_command_response("deploy", {"files": []})

            assert (tmp_path / OUTPUT_FOLDER / "deploy_result.json").exists()

    def test_handles_write_error_gracefully(self, tmp_path):
        with change_directory(tmp_path):
            with mock.patch(
                "snowflake.cli._plugins.dcm.utils.SecurePath.write_text",
                side_effect=OSError("disk full"),
            ):
                save_command_response("plan", {"version": 2})

            json_file = tmp_path / OUTPUT_FOLDER / "plan_result.json"
            assert not json_file.exists()


@mock.patch("snowflake.cli._plugins.dcm.utils.StageManager.get_recursive")
@mock.patch("snowflake.cli._plugins.dcm.utils.StageManager.create")
@mock.patch("snowflake.cli._plugins.dcm.utils.FQN.from_resource")
class TestCollectOutput:
    """The backend nests rendered definitions under ``rendered/`` and writes
    ``*_result.json`` as siblings; ``collect_output`` downloads straight into
    ``out/`` so definitions land at ``out/rendered/`` and result files at the
    ``out/`` root, replacing any previous run's ``out/rendered/``.
    """

    @staticmethod
    def _stub_stage(mock_from_resource):
        mock_from_resource.return_value = FQN.from_string(
            "MY_DB.MY_SCHEMA.OUTPUT_TMP_STAGE"
        )

    def test_downloads_rendered_and_result_json_into_out(
        self, mock_from_resource, mock_create, mock_get_recursive, tmp_path
    ):
        self._stub_stage(mock_from_resource)

        def fake_download(stage_path, dest_path, **kwargs):
            dest = Path(dest_path)
            (dest / "rendered" / "sources").mkdir(parents=True, exist_ok=True)
            (dest / "rendered" / "sources" / "table.sql").write_text("SELECT 1")
            (dest / "rendered" / "manifest.yml").write_text("name: x")
            (dest / "compile_result.json").write_text("{}")
            return []

        mock_get_recursive.side_effect = fake_download

        with change_directory(tmp_path):
            with collect_output(FQN.from_string("my_project"), command_name="compile"):
                pass

            out_dir = tmp_path / OUTPUT_FOLDER
            # Definitions land directly at out/rendered/ (no intermediate level).
            assert (
                out_dir / RENDERED_FOLDER / "sources" / "table.sql"
            ).read_text() == "SELECT 1"
            assert (out_dir / RENDERED_FOLDER / "manifest.yml").exists()
            # The result JSON is directly under out/, never inside out/rendered/.
            assert (out_dir / "compile_result.json").exists()
            assert not (out_dir / RENDERED_FOLDER / "compile_result.json").exists()

    def test_writes_rendered_metadata_marker(
        self, mock_from_resource, mock_create, mock_get_recursive, tmp_path
    ):
        self._stub_stage(mock_from_resource)

        def fake_download(stage_path, dest_path, **kwargs):
            (Path(dest_path) / "rendered").mkdir(parents=True, exist_ok=True)
            (Path(dest_path) / "rendered" / "manifest.yml").write_text("name: x")
            return []

        mock_get_recursive.side_effect = fake_download

        with change_directory(tmp_path):
            with collect_output(FQN.from_string("my_project"), command_name="compile"):
                pass

            marker = tmp_path / OUTPUT_FOLDER / RENDERED_FOLDER / RENDERED_METADATA_FILE
            assert marker.exists()
            metadata = json.loads(marker.read_text())
            assert metadata["command"] == "compile"
            assert metadata["project"] == FQN.from_string("my_project").identifier
            assert metadata["rendered_at"]

    def test_replaces_stale_rendered(
        self, mock_from_resource, mock_create, mock_get_recursive, tmp_path
    ):
        self._stub_stage(mock_from_resource)

        def fake_download(stage_path, dest_path, **kwargs):
            rendered = Path(dest_path) / "rendered"
            rendered.mkdir(parents=True, exist_ok=True)
            (rendered / "new.sql").write_text("new")
            return []

        mock_get_recursive.side_effect = fake_download

        with change_directory(tmp_path):
            stale = tmp_path / OUTPUT_FOLDER / RENDERED_FOLDER
            stale.mkdir(parents=True)
            (stale / "old.sql").write_text("old")

            with collect_output(FQN.from_string("my_project"), command_name="plan"):
                pass

            rendered = tmp_path / OUTPUT_FOLDER / RENDERED_FOLDER
            assert (rendered / "new.sql").read_text() == "new"
            assert not (rendered / "old.sql").exists()

    def test_no_error_when_no_rendered_produced(
        self, mock_from_resource, mock_create, mock_get_recursive, tmp_path
    ):
        self._stub_stage(mock_from_resource)

        def fake_download(stage_path, dest_path, **kwargs):
            (Path(dest_path) / "plan_result.json").write_text("{}")
            return []

        mock_get_recursive.side_effect = fake_download

        with change_directory(tmp_path):
            with collect_output(FQN.from_string("my_project"), command_name="plan"):
                pass

            out_dir = tmp_path / OUTPUT_FOLDER
            assert (out_dir / "plan_result.json").exists()
            assert not (out_dir / RENDERED_FOLDER).exists()


class TestSaveErrorResultOnFailure:
    """When a --save-output run fails before the backend produced its own
    result file, the raised error is persisted to out/<command>_result.json so
    the failure isn't left terminal-only.
    """

    def test_writes_error_result_on_failure(self, tmp_path):
        with change_directory(tmp_path):
            with pytest.raises(RuntimeError, match="boom"):
                with save_error_result_on_failure("plan", save_output=True):
                    raise RuntimeError("boom")

            result_file = tmp_path / OUTPUT_FOLDER / "plan_result.json"
            assert result_file.exists()
            assert result_file.read_text() == "boom"

    def test_preserves_json_error_body(self, tmp_path):
        with change_directory(tmp_path):
            with pytest.raises(RuntimeError):
                with save_error_result_on_failure("plan", save_output=True):
                    raise RuntimeError('{"error": "compilation failed"}')

            result_file = tmp_path / OUTPUT_FOLDER / "plan_result.json"
            assert json.loads(result_file.read_text()) == {
                "error": "compilation failed"
            }

    def test_does_not_clobber_backend_result_file(self, tmp_path):
        with change_directory(tmp_path):
            out_dir = tmp_path / OUTPUT_FOLDER
            out_dir.mkdir()
            (out_dir / "plan_result.json").write_text('{"from": "backend"}')

            with pytest.raises(RuntimeError):
                with save_error_result_on_failure("plan", save_output=True):
                    raise RuntimeError("boom")

            assert (out_dir / "plan_result.json").read_text() == '{"from": "backend"}'

    def test_noop_without_save_output(self, tmp_path):
        with change_directory(tmp_path):
            with pytest.raises(RuntimeError):
                with save_error_result_on_failure("plan", save_output=False):
                    raise RuntimeError("boom")

            assert not (tmp_path / OUTPUT_FOLDER / "plan_result.json").exists()

    def test_noop_on_success(self, tmp_path):
        with change_directory(tmp_path):
            with save_error_result_on_failure("plan", save_output=True):
                pass

            assert not (tmp_path / OUTPUT_FOLDER / "plan_result.json").exists()
