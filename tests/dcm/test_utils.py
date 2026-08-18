import json
from unittest import mock

import pytest
from snowflake.cli._plugins.dcm.exceptions import QueryStatusUnavailableCliError
from snowflake.cli._plugins.dcm.utils import (
    OUTPUT_FOLDER,
    announce_output_artifacts,
    output_stage,
    prepare_output_folder,
    result_file_exists,
    save_command_response,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.utils.path_utils import change_directory

RENDERED_FOLDER = "rendered"


class TestPrepareOutputFolder:
    def test_drops_the_whole_out_folder(self, tmp_path):
        with change_directory(tmp_path):
            out_dir = tmp_path / OUTPUT_FOLDER
            rendered_dir = out_dir / RENDERED_FOLDER / "models"
            rendered_dir.mkdir(parents=True)
            (rendered_dir / "orders.sql").write_text("SELECT 1")
            (out_dir / "plan_result.json").write_text('{"old": "data"}')
            (out_dir / "deploy_result.json").write_text('{"other": "command"}')

            prepare_output_folder()

            assert list(out_dir.iterdir()) == []

    def test_creates_the_out_folder_when_missing(self, tmp_path):
        with change_directory(tmp_path):
            prepare_output_folder()

            assert (tmp_path / OUTPUT_FOLDER).is_dir()


class TestSaveCommandResponse:
    def test_saves_json_file_from_string_payload(self, tmp_path):
        with change_directory(tmp_path):
            prepare_output_folder()
            raw_data = '{"version": 2, "changeset": []}'

            save_command_response("plan", raw_data)

            json_file = tmp_path / OUTPUT_FOLDER / "plan_result.json"
            assert json_file.exists()
            assert json_file.read_text() == raw_data

    def test_saves_json_file_from_dict_payload(self, tmp_path):
        with change_directory(tmp_path):
            prepare_output_folder()
            raw_data = {"expectations": []}

            save_command_response("test", raw_data)

            json_file = tmp_path / OUTPUT_FOLDER / "test_result.json"
            assert json.loads(json_file.read_text()) == raw_data

    def test_skips_the_write_when_the_file_is_already_there(self, tmp_path):
        with change_directory(tmp_path):
            prepare_output_folder()
            json_file = tmp_path / OUTPUT_FOLDER / "plan_result.json"
            json_file.write_text('{"from": "the backend download"}')

            save_command_response("plan", {"version": 2})

            assert json.loads(json_file.read_text()) == {"from": "the backend download"}

    def test_fails_when_the_write_fails(self, tmp_path):
        with change_directory(tmp_path):

            with mock.patch(
                "snowflake.cli._plugins.dcm.utils.SecurePath.write_text",
                side_effect=OSError("disk full"),
            ):
                with pytest.raises(CliError, match="disk full"):
                    save_command_response("plan", {"version": 2})

            assert not (tmp_path / OUTPUT_FOLDER / "plan_result.json").exists()


class TestAnnounceOutputArtifacts:
    def test_announces_when_files_were_produced(self, tmp_path):
        with change_directory(tmp_path):
            prepare_output_folder()
            (tmp_path / OUTPUT_FOLDER / "plan_result.json").write_text("{}")

            with mock.patch(
                "snowflake.cli._plugins.dcm.utils.cli_console.step"
            ) as mock_step:
                announce_output_artifacts()

            mock_step.assert_called_once()

    def test_announces_for_nested_files_only(self, tmp_path):
        with change_directory(tmp_path):
            prepare_output_folder()
            rendered = tmp_path / OUTPUT_FOLDER / RENDERED_FOLDER
            rendered.mkdir()
            (rendered / "manifest.yml").write_text("version: 2")

            with mock.patch(
                "snowflake.cli._plugins.dcm.utils.cli_console.step"
            ) as mock_step:
                announce_output_artifacts()

            mock_step.assert_called_once()

    def test_stays_silent_when_the_folder_is_empty(self, tmp_path):
        with change_directory(tmp_path):
            prepare_output_folder()

            with mock.patch(
                "snowflake.cli._plugins.dcm.utils.cli_console.step"
            ) as mock_step:
                announce_output_artifacts()

            mock_step.assert_not_called()

    def test_stays_silent_when_the_folder_is_missing(self, tmp_path):
        with change_directory(tmp_path):
            with mock.patch(
                "snowflake.cli._plugins.dcm.utils.cli_console.step"
            ) as mock_step:
                announce_output_artifacts()

            mock_step.assert_not_called()


class TestResultFileExists:
    def test_true_when_the_file_is_in_the_out_folder(self, tmp_path):
        with change_directory(tmp_path):
            prepare_output_folder()
            (tmp_path / OUTPUT_FOLDER / "plan_result.json").write_text("{}")

            assert result_file_exists("plan")
            assert not result_file_exists("deploy")

    def test_false_when_the_out_folder_is_empty(self, tmp_path):
        with change_directory(tmp_path):
            prepare_output_folder()

            assert not result_file_exists("plan")

    def test_nested_result_file_is_not_the_commands_own(self, tmp_path):
        with change_directory(tmp_path):
            prepare_output_folder()
            rendered = tmp_path / OUTPUT_FOLDER / RENDERED_FOLDER
            rendered.mkdir()
            (rendered / "plan_result.json").write_text("{}")

            assert not result_file_exists("plan")


OUTPUT_TMP_STAGE = "DCM_MY_PROJECT_OUTPUT_TMP_STAGE"


def _project_identifier():
    return FQN.from_string("my_project")


@pytest.fixture(autouse=True)
def mock_from_resource():
    with mock.patch(
        "snowflake.cli._plugins.dcm.utils.FQN.from_resource",
        return_value=FQN(
            database="MockDatabase",
            schema="MockSchema",
            name=OUTPUT_TMP_STAGE,
        ),
    ) as _fixture:
        yield _fixture


class TestCollectOutput:
    @mock.patch("snowflake.cli._plugins.dcm.utils.StageManager")
    def test_downloads_directly_into_out(self, stage_manager_cls, tmp_path):
        from snowflake.cli._plugins.dcm.utils import _collect_output

        stage_manager = stage_manager_cls.return_value

        def fake_get_recursive(stage_path, dest_path):
            rendered_dir = dest_path / RENDERED_FOLDER
            rendered_dir.mkdir(parents=True)
            (rendered_dir / "obj.sql").write_text("SELECT 1")
            (dest_path / "plan_result.json").write_text('{"version": 2}')

        stage_manager.get_recursive.side_effect = fake_get_recursive

        with change_directory(tmp_path):
            prepare_output_folder()
            with _collect_output(
                _project_identifier(),
                command_name="plan",
            ):
                pass

            out_dir = tmp_path / OUTPUT_FOLDER
            assert (out_dir / "plan_result.json").exists()
            assert (out_dir / RENDERED_FOLDER / "obj.sql").exists()

    @mock.patch("snowflake.cli._plugins.dcm.utils.StageManager")
    def test_no_error_when_no_rendered_produced(self, stage_manager_cls, tmp_path):
        from snowflake.cli._plugins.dcm.utils import _collect_output

        stage_manager = stage_manager_cls.return_value
        stage_manager.get_recursive.side_effect = lambda stage_path, dest_path: []

        with change_directory(tmp_path):
            prepare_output_folder()
            with _collect_output(
                _project_identifier(),
                command_name="plan",
            ):
                pass

            assert not (tmp_path / OUTPUT_FOLDER / RENDERED_FOLDER).exists()

    @mock.patch("snowflake.cli._plugins.dcm.utils.StageManager")
    def test_downloads_artifacts_when_command_fails(self, stage_manager_cls, tmp_path):
        from snowflake.cli._plugins.dcm.utils import _collect_output

        stage_manager = stage_manager_cls.return_value

        def fake_get_recursive(stage_path, dest_path):
            (dest_path / "plan_result.json").write_text('{"errors": ["compile"]}')

        stage_manager.get_recursive.side_effect = fake_get_recursive

        with change_directory(tmp_path):
            prepare_output_folder()
            with pytest.raises(RuntimeError, match="plan failed"):
                with _collect_output(
                    _project_identifier(),
                    command_name="plan",
                ):
                    raise RuntimeError("plan failed")

            assert (tmp_path / OUTPUT_FOLDER / "plan_result.json").exists()

    @mock.patch("snowflake.cli._plugins.dcm.utils.StageManager")
    def test_does_not_download_when_the_operation_was_left_running(
        self, stage_manager_cls, tmp_path
    ):
        from snowflake.cli._plugins.dcm.utils import _collect_output

        stage_manager = stage_manager_cls.return_value

        with change_directory(tmp_path):
            prepare_output_folder()
            with pytest.raises(QueryStatusUnavailableCliError):
                with _collect_output(
                    _project_identifier(),
                    command_name="plan",
                ):
                    raise QueryStatusUnavailableCliError("no status for query 42")

        stage_manager.get_recursive.assert_not_called()

    @mock.patch("snowflake.cli._plugins.dcm.utils.StageManager")
    def test_download_failure_does_not_mask_command_error(
        self, stage_manager_cls, tmp_path
    ):
        from snowflake.cli._plugins.dcm.utils import _collect_output

        stage_manager = stage_manager_cls.return_value
        stage_manager.get_recursive.side_effect = OSError("nothing to download")

        with change_directory(tmp_path):
            prepare_output_folder()
            with pytest.raises(RuntimeError, match="plan failed"):
                with _collect_output(
                    _project_identifier(),
                    command_name="plan",
                ):
                    raise RuntimeError("plan failed")

    @mock.patch("snowflake.cli._plugins.dcm.utils.StageManager")
    def test_download_failure_propagates_on_success_path(
        self, stage_manager_cls, tmp_path
    ):
        from snowflake.cli._plugins.dcm.utils import _collect_output

        stage_manager = stage_manager_cls.return_value
        stage_manager.get_recursive.side_effect = OSError("stage unreachable")

        with change_directory(tmp_path):
            with pytest.raises(OSError, match="stage unreachable"):
                with _collect_output(
                    _project_identifier(),
                    command_name="plan",
                ):
                    pass


class TestOutputStage:
    """The save_output-conditional wrapper the commands hold open around the
    operation that writes to OUTPUT_PATH."""

    @mock.patch("snowflake.cli._plugins.dcm.utils.StageManager")
    def test_yields_nothing_and_touches_no_stage_without_save_output(
        self, stage_manager_cls, tmp_path
    ):
        with change_directory(tmp_path):
            with output_stage(
                _project_identifier(),
                command_name="plan",
                save_output=False,
            ) as stage_path:
                assert stage_path is None

        stage_manager_cls.assert_not_called()

    @mock.patch("snowflake.cli._plugins.dcm.utils.StageManager")
    def test_yields_the_created_stage_with_save_output(
        self, stage_manager_cls, tmp_path
    ):
        stage_manager = stage_manager_cls.return_value

        with change_directory(tmp_path):
            prepare_output_folder()
            with output_stage(
                _project_identifier(),
                command_name="plan",
                save_output=True,
            ) as stage_path:
                assert (
                    stage_path == f"@MockDatabase.MockSchema.{OUTPUT_TMP_STAGE}/outputs"
                )

        stage_manager.create.assert_called_once()
        stage_manager.get_recursive.assert_called_once()
