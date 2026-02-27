import json

from snowflake.cli._plugins.dcm.manager import OUTPUT_FOLDER
from snowflake.cli._plugins.dcm.utils import (
    clear_command_artifacts,
    save_command_response,
)
from snowflake.cli.api.utils.path_utils import change_directory


class TestClearCommandArtifacts:
    def test_clears_json_file(self, tmp_path):
        with change_directory(tmp_path):
            out_dir = tmp_path / OUTPUT_FOLDER
            out_dir.mkdir()
            json_file = out_dir / "plan.json"
            json_file.write_text('{"old": "data"}')

            clear_command_artifacts("plan")

            assert not json_file.exists()

    def test_clears_artifacts_directory(self, tmp_path):
        with change_directory(tmp_path):
            out_dir = tmp_path / OUTPUT_FOLDER
            artifacts_dir = out_dir / "plan"
            artifacts_dir.mkdir(parents=True)
            (artifacts_dir / "plan_output").mkdir()
            (artifacts_dir / "plan_output" / "rendered.sql").write_text("SELECT 1")

            clear_command_artifacts("plan")

            assert not artifacts_dir.exists()

    def test_does_not_touch_other_commands_files(self, tmp_path):
        with change_directory(tmp_path):
            out_dir = tmp_path / OUTPUT_FOLDER
            out_dir.mkdir()
            deploy_json = out_dir / "deploy.json"
            deploy_json.write_text('{"deploy": "data"}')
            plan_json = out_dir / "plan.json"
            plan_json.write_text('{"plan": "data"}')

            clear_command_artifacts("plan")

            assert deploy_json.exists()
            assert deploy_json.read_text() == '{"deploy": "data"}'
            assert not plan_json.exists()

    def test_noop_when_out_dir_missing(self, tmp_path):
        with change_directory(tmp_path):
            clear_command_artifacts("plan")


class TestSaveCommandResponse:
    def test_saves_json_file_from_string_payload(self, tmp_path):
        with change_directory(tmp_path):
            raw_data = '{"version": 2, "changeset": []}'

            save_command_response("plan", raw_data)

            json_file = tmp_path / OUTPUT_FOLDER / "plan.json"
            assert json_file.exists()
            assert json_file.read_text() == raw_data

    def test_saves_json_file_from_dict_payload(self, tmp_path):
        with change_directory(tmp_path):
            raw_data = {"expectations": []}

            save_command_response("test", raw_data)

            json_file = tmp_path / OUTPUT_FOLDER / "test.json"
            assert json.loads(json_file.read_text()) == raw_data

    def test_creates_out_directory(self, tmp_path):
        with change_directory(tmp_path):
            assert not (tmp_path / OUTPUT_FOLDER).exists()

            save_command_response("refresh", {"refreshed_tables": []})

            assert (tmp_path / OUTPUT_FOLDER).exists()
            assert (tmp_path / OUTPUT_FOLDER / "refresh.json").exists()
