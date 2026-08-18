import json
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
from snowflake.cli._plugins.dcm.commands import (
    _check_account_identifier,
    _check_project_owner,
)
from snowflake.cli._plugins.dcm.exceptions import QueryStatusUnavailableCliError
from snowflake.cli._plugins.dcm.models import DCMAsset, DCMManifest, DCMTarget
from snowflake.cli._plugins.dcm.multistep_progress import MultiStepProgress
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN, AccountIdentifier
from snowflake.cli.api.utils.path_utils import change_directory

TEST_SFQID = "af72f4cc-107c-4f1b-b8a9-7a9811203bc5"


def _analyze_response(files=None):
    """Helper to create a JSON analyze response string."""
    if files is None:
        files = [
            {
                "source_path": "sources/definitions/ok.sql",
                "definitions": [{"id": {"name": "OK"}, "issues": []}],
                "issues": [],
            }
        ]
    return json.dumps({"files": files})


def _assert_json_dumped(command: str, api_result: dict[str, Any], tmp_path: Path):
    json_file = tmp_path / "out" / f"{command}_result.json"
    assert json_file.exists()
    assert json.loads(json_file.read_text()) == api_result


def _mock_cursor_for_format(mock_cursor, data: dict, format_name: str):
    columns: list[str] | list[dict[str, object]]
    if format_name == "json":
        columns = ["result"]
    elif format_name == "json_ext":
        columns = [{"name": "result", "type_code": 5}]
    else:
        raise ValueError(f"Unsupported format: {format_name}")
    return mock_cursor(rows=[(json.dumps(data),)], columns=columns)


def _plan_cursor(mock_cursor, row: str = json.dumps({"version": 2, "changeset": []})):
    return mock_cursor(rows=[(row,)], columns=("result",))


def _created_output_path(mock_output_stage):
    """The OUTPUT_PATH the command should have passed: the stage output_stage
    created, plus the subdirectory it downloads artifacts from."""
    return f"@{mock_output_stage.create.call_args.args[0]}/outputs"


def _assert_format_result(payload, expected_data, format_name: str):
    if format_name == "json":
        assert payload == [{"result": json.dumps(expected_data)}]
    elif format_name == "json_ext":
        assert payload == [{"result": expected_data}]
    else:
        raise ValueError(f"Unsupported format: {format_name}")


@pytest.fixture
def mock_dcm_manager():
    with mock.patch(
        "snowflake.cli._plugins.dcm.commands.DCMProjectManager"
    ) as _fixture:
        yield _fixture


@pytest.fixture
def mock_manifest_load():
    with mock.patch("snowflake.cli._plugins.dcm.commands.DCMManifest.load") as _fixture:
        yield _fixture


@pytest.fixture
def mock_object_manager():
    with mock.patch("snowflake.cli._plugins.dcm.commands.ObjectManager") as _fixture:
        yield _fixture


@pytest.fixture
def mock_server_poll():
    with mock.patch("snowflake.cli._plugins.dcm.commands.ServerPoll") as _fixture:
        yield _fixture


@pytest.fixture(autouse=True)
def mock_output_stage():
    """The StageManager backing the --save-output output stage.

    output_stage creates and drains that stage from the command, so unlike the DCM
    manager it is not covered by mock_dcm_manager. Autouse because without it a
    --save-output test issues real stage queries.
    """
    with mock.patch(
        "snowflake.cli._plugins.dcm.utils.StageManager"
    ) as stage_manager_cls:
        yield stage_manager_cls.return_value


@pytest.fixture
def mock_multistep_progress():
    """Wraps (rather than replaces) MultiStepProgress: real progress-tracking
    behavior is preserved, but the constructor call is recorded so a test can
    assert on the exact StepDefinition list a command wired up."""
    with mock.patch(
        "snowflake.cli._plugins.dcm.commands.MultiStepProgress",
        wraps=MultiStepProgress,
    ) as _fixture:
        yield _fixture


@pytest.fixture
def mock_project_exists():
    with mock.patch(
        "snowflake.cli._plugins.dcm.commands.ObjectManager.object_exists",
        return_value=True,
    ) as _fixture:
        yield _fixture


_DEFAULT_TARGET_FIELDS = {
    "account_identifier": "MY_ORG-MY_ACCOUNT",
    "project_owner": "MY_ROLE",
}


class TestDCMCreate:
    def test_create(
        self, mock_dcm_manager, mock_object_manager, runner, project_directory
    ):
        mock_object_manager().object_exists.return_value = False
        with project_directory("dcm_project"):
            command = ["dcm", "create", "my_project"]
            result = runner.invoke(command)
            assert result.exit_code == 0, result.output

            mock_dcm_manager().create.assert_called_once_with(
                project_identifier=FQN.from_string("my_project")
            )

    @pytest.mark.parametrize("if_not_exists", [False, True])
    def test_create_object_exists(
        self,
        mock_dcm_manager,
        mock_object_manager,
        runner,
        project_directory,
        if_not_exists,
    ):
        mock_object_manager().object_exists.return_value = True
        with project_directory("dcm_project"):
            command = ["dcm", "create", "my_project"]
            if if_not_exists:
                command.append("--if-not-exists")
            result = runner.invoke(command)
            if if_not_exists:
                assert result.exit_code == 0, result.output
                assert "DCM Project 'my_project' already exists." in result.output
            else:
                assert result.exit_code == 1, result.output

            mock_dcm_manager().create.assert_not_called()

    def test_create_with_target_flag(
        self,
        mock_object_manager,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
    ):
        mock_object_manager().object_exists.return_value = False
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "create", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().create.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )


@pytest.fixture(autouse=True)
def mock_check_account_identifier():
    with mock.patch(
        "snowflake.cli._plugins.dcm.commands._check_account_identifier"
    ) as _fixture:
        yield _fixture


@pytest.fixture(autouse=True)
def mock_check_project_owner():
    with mock.patch(
        "snowflake.cli._plugins.dcm.commands._check_project_owner"
    ) as _fixture:
        yield _fixture


@pytest.fixture(autouse=True)
def frozen_elapsed_time():
    """Elapsed time in printed progress lines is wall-clock and non-deterministic.

    Without this, snapshot assertions on those lines would flake on a slow
    test run.
    """
    with mock.patch(
        "snowflake.cli._plugins.dcm.multistep_progress.MultiStepProgress._elapsed_suffix",
        return_value=" (1m 12s)",
    ):
        yield


def _manifest_without_config():
    """Helper to create a manifest with target that has no templating_config."""
    return DCMManifest.from_dict(
        {
            "manifest_version": 2,
            "type": "dcm_project",
            "default_target": "dev",
            "targets": {"dev": {"project_name": "ignored", **_DEFAULT_TARGET_FIELDS}},
        }
    )


def _manifest_with_assets():
    """Helper to create a manifest declaring an assets section."""
    return DCMManifest.from_dict(
        {
            "manifest_version": 2,
            "type": "dcm_project",
            "default_target": "dev",
            "targets": {"dev": {"project_name": "ignored", **_DEFAULT_TARGET_FIELDS}},
            "assets": {"seeds": {"paths": ["data/*.csv"]}},
        }
    )


def _manifest_with_env_vars():
    """Helper to create a manifest declaring env_vars/env_secrets."""
    return DCMManifest.from_dict(
        {
            "manifest_version": 2,
            "type": "dcm_project",
            "default_target": "dev",
            "targets": {"dev": {"project_name": "ignored", **_DEFAULT_TARGET_FIELDS}},
            "templating": {
                "env_vars": [{"DB_HOST": None}],
                "env_secrets": [{"AWS_SECRET_KEY": None}],
            },
        }
    )


class TestDCMDeploy:
    def test_deploy_project(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar"])

        assert result.exit_code == 0, result.output

        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            alias=None,
            skip_plan=False,
            env_vars={},
        )

    def test_deploy_wires_up_expected_progress_steps(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        mock_multistep_progress,
    ):
        # given
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        # when
        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar"])

        # then
        assert result.exit_code == 0, result.output
        steps = mock_multistep_progress.call_args.args[0]
        assert [step.label for step in steps] == [
            "UPLOAD",
            "RENDER",
            "COMPILE",
            "PLAN",
            "DEPLOY",
        ]

    def test_deploy_project_with_variables(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar", "-D", "key=value"])
        assert result.exit_code == 0, result.output

        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=["key=value"],
            alias=None,
            skip_plan=False,
            env_vars={},
        )

    def test_deploy_project_with_alias(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar", "--alias", "my_alias"])
        assert result.exit_code == 0, result.output

        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            alias="my_alias",
            skip_plan=False,
            env_vars={},
        )

    def test_deploy_project_with_sync(
        self,
        mock_dcm_manager,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        """Test that files are synced to project stage when from_stage is not provided."""
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "my_project"])
            assert result.exit_code == 0, result.output

        call_args = mock_dcm_manager().deploy_async.call_args
        assert "DCM_FOOBAR" in call_args.kwargs["from_stage"]
        assert call_args.kwargs["from_stage"].endswith("_TMP_STAGE")

    def test_deploy_project_with_from_local_directory(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        tmp_path,
        mock_server_poll,
    ):
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )
        mock_manifest_load.return_value = _manifest_without_config()

        source_dir = tmp_path / "source_project"
        source_dir.mkdir()

        manifest_file = source_dir / "manifest.yml"
        manifest_file.write_text("type: dcm_project\n")

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "deploy", "my_project", "--from", str(source_dir)]
            )
            assert result.exit_code == 0, result.output

        mock_dcm_manager().sync_local_files.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            source_directory=str(source_dir),
            progress=mock.ANY,
            assets=[],
        )

        call_args = mock_dcm_manager().deploy_async.call_args
        assert call_args.kwargs["from_stage"].endswith("_TMP_STAGE")

    def test_deploy_threads_declared_assets_into_sync(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        tmp_path,
        mock_server_poll,
    ):
        # The command resolves assets from the manifest (via TargetContext) and
        # hands them to sync_local_files -- there is no second manifest load.
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_assets()

        source_dir = tmp_path / "source_project"
        source_dir.mkdir()
        (source_dir / "manifest.yml").write_text("type: dcm_project\n")

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "deploy", "my_project", "--from", str(source_dir)]
            )
            assert result.exit_code == 0, result.output

        mock_dcm_manager().sync_local_files.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            source_directory=str(source_dir),
            progress=mock.ANY,
            assets=[DCMAsset(name="seeds", paths=["data/*.csv"])],
        )

    def test_deploy_with_target_flag(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            alias=None,
            skip_plan=False,
            env_vars={},
        )

    def test_deploy_with_default_target(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            alias=None,
            skip_plan=False,
            env_vars={},
        )

    def test_deploy_explicit_identifier_still_uses_target_config(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        """When explicit identifier is provided, it overrides target's project_name
        but configuration from target should still be applied."""
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {
                    "dev": {
                        "project_name": "target_project",
                        "templating_config": "dev_config",
                        **_DEFAULT_TARGET_FIELDS,
                    }
                },
                "templating": {"configurations": {"dev_config": {}}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "deploy", "explicit_project", "--target", "dev"]
            )

        assert result.exit_code == 0, result.output
        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("explicit_project"),
            configuration="DEV_CONFIG",
            from_stage="TMP_STAGE",
            variables=None,
            alias=None,
            skip_plan=False,
            env_vars={},
        )

    def test_deploy_with_target_uses_configuration(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {
                    "dev": {
                        "project_name": "my_project",
                        "templating_config": "dev_config",
                        **_DEFAULT_TARGET_FIELDS,
                    }
                },
                "templating": {"configurations": {"dev_config": {}}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            configuration="DEV_CONFIG",
            from_stage="TMP_STAGE",
            variables=None,
            alias=None,
            skip_plan=False,
            env_vars={},
        )

    def test_deploy_with_save_output(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        tmp_path,
        mock_server_poll,
    ):
        plan_response = {"version": 2, "changeset": []}
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(
            mock_cursor, json.dumps(plan_response)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "deploy", "fooBar", "--save-output"])

            assert result.exit_code == 0, result.output
            _assert_json_dumped("deploy", plan_response, tmp_path)

    @pytest.mark.parametrize("format_name", ["json", "json_ext"])
    def test_deploy_with_json_formats_returns_response(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        project_directory,
        format_name,
        mock_server_poll,
    ):
        plan_response = {"version": 2, "changeset": []}
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _mock_cursor_for_format(
            mock_cursor, plan_response, format_name
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar", "--format", format_name])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        _assert_format_result(payload, plan_response, format_name)

    def test_deploy_collects_declared_env_vars_from_shell(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        monkeypatch,
        mock_server_poll,
    ):
        monkeypatch.setenv("DB_HOST", "prod.analytics.internal")
        monkeypatch.setenv("AWS_SECRET_KEY", "shhh")
        monkeypatch.setenv("UNRELATED_VAR", "should-not-be-sent")
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            alias=None,
            skip_plan=False,
            env_vars={"DB_HOST": "prod.analytics.internal", "AWS_SECRET_KEY": "shhh"},
        )

    def test_deploy_omits_declared_env_var_missing_from_shell(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        monkeypatch,
        mock_server_poll,
    ):
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project"):
            with mock.patch(
                "snowflake.cli._plugins.dcm.env.cli_console"
            ) as mock_console:
                result = runner.invoke(["dcm", "deploy", "fooBar"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            alias=None,
            skip_plan=False,
            env_vars={},
        )
        mock_console.warning.assert_called_once()
        warning_message = mock_console.warning.call_args[0][0]
        assert "DB_HOST" in warning_message
        assert "AWS_SECRET_KEY" in warning_message

    def test_deploy_reads_env_vars_from_env_file(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        monkeypatch,
        mock_server_poll,
    ):
        """--env-file fills in names the shell doesn't already provide."""
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project") as project_dir:
            (project_dir / ".env").write_text(
                "DB_HOST=prod.analytics.internal\nAWS_SECRET_KEY=shhh\n"
            )
            result = runner.invoke(
                [
                    "dcm",
                    "deploy",
                    "fooBar",
                    "--env-file",
                    str(project_dir / ".env"),
                ]
            )

        assert result.exit_code == 0, result.output
        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            alias=None,
            skip_plan=False,
            env_vars={"DB_HOST": "prod.analytics.internal", "AWS_SECRET_KEY": "shhh"},
        )

    def test_deploy_env_file_shell_value_wins_over_file(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        monkeypatch,
        mock_server_poll,
    ):
        monkeypatch.setenv("DB_HOST", "from-shell")
        monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project") as project_dir:
            (project_dir / ".env").write_text(
                "DB_HOST=from-file\nAWS_SECRET_KEY=from-file\n"
            )
            result = runner.invoke(
                [
                    "dcm",
                    "deploy",
                    "fooBar",
                    "--env-file",
                    str(project_dir / ".env"),
                ]
            )

        assert result.exit_code == 0, result.output
        mock_dcm_manager().deploy_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            alias=None,
            skip_plan=False,
            env_vars={"DB_HOST": "from-shell", "AWS_SECRET_KEY": "from-file"},
        )

    def test_deploy_env_file_missing_fails_even_with_no_declared_env_vars(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
    ):
        """A user who typed --env-file wants feedback on it regardless of
        whether the manifest happens to declare any env vars."""
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project") as project_dir:
            result = runner.invoke(
                [
                    "dcm",
                    "deploy",
                    "fooBar",
                    "--env-file",
                    str(project_dir / "does_not_exist.env"),
                ]
            )

        assert result.exit_code == 1, result.output
        # The error is rendered in a word-wrapped Rich panel, so a long path
        # (e.g. Windows temp dirs) can split "was not found" across two
        # bordered lines -- collapse border/whitespace before checking.
        normalized_output = " ".join(result.output.replace("|", " ").split())
        assert "was not found" in normalized_output
        mock_dcm_manager().deploy_async.assert_not_called()

    def test_deploy_env_file_rejects_stage_path(
        self, mock_dcm_manager, mock_manifest_load, runner, project_directory
    ):
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "deploy", "fooBar", "--env-file", "@my_stage"]
            )

        assert result.exit_code == 1, result.output
        assert "Stage paths are not supported" in result.output

    @pytest.mark.parametrize(
        "extra_args,expected_labels",
        [
            ([], ["RENDER", "COMPILE", "PLAN", "DEPLOY"]),
            (["--skip-plan"], ["RENDER", "COMPILE", "DEPLOY"]),
        ],
        ids=["with_plan", "skip_plan"],
    )
    def test_deploy_tracks_plan_step_only_when_planning(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        extra_args,
        expected_labels,
    ):
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar", *extra_args])

        assert result.exit_code == 0, result.output
        server_steps = mock_server_poll.call_args.args[2]
        assert [step.label for step in server_steps] == expected_labels
        assert f"Step 1/{len(expected_labels) + 1} - UPLOAD" in result.output


class TestDCMPurge:
    @pytest.mark.parametrize(
        "project_identifier,user_inputs,expected_prompt_count",
        [
            ("fooBar", ["purge fooBar"], 1),
            ("fooBar", ["PURGE FOOBAR"], 1),
            ("fooBar", ["invalid", "purge", "purge wrong_project", "purge fooBar"], 4),
            (
                "fooBar",
                ["purge wrong_project", "purge different_project", "purge fooBar"],
                3,
            ),
            ("fooBar", ["purge ", "purge  ", "purge fooBar"], 3),
            (
                '"my db"."my schema"."my project"',
                ['purge "my db"."my schema"."my project"'],
                1,
            ),
        ],
    )
    @mock.patch("snowflake.cli._plugins.dcm.commands.typer.prompt")
    def test_purge_confirmation_input_validation(
        self,
        mock_prompt,
        project_identifier,
        user_inputs,
        expected_prompt_count,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_prompt.side_effect = user_inputs
        mock_dcm_manager().purge_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "purge", project_identifier, "--interactive"]
            )

        assert result.exit_code == 0, result.output
        assert mock_prompt.call_count == expected_prompt_count
        mock_dcm_manager().purge_async.assert_called_once_with(
            project_identifier=FQN.from_string(project_identifier),
            alias=None,
            skip_plan=False,
        )

    @mock.patch(
        "snowflake.cli._plugins.dcm.commands.typer.prompt", return_value="purge fooBar"
    )
    def test_purge_project_with_alias(
        self,
        mock_prompt,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().purge_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "purge", "fooBar", "--alias", "my_alias", "--interactive"]
            )

        assert result.exit_code == 0, result.output

        mock_dcm_manager().purge_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            alias="my_alias",
            skip_plan=False,
        )

    def test_purge_wires_up_expected_progress_steps(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        mock_multistep_progress,
    ):
        # given
        mock_dcm_manager().purge_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_manifest_load.return_value = _manifest_without_config()

        # when
        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "purge", "fooBar", "--force"])

        # then
        assert result.exit_code == 0, result.output
        steps = mock_multistep_progress.call_args.args[0]
        assert [step.label for step in steps] == [
            "PLAN",
            "PURGE",
        ]

    @mock.patch(
        "snowflake.cli._plugins.dcm.commands.typer.prompt", return_value="cancel"
    )
    def test_purge_cancel(
        self,
        mock_prompt,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "purge", "fooBar", "--interactive"])

        assert result.exit_code != 0
        mock_dcm_manager().purge_async.assert_not_called()

    @mock.patch(
        "snowflake.cli._plugins.dcm.commands.typer.prompt",
        return_value="purge my_project",
    )
    def test_purge_with_target_flag(
        self,
        mock_prompt,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().purge_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "purge", "--target", "dev", "--interactive"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().purge_async.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            alias=None,
            skip_plan=False,
        )

    @mock.patch(
        "snowflake.cli._plugins.dcm.commands.typer.prompt",
        side_effect=["purge wrong_project", "purge fooBar"],
    )
    def test_purge_shows_mismatch_message(
        self,
        mock_prompt,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().purge_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "purge", "fooBar", "--interactive"])

        assert result.exit_code == 0, result.output
        assert "  Project identifier mismatch" in result.output
        assert "Expected: fooBar" in result.output
        assert "provided: wrong_project" in result.output
        mock_dcm_manager().purge_async.assert_called_once()

    @mock.patch(
        "snowflake.cli._plugins.dcm.commands.typer.prompt", return_value="purge fooBar"
    )
    def test_purge_with_save_output(
        self,
        mock_prompt,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        tmp_path,
    ):
        plan_response = {"version": 2, "changeset": []}
        mock_dcm_manager().purge_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(
            mock_cursor, json.dumps(plan_response)
        )
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(
                ["dcm", "purge", "fooBar", "--save-output", "--interactive"]
            )

            assert result.exit_code == 0, result.output
            _assert_json_dumped("purge", plan_response, tmp_path)

    @pytest.mark.parametrize("extra_args", [[], ["--no-interactive"]])
    @mock.patch("snowflake.cli._plugins.dcm.commands._confirm_purge")
    def test_purge_with_force_skips_prompt(
        self,
        mock_confirm_purge,
        extra_args,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().purge_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "purge", "fooBar", "--force", *extra_args])

        assert result.exit_code == 0, result.output
        mock_confirm_purge.assert_not_called()
        mock_dcm_manager().purge_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            alias=None,
            skip_plan=False,
        )

    def test_purge_no_interactive_without_force_fails(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "purge", "fooBar", "--no-interactive"])

        assert result.exit_code != 0
        assert (
            "Cannot purge the DCM project non-interactively without --force"
            in result.output
        )
        mock_dcm_manager().purge_async.assert_not_called()

    @mock.patch(
        "snowflake.cli.api.commands.flags.is_tty_interactive", return_value=False
    )
    def test_purge_default_non_tty_without_force_fails(
        self,
        mock_is_tty,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "purge", "fooBar"])

        assert result.exit_code != 0
        assert (
            "Cannot purge the DCM project non-interactively without --force"
            in result.output
        )
        mock_dcm_manager().purge_async.assert_not_called()

    @mock.patch(
        "snowflake.cli.api.commands.flags.is_tty_interactive", return_value=True
    )
    @mock.patch(
        "snowflake.cli._plugins.dcm.commands.typer.prompt", return_value="purge fooBar"
    )
    def test_purge_default_tty_shows_prompt(
        self,
        mock_prompt,
        mock_is_tty,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().purge_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "purge", "fooBar"])

        assert result.exit_code == 0, result.output
        mock_prompt.assert_called()
        mock_dcm_manager().purge_async.assert_called_once()

    @pytest.mark.parametrize(
        "extra_args,expected_labels",
        [
            ([], ["PLAN", "PURGE"]),
            (["--skip-plan"], ["PURGE"]),
        ],
        ids=["with_plan", "skip_plan"],
    )
    def test_purge_tracks_plan_step_only_when_planning(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        extra_args,
        expected_labels,
    ):
        mock_dcm_manager().purge_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "purge", "fooBar", "--force", *extra_args])

        assert result.exit_code == 0, result.output
        server_steps = mock_server_poll.call_args.args[2]
        assert [step.label for step in server_steps] == expected_labels


class TestDCMPlan:
    def test_plan_project(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                [
                    "dcm",
                    "plan",
                    "fooBar",
                    "-D",
                    "key=value",
                ]
            )
        assert result.exit_code == 0, result.output

        mock_dcm_manager().plan_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=["key=value"],
            delta=False,
            output_path=None,
            env_vars={},
        )

    def test_plan_wires_up_expected_progress_steps(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        mock_multistep_progress,
    ):
        # given
        expected_labels = ["RENDER", "COMPILE", "PLAN"]
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        # when
        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "plan", "fooBar"])

        # then
        assert result.exit_code == 0, result.output
        steps = mock_multistep_progress.call_args.args[0]
        assert [step.label for step in steps] == ["UPLOAD", *expected_labels]
        assert f"Step 1/{len(expected_labels) + 1} - UPLOAD" in result.output
        _conn, _progress, server_steps, sfqid = mock_server_poll.call_args.args
        assert [step.label for step in server_steps] == expected_labels
        assert sfqid == TEST_SFQID

    def test_plan_project_with_delta(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                [
                    "dcm",
                    "plan",
                    "fooBar",
                    "--delta",
                ]
            )
        assert result.exit_code == 0, result.output

        mock_dcm_manager().plan_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            delta=True,
            output_path=None,
            env_vars={},
        )

    def test_plan_project_with_save_output(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        mock_output_stage,
    ):
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                [
                    "dcm",
                    "plan",
                    "fooBar",
                    "--save-output",
                ]
            )
        assert result.exit_code == 0, result.output

        mock_dcm_manager().plan_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            delta=False,
            output_path=_created_output_path(mock_output_stage),
            env_vars={},
        )

    def test_plan_drains_the_output_stage_only_after_the_query_finishes(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        mock_output_stage,
    ):
        """The backend writes to OUTPUT_PATH while the async query runs, so the
        stage may only be drained - which is what closing output_stage does - once
        the poll has seen the query finish."""
        calls: list[str] = []

        def record_poll():
            calls.append("poll")
            return _plan_cursor(mock_cursor)

        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.side_effect = record_poll
        mock_output_stage.get_recursive.side_effect = lambda **kwargs: calls.append(
            "drain"
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "plan", "fooBar", "--save-output"])

        assert result.exit_code == 0, result.output
        assert calls == ["poll", "drain"]

    def test_plan_leaves_the_output_stage_alone_when_it_stops_tracking_the_query(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_connect,
        mock_server_poll,
        mock_output_stage,
    ):
        """Giving up on a query whose status Snowflake never reported leaves it
        running, so there is nothing to collect: draining the stage would snapshot a
        run in flight and announce it as the artifacts of a finished one."""
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.side_effect = QueryStatusUnavailableCliError(
            f"Snowflake reported no status for query {TEST_SFQID}, so its progress "
            "cannot be tracked. The operation may still be running."
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "plan", "fooBar", "--save-output"])

        assert result.exit_code != 0
        assert "may still be running" in result.output
        mock_output_stage.get_recursive.assert_not_called()
        assert "Artifacts saved to" not in result.output

    def test_plan_project_with_from_stage_fails(
        self, mock_dcm_manager, runner, project_directory
    ):
        result = runner.invoke(["dcm", "plan", "fooBar", "--from", "@my_stage"])
        assert result.exit_code == 1, result.output
        assert "Stage paths are not supported" in result.output

    def test_plan_project_with_sync(
        self,
        mock_dcm_manager,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        """Test that files are synced to project stage when from_stage is not provided."""
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "plan", "my_project"])
            assert result.exit_code == 0, result.output

            call_args = mock_dcm_manager().plan_async.call_args
            assert "DCM_FOOBAR_" in call_args.kwargs["from_stage"]
            assert call_args.kwargs["from_stage"].endswith("_TMP_STAGE")

    def test_plan_project_with_from_local_directory(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        tmp_path,
    ):
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )
        mock_manifest_load.return_value = _manifest_without_config()

        source_dir = tmp_path / "source_project"
        source_dir.mkdir()
        manifest_file = source_dir / "manifest.yml"
        manifest_file.write_text("type: dcm_project\n")

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "plan", "my_project", "--from", str(source_dir)]
            )
            assert result.exit_code == 0, result.output

        mock_dcm_manager().sync_local_files.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            source_directory=str(source_dir),
            progress=mock.ANY,
            assets=[],
        )

        call_args = mock_dcm_manager().plan_async.call_args
        assert call_args.kwargs["from_stage"].endswith("_TMP_STAGE")

    def test_plan_with_save_output_saves_response(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        tmp_path,
    ):
        plan_response = {"version": 2, "changeset": []}
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(
            mock_cursor, json.dumps(plan_response)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "plan", "fooBar", "--save-output"])

            assert result.exit_code == 0, result.output
            _assert_json_dumped("plan", plan_response, tmp_path)

    def test_plan_with_save_output_drops_a_previous_runs_result_file(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_connect,
        mock_server_poll,
        tmp_path,
    ):
        """A result file left by an earlier run is cleared at command entry, so a
        run that writes nothing back cannot leave last run's output behind."""
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.side_effect = CliError("plan blew up")
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            out_dir = tmp_path / "out"
            out_dir.mkdir()
            (out_dir / "plan_result.json").write_text(
                '{"from": "an earlier --save-output run"}'
            )

            result = runner.invoke(["dcm", "plan", "fooBar", "--save-output"])

            assert result.exit_code != 0
            assert not (out_dir / "plan_result.json").exists()

    def test_plan_without_save_output_leaves_previous_artifacts_alone(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        tmp_path,
    ):
        """A run that writes nothing back must not delete an earlier run's output."""
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(
            mock_cursor, json.dumps({"version": 2, "changeset": []})
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            out_dir = tmp_path / "out"
            out_dir.mkdir()
            previous = out_dir / "plan_result.json"
            previous.write_text('{"from": "an earlier --save-output run"}')

            result = runner.invoke(["dcm", "plan", "fooBar"])

            assert result.exit_code == 0, result.output
            assert json.loads(previous.read_text()) == {
                "from": "an earlier --save-output run"
            }

    def test_plan_announces_artifacts_once(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        tmp_path,
    ):
        plan_response = {"version": 2, "changeset": []}
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(
            mock_cursor, json.dumps(plan_response)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "plan", "fooBar", "--save-output"])

            assert result.exit_code == 0, result.output
            assert result.output.count("Artifacts saved to") == 1

    def test_plan_announces_artifacts_when_the_response_cannot_be_parsed(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        tmp_path,
    ):
        """The reporter writes the result file before it fails to parse the
        response, so the user must still be told where it landed."""
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(
            mock_cursor, json.dumps({"not": "a plan response"})
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "plan", "fooBar", "--save-output"])

            assert result.exit_code != 0
            assert (tmp_path / "out" / "plan_result.json").exists()
            assert "Artifacts saved to" in result.output

    def test_plan_does_not_announce_without_save_output(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        tmp_path,
    ):
        plan_response = {"version": 2, "changeset": []}
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(
            mock_cursor, json.dumps(plan_response)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "plan", "fooBar"])

            assert result.exit_code == 0, result.output
            assert "Artifacts saved to" not in result.output

    def test_plan_announces_artifacts_downloaded_on_the_failure_path(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_connect,
        mock_server_poll,
        mock_output_stage,
        tmp_path,
    ):
        """output_stage downloads best-effort when the command fails, so the
        user must still be told where those artifacts landed."""

        def download_diagnostics(*args, **kwargs):
            out_dir = Path.cwd() / "out"
            out_dir.mkdir(exist_ok=True)
            (out_dir / "plan_result.json").write_text('{"errors": ["compile failed"]}')

        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.side_effect = CliError("plan blew up")
        mock_output_stage.get_recursive.side_effect = download_diagnostics
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "plan", "fooBar", "--save-output"])

            assert result.exit_code != 0
            assert (tmp_path / "out" / "plan_result.json").exists()
            assert "Artifacts saved to" in result.output

    def test_plan_does_not_announce_when_nothing_was_produced(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_connect,
        mock_server_poll,
        tmp_path,
    ):
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.side_effect = CliError("plan blew up")
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "plan", "fooBar", "--save-output"])

            assert result.exit_code != 0
            assert "Artifacts saved to" not in result.output

    def test_plan_with_save_output_keeps_downloaded_result_file(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        mock_output_stage,
        tmp_path,
    ):
        downloaded_result = {
            "version": 2,
            "changeset": [],
            "downloaded_by_backend": True,
        }
        plan_response = {"version": 2, "changeset": []}

        def download_result_file(*args, **kwargs):
            out_dir = Path.cwd() / "out"
            out_dir.mkdir(exist_ok=True)
            (out_dir / "plan_result.json").write_text(json.dumps(downloaded_result))

        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(
            mock_cursor, json.dumps(plan_response)
        )
        mock_output_stage.get_recursive.side_effect = download_result_file
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "plan", "fooBar", "--save-output"])

            assert result.exit_code == 0, result.output
            _assert_json_dumped("plan", downloaded_result, tmp_path)

    @pytest.mark.parametrize("format_name", ["json", "json_ext"])
    def test_plan_with_json_formats_returns_response(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        project_directory,
        format_name,
    ):
        plan_response = {"version": 2, "changeset": []}
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _mock_cursor_for_format(
            mock_cursor, plan_response, format_name
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "plan", "fooBar", "--format", format_name])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        _assert_format_result(payload, plan_response, format_name)

    def test_plan_collects_declared_env_vars_from_shell(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        monkeypatch,
    ):
        monkeypatch.setenv("DB_HOST", "prod.analytics.internal")
        monkeypatch.setenv("AWS_SECRET_KEY", "shhh")
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "plan", "fooBar"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().plan_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            delta=False,
            output_path=None,
            env_vars={"DB_HOST": "prod.analytics.internal", "AWS_SECRET_KEY": "shhh"},
        )

    def test_plan_omits_declared_env_var_missing_from_shell(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        monkeypatch,
    ):
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project"):
            with mock.patch(
                "snowflake.cli._plugins.dcm.env.cli_console"
            ) as mock_console:
                result = runner.invoke(["dcm", "plan", "fooBar"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().plan_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            delta=False,
            output_path=None,
            env_vars={},
        )
        mock_console.warning.assert_called_once()

    def test_plan_reads_env_vars_from_env_file(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
        monkeypatch,
    ):
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
        mock_dcm_manager().plan_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project") as project_dir:
            (project_dir / ".env").write_text(
                "DB_HOST=prod.analytics.internal\nAWS_SECRET_KEY=shhh\n"
            )
            result = runner.invoke(
                ["dcm", "plan", "fooBar", "--env-file", str(project_dir / ".env")]
            )

        assert result.exit_code == 0, result.output
        mock_dcm_manager().plan_async.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            delta=False,
            output_path=None,
            env_vars={"DB_HOST": "prod.analytics.internal", "AWS_SECRET_KEY": "shhh"},
        )


class TestDCMRawAnalyze:
    def test_raw_analyze_basic(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "raw-analyze", "fooBar"])
        assert result.exit_code == 0, result.output

        mock_dcm_manager().raw_analyze.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            output_path=None,
            env_vars={},
        )

    def test_raw_analyze_wires_up_expected_progress_steps(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_multistep_progress,
    ):
        # given
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        # when
        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "raw-analyze", "fooBar"])

        # then
        assert result.exit_code == 0, result.output
        steps = mock_multistep_progress.call_args.args[0]
        assert [step.label for step in steps] == ["UPLOAD", "ANALYZE"]

    def test_raw_analyze_collects_declared_env_vars_from_shell(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        monkeypatch,
    ):
        monkeypatch.setenv("DB_HOST", "prod.analytics.internal")
        monkeypatch.setenv("AWS_SECRET_KEY", "shhh")
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "raw-analyze", "fooBar"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().raw_analyze.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            output_path=None,
            env_vars={"DB_HOST": "prod.analytics.internal", "AWS_SECRET_KEY": "shhh"},
        )

    def test_raw_analyze_omits_declared_env_var_missing_from_shell(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        monkeypatch,
    ):
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project"):
            with mock.patch(
                "snowflake.cli._plugins.dcm.env.cli_console"
            ) as mock_console:
                result = runner.invoke(["dcm", "raw-analyze", "fooBar"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().raw_analyze.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            output_path=None,
            env_vars={},
        )
        mock_console.warning.assert_called_once()

    def test_raw_analyze_reads_env_vars_from_env_file(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        monkeypatch,
    ):
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project") as project_dir:
            (project_dir / ".env").write_text(
                "DB_HOST=prod.analytics.internal\nAWS_SECRET_KEY=shhh\n"
            )
            result = runner.invoke(
                [
                    "dcm",
                    "raw-analyze",
                    "fooBar",
                    "--env-file",
                    str(project_dir / ".env"),
                ]
            )

        assert result.exit_code == 0, result.output
        mock_dcm_manager().raw_analyze.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            output_path=None,
            env_vars={"DB_HOST": "prod.analytics.internal", "AWS_SECRET_KEY": "shhh"},
        )

    def test_raw_analyze_with_issues_exits(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        issue_response = _analyze_response(
            files=[
                {
                    "source_path": "sources/definitions/bad.sql",
                    "definitions": [],
                    "issues": [{"message": "syntax error", "severity": "ERROR"}],
                }
            ]
        )
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(issue_response,)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "raw-analyze", "fooBar"])
        assert result.exit_code == 1, result.output
        assert "1 error(s)" in result.output

    def test_raw_analyze_with_variables(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "raw-analyze", "fooBar", "-D", "key=value"])
        assert result.exit_code == 0, result.output

        mock_dcm_manager().raw_analyze.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=["key=value"],
            output_path=None,
            env_vars={},
        )

    def test_raw_analyze_with_target(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "raw-analyze", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().raw_analyze.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            output_path=None,
            env_vars={},
        )

    def test_raw_analyze_with_default_target(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "raw-analyze"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().raw_analyze.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            output_path=None,
            env_vars={},
        )

    def test_raw_analyze_explicit_identifier_with_target_config(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        """When explicit identifier is provided, it overrides target's project_name
        but configuration from target should still be applied."""
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "default_target": "dev",
                "targets": {
                    "dev": {
                        "project_name": "target_project",
                        "templating_config": "dev_config",
                        **_DEFAULT_TARGET_FIELDS,
                    }
                },
                "templating": {"configurations": {"dev_config": {}}},
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "raw-analyze", "explicit_project", "--target", "dev"]
            )

        assert result.exit_code == 0, result.output
        mock_dcm_manager().raw_analyze.assert_called_once_with(
            project_identifier=FQN.from_string("explicit_project"),
            configuration="DEV_CONFIG",
            from_stage="TMP_STAGE",
            variables=None,
            output_path=None,
            env_vars={},
        )

    def test_raw_analyze_with_from_local_directory(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        tmp_path,
    ):
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )
        mock_manifest_load.return_value = _manifest_without_config()

        source_dir = tmp_path / "source_project"
        source_dir.mkdir()
        manifest_file = source_dir / "manifest.yml"
        manifest_file.write_text("type: dcm_project\n")

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "raw-analyze", "my_project", "--from", str(source_dir)]
            )
            assert result.exit_code == 0, result.output

        mock_dcm_manager().sync_local_files.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            source_directory=str(source_dir),
            progress=mock.ANY,
            assets=[],
        )

        call_args = mock_dcm_manager().raw_analyze.call_args
        assert call_args.kwargs["from_stage"].endswith("_TMP_STAGE")

    def test_raw_analyze_with_sync(
        self,
        mock_dcm_manager,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        """Test that files are synced to project stage when from_stage is not provided."""
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = (
            "MockDatabase.MockSchema.DCM_FOOBAR_1234567890_TMP_STAGE"
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "raw-analyze", "my_project"])
            assert result.exit_code == 0, result.output

        call_args = mock_dcm_manager().raw_analyze.call_args
        assert "DCM_FOOBAR_" in call_args.kwargs["from_stage"]
        assert call_args.kwargs["from_stage"].endswith("_TMP_STAGE")

    def test_raw_analyze_with_save_output(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_output_stage,
    ):
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(_analyze_response(),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                [
                    "dcm",
                    "raw-analyze",
                    "fooBar",
                    "--save-output",
                ]
            )
        assert result.exit_code == 0, result.output

        mock_dcm_manager().raw_analyze.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            output_path=_created_output_path(mock_output_stage),
            env_vars={},
        )

    def test_raw_analyze_clears_stale_compile_result_before_running(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_connect,
        tmp_path,
    ):
        """A compile_result.json left by a previous run must not survive as if it
        described this run."""
        mock_dcm_manager().raw_analyze.side_effect = CliError("analyze blew up")
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            out_dir = tmp_path / "out"
            out_dir.mkdir()
            (out_dir / "compile_result.json").write_text('{"stale": "previous run"}')

            result = runner.invoke(["dcm", "raw-analyze", "fooBar", "--save-output"])

            assert result.exit_code != 0
            assert not (out_dir / "compile_result.json").exists()

    def test_raw_analyze_with_save_output_writes_compile_result(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        tmp_path,
    ):
        """raw-analyze's result file is compile_result.json, matching the file the
        backend itself writes."""
        analyze_response = json.loads(_analyze_response())
        mock_dcm_manager().raw_analyze.return_value = mock_cursor(
            rows=[(json.dumps(analyze_response),)], columns=("result",)
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "raw-analyze", "fooBar", "--save-output"])

            assert result.exit_code == 0, result.output
            _assert_json_dumped("compile", analyze_response, tmp_path)
            assert not (tmp_path / "out" / "raw-analyze_result.json").exists()

    def test_raw_analyze_with_save_output_keeps_downloaded_result_file(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        tmp_path,
    ):
        downloaded_result = {"files": [], "downloaded_by_backend": True}
        analyze_response = {"files": []}

        def raw_analyze_downloading_result_file(*args, **kwargs):
            out_dir = Path.cwd() / "out"
            out_dir.mkdir(exist_ok=True)
            (out_dir / "compile_result.json").write_text(json.dumps(downloaded_result))
            return mock_cursor(
                rows=[(json.dumps(analyze_response),)], columns=("result",)
            )

        mock_dcm_manager().raw_analyze.side_effect = raw_analyze_downloading_result_file
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "raw-analyze", "fooBar", "--save-output"])

            assert result.exit_code == 0, result.output
            _assert_json_dumped("compile", downloaded_result, tmp_path)

    def test_raw_analyze_from_stage_fails(
        self, mock_dcm_manager, runner, project_directory
    ):
        result = runner.invoke(["dcm", "raw-analyze", "fooBar", "--from", "@my_stage"])
        assert result.exit_code == 1, result.output
        assert "Stage paths are not supported" in result.output

    def test_raw_analyze_hidden_from_help(self, runner):
        """Test that raw-analyze command is hidden from DCM help output."""
        result = runner.invoke(["dcm", "--help"])
        assert result.exit_code == 0
        assert "raw-analyze" not in result.output

    @pytest.mark.parametrize("format_name", ["json", "json_ext"])
    def test_raw_analyze_with_json_formats_returns_response(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        mock_connect,
        project_directory,
        format_name,
    ):
        analyze_response = _analyze_response()
        mock_dcm_manager().raw_analyze.return_value = _mock_cursor_for_format(
            mock_cursor, json.loads(analyze_response), format_name
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "raw-analyze", "fooBar", "--format", format_name]
            )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        _assert_format_result(payload, json.loads(analyze_response), format_name)


class TestDCMList:
    def test_list_command_alias(self, mock_connect, runner):
        result = runner.invoke(
            [
                "object",
                "list",
                "dcm",
                "--like",
                "%PROJECT_NAME%",
                "--in",
                "database",
                "my_db",
            ]
        )

        assert result.exit_code == 0, result.output
        result = runner.invoke(
            ["dcm", "list", "--like", "%PROJECT_NAME%", "--in", "database", "my_db"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 2
        assert (
            queries[0]
            == queries[1]
            == "show DCM Projects like '%PROJECT_NAME%' in database my_db"
        )

    @pytest.mark.parametrize(
        "terse, limit, expected_query_suffix",
        [
            (True, None, "show terse DCM Projects like '%%'"),
            (False, 10, "show DCM Projects like '%%' limit 10"),
            (False, 5, "show DCM Projects like '%%' limit 5"),
            (True, 10, "show terse DCM Projects like '%%' limit 10"),
        ],
    )
    def test_dcm_list_with_terse_and_limit_options(
        self, mock_connect, terse, limit, expected_query_suffix, runner
    ):
        """Test DCM list command with TERSE and LIMIT options."""
        cmd = ["dcm", "list"]

        if terse:
            cmd.extend(["--terse"])
        if limit is not None:
            cmd.extend(["--limit", str(limit)])

        result = runner.invoke(cmd, catch_exceptions=False)
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 1
        assert queries[0] == expected_query_suffix

    def test_dcm_list_with_all_options_combined(self, mock_connect, runner):
        """Test DCM list command with all options (like, scope, terse, limit) combined."""
        result = runner.invoke(
            [
                "dcm",
                "list",
                "--like",
                "test%",
                "--in",
                "database",
                "my_db",
                "--terse",
                "--limit",
                "20",
            ],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 1
        expected_query = (
            "show terse DCM Projects like 'test%' in database my_db limit 20"
        )
        assert queries[0] == expected_query


class TestDCMListDeployments:
    def test_list_deployments(self, mock_dcm_manager, runner):
        result = runner.invoke(["dcm", "list-deployments", "fooBar"])

        assert result.exit_code == 0, result.output

        mock_dcm_manager().list_deployments.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar")
        )

    def test_list_deployments_with_target_flag(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        project_directory,
    ):
        mock_dcm_manager().list_deployments.return_value = mock_cursor(
            rows=[], columns=("name",)
        )
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "list-deployments", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().list_deployments.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )


class TestDCMDropDeployment:
    @pytest.mark.parametrize("if_exists", [True, False])
    def test_drop_deployment(self, mock_dcm_manager, runner, if_exists):
        command = ["dcm", "drop-deployment", "fooBar", "--deployment", "v1"]
        if if_exists:
            command.append("--if-exists")

        result = runner.invoke(command)

        assert result.exit_code == 0, result.output
        assert "Deployment 'v1' dropped from DCM Project 'fooBar'" in result.output

        mock_dcm_manager().drop_deployment.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            deployment_name="v1",
            if_exists=if_exists,
        )

    @pytest.mark.parametrize(
        "deployment_name,should_warn",
        [
            ("deployment", True),
            ("DEPLOYMENT", True),
            ("Deployment", True),
            ("DEPLOYMENT$1", False),
            ("v1", False),
            ("my_deployment", False),
            ("deployment1", False),
            ("actual_deployment", False),
        ],
    )
    def test_drop_deployment_shell_expansion_warning(
        self, mock_dcm_manager, runner, deployment_name, should_warn
    ):
        """Test that warning is displayed for deployment names that look like shell expansion results."""
        result = runner.invoke(
            ["dcm", "drop-deployment", "fooBar", "--deployment", deployment_name]
        )

        assert result.exit_code == 0, result.output

        if should_warn:
            assert "might be truncated due to shell expansion" in result.output
            assert "try using single quotes" in result.output
        else:
            assert "might be truncated due to shell expansion" not in result.output

        mock_dcm_manager().drop_deployment.assert_called_once_with(
            project_identifier=FQN.from_string("fooBar"),
            deployment_name=deployment_name,
            if_exists=False,
        )


class TestDCMDrop:
    def test_drop_project(self, mock_connect, runner):
        result = runner.invoke(
            [
                "object",
                "drop",
                "dcm",
                "my_project",
            ]
        )

        assert result.exit_code == 0, result.output

        result = runner.invoke(
            ["dcm", "drop", "my_project"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 2
        assert queries[0] == queries[1] == "drop DCM Project IDENTIFIER('my_project')"

    def test_drop_with_target_flag(
        self,
        mock_object_manager,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        project_directory,
    ):
        mock_object_manager().drop.return_value = mock_cursor(
            rows=[], columns=("status",)
        )
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "drop", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_object_manager().drop.assert_called_once_with(
            object_type="dcm",
            fqn=FQN.from_string("my_project"),
            if_exists=False,
        )


class TestDCMDescribe:
    def test_describe_command_alias(self, mock_connect, runner):
        result = runner.invoke(
            [
                "object",
                "describe",
                "dcm",
                "PROJECT_NAME",
            ]
        )

        assert result.exit_code == 0, result.output
        result = runner.invoke(
            ["dcm", "describe", "PROJECT_NAME"],
            catch_exceptions=False,
        )
        assert result.exit_code == 0, result.output

        queries = mock_connect.mocked_ctx.get_queries()
        assert len(queries) == 2
        assert (
            queries[0]
            == queries[1]
            == "describe DCM Project IDENTIFIER('PROJECT_NAME')"
        )

    def test_describe_with_target_flag(
        self,
        mock_object_manager,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        project_directory,
    ):
        mock_object_manager().describe.return_value = mock_cursor(
            rows=[], columns=("name",)
        )
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "describe", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_object_manager().describe.assert_called_once_with(
            object_type="dcm",
            fqn=FQN.from_string("my_project"),
        )


class TestDCMPreview:
    def test_preview_basic(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
    ):
        mock_dcm_manager().preview.return_value = mock_cursor(
            rows=[(1, "Alice", "alice@example.com"), (2, "Bob", "bob@example.com")],
            columns=("id", "name", "email"),
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "preview", "my_project", "--object", "my_table"]
            )

        assert result.exit_code == 0, result.output

        mock_dcm_manager().preview.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            object_identifier=FQN.from_string("my_table"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            limit=None,
            env_vars={},
        )

    def test_preview_wires_up_expected_progress_steps(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_multistep_progress,
    ):
        # given
        mock_dcm_manager().preview.return_value = mock_cursor(
            rows=[(1, "Alice", "alice@example.com")],
            columns=("id", "name", "email"),
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        # when
        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "preview", "my_project", "--object", "my_table"]
            )

        # then
        assert result.exit_code == 0, result.output
        steps = mock_multistep_progress.call_args.args[0]
        assert [step.label for step in steps] == ["UPLOAD", "PREVIEW"]

    def test_preview_with_from_stage_fails(
        self, mock_dcm_manager, runner, project_directory
    ):
        result = runner.invoke(
            [
                "dcm",
                "preview",
                "my_project",
                "--object",
                "my_table",
                "--from",
                "@my_stage",
            ]
        )
        assert result.exit_code == 1, result.output
        assert "Stage paths are not supported" in result.output

    @pytest.mark.parametrize(
        "extra_args,expected_vars,expected_limit",
        [
            (
                ["-D", "key=value", "--limit", "10"],
                ["key=value"],
                10,
            ),
            (
                ["-D", "var1=val1", "-D", "var2=val2", "--limit", "5"],
                ["var1=val1", "var2=val2"],
                5,
            ),
            (
                ["--limit", "100"],
                None,
                100,
            ),
        ],
    )
    def test_preview_with_various_options(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        extra_args,
        expected_vars,
        expected_limit,
    ):
        mock_dcm_manager().preview.return_value = mock_cursor(
            rows=[(1, "Alice", "alice@example.com")],
            columns=("id", "name", "email"),
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(
                [
                    "dcm",
                    "preview",
                    "my_project",
                    "--object",
                    "my_table",
                ]
                + extra_args
            )
        assert result.exit_code == 0, result.output

        mock_dcm_manager().preview.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            object_identifier=FQN.from_string("my_table"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=expected_vars,
            limit=expected_limit,
            env_vars={},
        )

    def test_preview_without_object_fails(self, runner, project_directory):
        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "preview", "my_project"])

        assert result.exit_code == 2
        assert "Missing option '--object'" in result.output

    def test_preview_collects_declared_env_vars_from_shell(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        monkeypatch,
    ):
        monkeypatch.setenv("DB_HOST", "prod.analytics.internal")
        monkeypatch.setenv("AWS_SECRET_KEY", "shhh")
        mock_dcm_manager().preview.return_value = mock_cursor(
            rows=[(1, "Alice", "alice@example.com")],
            columns=("id", "name", "email"),
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project"):
            result = runner.invoke(
                ["dcm", "preview", "my_project", "--object", "my_table"]
            )

        assert result.exit_code == 0, result.output
        mock_dcm_manager().preview.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            object_identifier=FQN.from_string("my_table"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            limit=None,
            env_vars={"DB_HOST": "prod.analytics.internal", "AWS_SECRET_KEY": "shhh"},
        )

    def test_preview_omits_declared_env_var_missing_from_shell(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        monkeypatch,
    ):
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
        mock_dcm_manager().preview.return_value = mock_cursor(
            rows=[(1, "Alice", "alice@example.com")],
            columns=("id", "name", "email"),
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project"):
            with mock.patch(
                "snowflake.cli._plugins.dcm.env.cli_console"
            ) as mock_console:
                result = runner.invoke(
                    ["dcm", "preview", "my_project", "--object", "my_table"]
                )

        assert result.exit_code == 0, result.output
        mock_dcm_manager().preview.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            object_identifier=FQN.from_string("my_table"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            limit=None,
            env_vars={},
        )
        mock_console.warning.assert_called_once()

    def test_preview_reads_env_vars_from_env_file(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        monkeypatch,
    ):
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("AWS_SECRET_KEY", raising=False)
        mock_dcm_manager().preview.return_value = mock_cursor(
            rows=[(1, "Alice", "alice@example.com")],
            columns=("id", "name", "email"),
        )
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_with_env_vars()

        with project_directory("dcm_project") as project_dir:
            (project_dir / ".env").write_text(
                "DB_HOST=prod.analytics.internal\nAWS_SECRET_KEY=shhh\n"
            )
            result = runner.invoke(
                [
                    "dcm",
                    "preview",
                    "my_project",
                    "--object",
                    "my_table",
                    "--env-file",
                    str(project_dir / ".env"),
                ]
            )

        assert result.exit_code == 0, result.output
        mock_dcm_manager().preview.assert_called_once_with(
            project_identifier=FQN.from_string("my_project"),
            object_identifier=FQN.from_string("my_table"),
            configuration=None,
            from_stage="TMP_STAGE",
            variables=None,
            limit=None,
            env_vars={"DB_HOST": "prod.analytics.internal", "AWS_SECRET_KEY": "shhh"},
        )


class TestDCMRefresh:
    def test_refresh_with_outdated_tables(
        self, mock_dcm_manager, runner, mock_cursor, snapshot
    ):
        refresh_result = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "JW_DCM_TESTALL.ANALYTICS.DYNAMIC_EMPLOYEES",
                        "data_timestamp": "1760357032.175",
                        "statistics": {
                            "inserted_rows": 12345,
                            "deleted_rows": 999999999995,
                        },
                    }
                ]
            }
        }
        mock_dcm_manager().refresh.return_value = mock_cursor(
            rows=[(json.dumps(refresh_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "refresh", "my_project"])

        assert result.exit_code == 0, result.output
        assert result.output == snapshot
        mock_dcm_manager().refresh.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    def test_refresh_with_fresh_tables(
        self, mock_dcm_manager, runner, mock_cursor, snapshot
    ):
        refresh_result = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "JW_DCM_TESTALL.ANALYTICS.DYNAMIC_EMPLOYEES",
                        "data_timestamp": "1760356974.543",
                        "statistics": {"inserted_rows": 0, "deleted_rows": 0},
                    }
                ]
            }
        }
        mock_dcm_manager().refresh.return_value = mock_cursor(
            rows=[(json.dumps(refresh_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "refresh", "my_project"])

        assert result.exit_code == 0, result.output
        assert result.output == snapshot
        mock_dcm_manager().refresh.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    def test_refresh_with_no_dynamic_tables(
        self, mock_dcm_manager, runner, mock_cursor, snapshot
    ):
        refresh_result = {"dts_refresh_result": {"refreshed_tables": []}}
        mock_dcm_manager().refresh.return_value = mock_cursor(
            rows=[(json.dumps(refresh_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "refresh", "my_project"])

        assert result.exit_code == 0, result.output
        assert result.output == snapshot
        mock_dcm_manager().refresh.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    def test_refresh_with_target_flag(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        project_directory,
    ):
        refresh_result = {"dts_refresh_result": {"refreshed_tables": []}}
        mock_dcm_manager().refresh.return_value = mock_cursor(
            rows=[(json.dumps(refresh_result),)], columns=("result",)
        )
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "refresh", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().refresh.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    def test_refresh_with_save_output(
        self,
        mock_dcm_manager,
        runner,
        mock_cursor,
        tmp_path,
    ):
        refresh_result = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "DB.SCHEMA.DYNAMIC_TABLE",
                        "statistics": {"inserted_rows": 0, "deleted_rows": 0},
                    }
                ]
            }
        }
        mock_dcm_manager().refresh.return_value = mock_cursor(
            rows=[(json.dumps(refresh_result),)], columns=("result",)
        )

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "refresh", "my_project", "--save-output"])

            assert result.exit_code == 0, result.output
            _assert_json_dumped("refresh", refresh_result, tmp_path)

    @pytest.mark.parametrize("format_name", ["json", "json_ext"])
    def test_refresh_with_json_formats_returns_response(
        self,
        mock_dcm_manager,
        runner,
        mock_cursor,
        format_name,
    ):
        refresh_result = {
            "dts_refresh_result": {
                "refreshed_tables": [
                    {
                        "table_name": "DB.SCHEMA.DYNAMIC_TABLE",
                        "statistics": {"inserted_rows": 0, "deleted_rows": 0},
                    }
                ]
            }
        }
        mock_dcm_manager().refresh.return_value = _mock_cursor_for_format(
            mock_cursor, refresh_result, format_name
        )

        result = runner.invoke(
            ["dcm", "refresh", "my_project", "--format", format_name]
        )

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        _assert_format_result(payload, refresh_result, format_name)


class TestDCMTest:
    def test_test_all_passing(self, mock_dcm_manager, runner, mock_cursor, snapshot):
        test_result = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.EMPLOYEES",
                    "expectation_name": "ROW_COUNT_CHECK",
                    "expectation_violated": False,
                },
                {
                    "table_name": "DB.SCHEMA.ORDERS",
                    "expectation_name": "NULL_CHECK",
                    "expectation_violated": False,
                },
            ]
        }
        mock_dcm_manager().test.return_value = mock_cursor(
            rows=[(json.dumps(test_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "test", "my_project"])

        assert result.exit_code == 0, result.output
        assert result.output == snapshot
        mock_dcm_manager().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    def test_test_with_failures(self, mock_dcm_manager, runner, mock_cursor, snapshot):
        test_result = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.EMPLOYEES",
                    "expectation_name": "ROW_COUNT_CHECK",
                    "expectation_violated": False,
                },
                {
                    "table_name": "DB.SCHEMA.ORDERS",
                    "expectation_name": "NULL_CHECK",
                    "expectation_violated": True,
                    "expectation_expression": "= 0",
                    "metric_name": "null_count",
                    "value": 15,
                },
            ]
        }
        mock_dcm_manager().test.return_value = mock_cursor(
            rows=[(json.dumps(test_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "test", "my_project"])

        assert result.exit_code == 1, result.output
        assert result.output == snapshot
        mock_dcm_manager().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    def test_test_no_expectations(
        self, mock_dcm_manager, runner, mock_cursor, snapshot
    ):
        test_result = {"expectations": []}
        mock_dcm_manager().test.return_value = mock_cursor(
            rows=[(json.dumps(test_result),)], columns=("result",)
        )

        result = runner.invoke(["dcm", "test", "my_project"])

        assert result.exit_code == 0, result.output
        assert result.output == snapshot
        mock_dcm_manager().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    def test_test_with_target_flag(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        runner,
        mock_cursor,
        project_directory,
    ):
        test_result = {"expectations": []}
        mock_dcm_manager().test.return_value = mock_cursor(
            rows=[(json.dumps(test_result),)], columns=("result",)
        )
        mock_manifest_load.return_value = DCMManifest.from_dict(
            {
                "manifest_version": 2,
                "type": "dcm_project",
                "targets": {
                    "dev": {"project_name": "my_project", **_DEFAULT_TARGET_FIELDS}
                },
            }
        )

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "test", "--target", "dev"])

        assert result.exit_code == 0, result.output
        mock_dcm_manager().test.assert_called_once_with(
            project_identifier=FQN.from_string("my_project")
        )

    def test_test_with_save_output(
        self,
        mock_dcm_manager,
        runner,
        mock_cursor,
        tmp_path,
    ):
        test_result = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.EMPLOYEES",
                    "expectation_name": "ROW_COUNT_CHECK",
                    "expectation_violated": False,
                }
            ]
        }
        mock_dcm_manager().test.return_value = mock_cursor(
            rows=[(json.dumps(test_result),)], columns=("result",)
        )

        with change_directory(tmp_path):
            result = runner.invoke(["dcm", "test", "my_project", "--save-output"])

            assert result.exit_code == 0, result.output
            _assert_json_dumped("test", test_result, tmp_path)

    @pytest.mark.parametrize("format_name", ["json", "json_ext"])
    def test_test_with_json_formats_returns_response(
        self,
        mock_dcm_manager,
        runner,
        mock_cursor,
        format_name,
    ):
        test_result = {
            "expectations": [
                {
                    "table_name": "DB.SCHEMA.EMPLOYEES",
                    "expectation_name": "ROW_COUNT_CHECK",
                    "expectation_violated": False,
                }
            ]
        }
        mock_dcm_manager().test.return_value = _mock_cursor_for_format(
            mock_cursor, test_result, format_name
        )

        result = runner.invoke(["dcm", "test", "my_project", "--format", format_name])

        assert result.exit_code == 0, result.output
        payload = json.loads(result.output)
        _assert_format_result(payload, test_result, format_name)


class TestAccountIdentifierValidationForCommands:
    def test_create_calls_check_account_identifier(
        self,
        mock_dcm_manager,
        mock_object_manager,
        mock_check_account_identifier,
        runner,
        project_directory,
    ):
        mock_object_manager().object_exists.return_value = False
        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "create", "my_project"])

        assert result.exit_code == 0, result.output
        mock_check_account_identifier.assert_called_once()

    def test_deploy_calls_check_account_identifier(
        self,
        mock_dcm_manager,
        mock_manifest_load,
        mock_check_account_identifier,
        runner,
        project_directory,
        mock_cursor,
        mock_connect,
        mock_server_poll,
    ):
        mock_dcm_manager().deploy_async.return_value = TEST_SFQID
        mock_server_poll.return_value.run.return_value = _plan_cursor(mock_cursor)
        mock_dcm_manager().sync_local_files.return_value = "TMP_STAGE"
        mock_manifest_load.return_value = _manifest_without_config()

        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "deploy", "fooBar"])

        assert result.exit_code == 0, result.output
        mock_check_account_identifier.assert_called_once()

    def test_no_validation_without_manifest(
        self,
        mock_dcm_manager,
        mock_object_manager,
        mock_check_account_identifier,
        runner,
    ):
        mock_object_manager().object_exists.return_value = False
        result = runner.invoke(["dcm", "create", "my_project"])

        assert result.exit_code == 0, result.output
        mock_check_account_identifier.assert_not_called()


class TestOwnershipValidationForCommands:
    """Tests for commands that require project owner validation."""

    def test_create_validates_ownership(
        self,
        mock_dcm_manager,
        mock_object_manager,
        mock_check_project_owner,
        runner,
        project_directory,
    ):
        mock_object_manager().object_exists.return_value = False
        with project_directory("dcm_project"):
            result = runner.invoke(["dcm", "create", "my_project"])

        assert result.exit_code == 0, result.output
        mock_check_project_owner.assert_called_once()

    def test_describe_no_ownership_validated(
        self,
        mock_check_project_owner,
        mock_connect,
        runner,
    ):
        runner.invoke(["dcm", "describe", "my_project"])

        mock_check_project_owner.assert_not_called()


@pytest.mark.parametrize(
    "manifest_account_identifier,session_account,should_warn",
    [
        (
            "MY_ORG-MY_ACCOUNT",
            AccountIdentifier("MY_ORG", "MY_ACCOUNT"),
            False,
        ),
        (
            "MY_ORG-MY_ACCOUNT",
            AccountIdentifier("OTHER_ORG", "OTHER_ACCOUNT"),
            True,
        ),
        (
            "MY_ORG.MY_ACCOUNT",
            AccountIdentifier("MY_ORG", "MY_ACCOUNT"),
            False,
        ),
        (
            "MY_ORG.MY_ACCOUNT",
            AccountIdentifier("OTHER_ORG", "OTHER_ACCOUNT"),
            True,
        ),
        (
            "my_org-my_account",
            AccountIdentifier("MY_ORG", "MY_ACCOUNT"),
            False,
        ),
        (
            "NO_SEPARATOR",
            AccountIdentifier("MY_ORG", "MY_ACCOUNT"),
            True,
        ),
    ],
    ids=[
        "hyphen_match",
        "hyphen_mismatch",
        "dot_match",
        "dot_mismatch",
        "case_insensitive_match",
        "no_separator",
    ],
)
@mock.patch("snowflake.cli._plugins.dcm.commands.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.commands.get_account_identifier")
@mock.patch("snowflake.cli._plugins.dcm.commands.get_cli_context")
def test_check_account_identifier(
    mock_ctx,
    mock_get_id,
    mock_console,
    manifest_account_identifier,
    session_account,
    should_warn,
):
    mock_get_id.return_value = session_account
    target = DCMTarget(
        name="DEV",
        project_name="P1",
        account_identifier=manifest_account_identifier,
        project_owner="MY_ROLE",
    )
    _check_account_identifier(target)
    if should_warn:
        mock_console.warning.assert_called_once()
        assert "Account mismatch" in mock_console.warning.call_args[0][0]
    else:
        mock_console.warning.assert_not_called()


@mock.patch("snowflake.cli._plugins.dcm.commands.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.commands.get_account_identifier")
@mock.patch("snowflake.cli._plugins.dcm.commands.get_cli_context")
def test_check_account_identifier_warns_on_get_account_identifier_error(
    mock_ctx, mock_get_id, mock_console
):
    mock_get_id.side_effect = Exception("Connection timeout")
    target = DCMTarget(
        name="DEV",
        project_name="P1",
        account_identifier="MY_ORG-MY_ACCOUNT",
        project_owner="MY_ROLE",
    )

    _check_account_identifier(target)

    mock_console.warning.assert_called_once()
    warning_message = mock_console.warning.call_args[0][0]
    assert "Cannot validate target's account identifier" in warning_message
    assert "Connection timeout" in warning_message
    assert "The current session account is required to match" in warning_message


@mock.patch("snowflake.cli._plugins.dcm.commands.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.commands.get_account_identifier")
@mock.patch("snowflake.cli._plugins.dcm.commands.get_cli_context")
def test_check_account_identifier_warns_when_target_account_identifier_is_empty(
    mock_ctx, mock_get_id, mock_console
):
    target = DCMTarget(
        name="DEV",
        project_name="P1",
        account_identifier="",
        project_owner="MY_ROLE",
    )

    _check_account_identifier(target)

    mock_get_id.assert_not_called()
    mock_console.warning.assert_called_once()
    warning_message = mock_console.warning.call_args[0][0]
    assert "account_identifier is not specified" in warning_message
    assert "The current session account is required to match" in warning_message


@pytest.mark.parametrize(
    "manifest_project_owner,session_role,should_warn",
    [
        ("MY_ROLE", "MY_ROLE", False),
        ("MY_ROLE", "my_role", False),
        ("FINANCE_ROLE", "ADMIN_ROLE", True),
        ('"my role"', '"my role"', False),
        ('"My Role"', '"my role"', True),
    ],
    ids=[
        "simple_match",
        "case_insensitive_match",
        "mismatch",
        "quoted_match",
        "quoted_mismatch",
    ],
)
@mock.patch("snowflake.cli._plugins.dcm.commands.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.commands.SqlExecutor")
def test_check_project_owner(
    mock_executor_cls,
    mock_console,
    manifest_project_owner,
    session_role,
    should_warn,
):
    mock_executor_cls().current_role.return_value = session_role
    target = DCMTarget(
        name="DEV",
        project_name="P1",
        account_identifier="MY_ORG-MY_ACCOUNT",
        project_owner=manifest_project_owner,
    )
    _check_project_owner(target)
    if should_warn:
        mock_console.warning.assert_called_once()
        assert "Role mismatch" in mock_console.warning.call_args[0][0]
    else:
        mock_console.warning.assert_not_called()


@mock.patch("snowflake.cli._plugins.dcm.commands.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.commands.SqlExecutor")
def test_check_project_owner_warns_when_current_role_is_none(
    mock_executor_cls, mock_console
):
    mock_executor_cls().current_role.return_value = None
    target = DCMTarget(name="DEV", project_name="P1", **_DEFAULT_TARGET_FIELDS)
    _check_project_owner(target)
    mock_console.warning.assert_called_once()
    warning_message = mock_console.warning.call_args[0][0]
    assert "Cannot validate target's project owner" in warning_message
    assert "The current session role is required to match" in warning_message


@mock.patch("snowflake.cli._plugins.dcm.commands.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.commands.SqlExecutor")
def test_check_project_owner_warns_on_current_role_error(
    mock_executor_cls, mock_console
):
    mock_executor_cls().current_role.side_effect = Exception("Connection timeout")
    target = DCMTarget(name="DEV", project_name="P1", **_DEFAULT_TARGET_FIELDS)

    _check_project_owner(target)

    mock_console.warning.assert_called_once()
    warning_message = mock_console.warning.call_args[0][0]
    assert "Cannot validate target's project owner" in warning_message
    assert "Connection timeout" in warning_message
    assert "The current session role is required to match" in warning_message


@mock.patch("snowflake.cli._plugins.dcm.commands.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.commands.SqlExecutor")
def test_check_project_owner_no_warning_when_target_value_gets_quoted_via_from_dict(
    mock_executor_cls, mock_console
):
    mock_executor_cls().current_role.return_value = '"my role"'
    target = DCMTarget.from_dict(
        {
            "name": "dev",
            "project_name": "P1",
            "account_identifier": "MY_ORG-MY_ACCOUNT",
            "project_owner": "my role",
        }
    )
    _check_project_owner(target)
    mock_console.warning.assert_not_called()


@mock.patch("snowflake.cli._plugins.dcm.commands.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.commands.SqlExecutor")
def test_check_project_owner_warns_when_target_project_owner_is_empty(
    mock_executor_cls, mock_console
):
    target = DCMTarget(
        name="DEV",
        project_name="P1",
        account_identifier="MY_ORG-MY_ACCOUNT",
        project_owner="",
    )

    _check_project_owner(target)

    mock_executor_cls().current_role.assert_not_called()
    mock_console.warning.assert_called_once()
    warning_message = mock_console.warning.call_args[0][0]
    assert "project_owner is not specified" in warning_message
    assert "The current session role is required to match" in warning_message


@mock.patch("snowflake.cli._plugins.dcm.commands.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.commands.get_account_identifier")
@mock.patch("snowflake.cli._plugins.dcm.commands.get_cli_context")
def test_check_account_identifier_mismatch_warning_sanitizes_manifest_value(
    mock_ctx, mock_get_id, mock_console
):
    mock_get_id.return_value = AccountIdentifier("MY_ORG", "MY_ACCOUNT")
    target = DCMTarget(
        name="DEV",
        project_name="P1",
        account_identifier="WRONG_ORG-WRONG_ACCOUNT\x1b[31m injected",
        project_owner="MY_ROLE",
    )

    _check_account_identifier(target)

    mock_console.warning.assert_called_once()
    warning_message = mock_console.warning.call_args[0][0]
    assert "\x1b" not in warning_message
    assert "WRONG_ORG-WRONG_ACCOUNT injected" in warning_message


@mock.patch("snowflake.cli._plugins.dcm.commands.cli_console")
@mock.patch("snowflake.cli._plugins.dcm.commands.SqlExecutor")
def test_check_project_owner_mismatch_warning_sanitizes_manifest_value(
    mock_executor_cls, mock_console
):
    mock_executor_cls().current_role.return_value = "ADMIN_ROLE"
    target = DCMTarget(
        name="DEV",
        project_name="P1",
        account_identifier="MY_ORG-MY_ACCOUNT",
        project_owner="FINANCE_ROLE\x1b[31m injected",
    )

    _check_project_owner(target)

    mock_console.warning.assert_called_once()
    warning_message = mock_console.warning.call_args[0][0]
    assert "\x1b" not in warning_message
    assert "FINANCE_ROLE injected" in warning_message
