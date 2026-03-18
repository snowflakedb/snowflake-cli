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

from unittest import mock

SUBPROCESS_RUN = "snowflake.cli._plugins.run.manager._subprocess_run"


class TestRunList:
    def test_list_scripts_shows_available_scripts(self, runner, project_directory):
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "--list"])
            assert result.exit_code == 0
            assert "Available scripts" in result.output
            assert "dev" in result.output
            assert "deploy" in result.output
            assert "build" in result.output

    def test_list_scripts_shows_descriptions(self, runner, project_directory):
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "--list"])
            assert result.exit_code == 0
            assert "Start local development server" in result.output
            assert "Build the project" in result.output

    def test_list_no_scripts_shows_message(self, runner, project_directory):
        with project_directory("streamlit_full_definition"):
            result = runner.invoke(["run", "--list"])
            assert result.exit_code == 0
            assert "No scripts defined" in result.output

    def test_no_args_shows_available_scripts(self, runner, project_directory):
        with project_directory("run_scripts"):
            result = runner.invoke(["run"])
            assert result.exit_code == 0
            assert "Available scripts" in result.output


class TestRunExecute:
    @mock.patch(SUBPROCESS_RUN)
    def test_run_simple_script(self, mock_run, runner, project_directory):
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "dev"])
            assert result.exit_code == 0
            mock_run.assert_called_once()
            assert "echo" in mock_run.call_args[0][0][0]

    @mock.patch(SUBPROCESS_RUN)
    def test_run_script_with_variable_interpolation(
        self, mock_run, runner, project_directory
    ):
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "build"])
            assert result.exit_code == 0
            call_args = mock_run.call_args[0][0]
            assert "TEMP" in " ".join(call_args)
            assert "DEV_PLATFORM" in " ".join(call_args)

    def test_run_nonexistent_script_fails(self, runner, project_directory):
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "nonexistent"])
            assert result.exit_code != 0
            assert "not found" in result.output

    def test_run_circular_dependency_detected(self, runner, project_directory):
        with project_directory("run_scripts_cycle"):
            result = runner.invoke(["run", "alpha"])
            assert result.exit_code != 0
            assert "Circular dependency detected" in result.output

    @mock.patch(SUBPROCESS_RUN)
    def test_run_with_dry_run(self, mock_run, runner, project_directory):
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "dev", "--dry-run"])
            assert result.exit_code == 0
            mock_run.assert_not_called()
            assert "echo" in result.output

    @mock.patch(SUBPROCESS_RUN)
    def test_run_composite_script(self, mock_run, runner, project_directory):
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "deploy-all"])
            assert result.exit_code == 0
            assert mock_run.call_count == 2
            assert "Done!" in result.output
            assert "2 scripts executed" in result.output

    @mock.patch(SUBPROCESS_RUN)
    def test_run_composite_script_stops_on_error(
        self, mock_run, runner, project_directory
    ):
        mock_run.return_value = mock.Mock(returncode=1)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "deploy-all"])
            assert result.exit_code == 1
            assert mock_run.call_count == 1

    @mock.patch(SUBPROCESS_RUN)
    def test_run_composite_script_continue_on_error(
        self, mock_run, runner, project_directory
    ):
        mock_run.return_value = mock.Mock(returncode=1)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "deploy-all", "--continue-on-error"])
            assert mock_run.call_count == 2
            assert "Completed with errors" in result.output

    @mock.patch(SUBPROCESS_RUN)
    def test_run_with_extra_args(self, mock_run, runner, project_directory):
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "dev", "--", "--server.port", "8502"])
            assert result.exit_code == 0
            cmd_str = " ".join(mock_run.call_args[0][0])
            assert "--server.port" in cmd_str
            assert "8502" in cmd_str

    @mock.patch(SUBPROCESS_RUN)
    def test_run_with_var_override(self, mock_run, runner, project_directory):
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "build", "-D", "env.database=PROD"])
            assert result.exit_code == 0
            cmd_str = " ".join(mock_run.call_args[0][0])
            assert "PROD" in cmd_str

    @mock.patch(SUBPROCESS_RUN)
    def test_run_shell_script(self, mock_run, runner, project_directory):
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "shell-test"])
            assert result.exit_code == 0
            assert mock_run.call_args[1]["shell"] is True


class TestRunHelp:
    def test_run_help_shows_usage(self, runner):
        result = runner.invoke(["run", "--help"])
        assert result.exit_code == 0
        assert "Execute project scripts" in result.output
        assert "--list" in result.output
        assert "--dry-run" in result.output


class TestRunManifestScripts:
    def test_list_scripts_from_manifest(self, runner, project_directory):
        with project_directory("run_manifest_scripts"):
            result = runner.invoke(["run", "--list"])
            assert result.exit_code == 0
            assert "Available scripts" in result.output
            assert "from manifest.yml" in result.output
            assert "validate" in result.output
            assert "deploy" in result.output

    @mock.patch(SUBPROCESS_RUN)
    def test_run_script_from_manifest(self, mock_run, runner, project_directory):
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_manifest_scripts"):
            result = runner.invoke(["run", "validate"])
            assert result.exit_code == 0
            mock_run.assert_called_once()
            assert "echo" in mock_run.call_args[0][0][0]

    @mock.patch(SUBPROCESS_RUN)
    def test_run_composite_script_from_manifest(
        self, mock_run, runner, project_directory
    ):
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_manifest_scripts"):
            result = runner.invoke(["run", "all"])
            assert result.exit_code == 0
            assert mock_run.call_count == 2

    def test_scripts_conflict_raises_error(self, runner, project_directory):
        with project_directory("run_scripts_conflict"):
            result = runner.invoke(["run", "--list"])
            assert result.exit_code != 0
            assert "Scripts defined in both" in result.output
            assert "manifest.yml" in result.output
            assert "snowflake.yml" in result.output

    def test_list_shows_source_file_snowflake(self, runner, project_directory):
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "--list"])
            assert result.exit_code == 0
            assert "from snowflake.yml" in result.output

    @mock.patch(SUBPROCESS_RUN)
    def test_run_manifest_only_project(self, mock_run, runner, project_directory):
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_manifest_only"):
            result = runner.invoke(["run", "test"])
            assert result.exit_code == 0
            mock_run.assert_called_once()
