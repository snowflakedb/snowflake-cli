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

import os
from pathlib import Path
from unittest import mock

import pytest

SUBPROCESS_RUN = "snowflake.cli._plugins.run.manager.subprocess.run"


class TestRunIntegration:
    """Integration tests for snow run command."""

    @mock.patch(SUBPROCESS_RUN)
    def test_run_echo_script_in_real_project(
        self, mock_run, runner, project_directory
    ):
        """Test running a simple echo script."""
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "dev"])
            assert result.exit_code == 0
            assert "Running script: dev" in result.output
            assert "echo" in result.output
            mock_run.assert_called_once()

    @mock.patch(SUBPROCESS_RUN)
    def test_composite_script_runs_all_steps(
        self, mock_run, runner, project_directory
    ):
        """Test composite script executes all child scripts."""
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "deploy-all"])
            assert result.exit_code == 0
            assert "[1/2] build" in result.output
            assert "[2/2] deploy" in result.output
            assert "Done! (2 scripts executed)" in result.output

    @mock.patch(SUBPROCESS_RUN)
    def test_variable_interpolation_from_env_section(
        self, mock_run, runner, project_directory
    ):
        """Test that env variables are properly interpolated."""
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "build"])
            assert result.exit_code == 0
            cmd = " ".join(mock_run.call_args[0][0])
            assert "TEMP.DEV_PLATFORM" in cmd

    @mock.patch(SUBPROCESS_RUN)
    def test_override_variable_from_cli(
        self, mock_run, runner, project_directory
    ):
        """Test that -D flag properly overrides variables."""
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "build", "-D", "env.database=PRODUCTION"])
            assert result.exit_code == 0
            cmd = " ".join(mock_run.call_args[0][0])
            assert "PRODUCTION" in cmd

    @mock.patch(SUBPROCESS_RUN)
    def test_shell_mode_for_pipes(
        self, mock_run, runner, project_directory
    ):
        """Test that shell=true scripts use shell execution."""
        mock_run.return_value = mock.Mock(returncode=0)
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "shell-test"])
            assert result.exit_code == 0
            call_kwargs = mock_run.call_args[1]
            assert call_kwargs["shell"] is True

    def test_missing_script_shows_available_scripts(self, runner, project_directory):
        """Test that missing script name shows list of available scripts."""
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "missing-script"])
            assert result.exit_code != 0
            assert "not found" in result.output
            assert "dev" in result.output or "Available scripts" in result.output

    def test_list_option_shows_all_scripts(self, runner, project_directory):
        """Test --list option shows all available scripts with descriptions."""
        with project_directory("run_scripts"):
            result = runner.invoke(["run", "--list"])
            assert result.exit_code == 0
            assert "dev" in result.output
            assert "build" in result.output
            assert "deploy" in result.output
            assert "Start local development server" in result.output
