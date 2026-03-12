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

import json
from unittest import mock


def make_docker_inspect_response(
    entrypoint: list[str] | None = None,
    env_vars: list[str] | None = None,
) -> str:
    """Helper to create a mock docker inspect JSON response."""
    return json.dumps(
        [
            {
                "Config": {
                    "Entrypoint": entrypoint,
                    "Env": env_vars or [],
                    "Labels": {},
                }
            }
        ]
    )


def make_pip_list_response(packages: list[dict]) -> str:
    """Helper to create a mock pip list JSON response."""
    return json.dumps(packages)


class TestValidateCustomImageCommand:
    """Tests for the custom-image validate CLI command."""

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_validate_custom_image_success(self, mock_run, runner):
        """Test successful validation with all checks passing."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        pip_list_response = make_pip_list_response(
            [
                {"name": "snowflake-ml-python", "version": "1.0"},
                {"name": "ray", "version": "2.0"},
                {"name": "ipykernel", "version": "6.0"},
                {"name": "sqlparse", "version": "0.5"},
                {"name": "jinja2", "version": "3.0"},
                {"name": "notebook", "version": "7.0"},
                {"name": "ipython", "version": "8.0"},
                {"name": "psutil", "version": "5.0"},
                {"name": "snowflake-snowpark-python", "version": "1.0"},
                {"name": "jupyter-server", "version": "2.0"},
                {"name": "lightgbm-ray", "version": "0.1"},
                {"name": "xgboost-ray", "version": "0.1"},
                {"name": "snowflake", "version": "1.0"},
                {"name": "snowflake.core", "version": "1.0"},
                {"name": "snowflake-connector-python", "version": "3.0"},
            ]
        )

        def side_effect(*args, **kwargs):
            cmd = args[0]
            cmd_str = " ".join(cmd)
            if cmd[0] == "docker":
                if "inspect" in cmd:
                    return mock.Mock(returncode=0, stdout=inspect_response, stderr="")
                elif "run" in cmd:
                    if "pip list" in cmd_str:
                        return mock.Mock(
                            returncode=0, stdout=pip_list_response, stderr=""
                        )
                    elif "pip check" in cmd_str:
                        return mock.Mock(
                            returncode=0,
                            stdout="No broken requirements found.",
                            stderr="",
                        )
            return mock.Mock(returncode=0, stdout="", stderr="")

        mock_run.side_effect = side_effect

        result = runner.invoke(["custom-image", "validate", "test-image:latest"])

        assert result.exit_code == 0, result.output
        assert "ALL CHECKS PASSED" in result.output

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_validate_custom_image_failure(self, mock_run, runner):
        """Test validation failure with missing packages."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        pip_list_response = make_pip_list_response(
            [
                {"name": "snowflake-ml-python", "version": "1.0"},
            ]
        )

        def side_effect(*args, **kwargs):
            cmd = args[0]
            cmd_str = " ".join(cmd)
            if cmd[0] == "docker":
                if "inspect" in cmd:
                    return mock.Mock(returncode=0, stdout=inspect_response, stderr="")
                elif "run" in cmd:
                    if "pip list" in cmd_str:
                        return mock.Mock(
                            returncode=0, stdout=pip_list_response, stderr=""
                        )
                    elif "pip check" in cmd_str:
                        return mock.Mock(returncode=0, stdout="", stderr="")
            return mock.Mock(returncode=0, stdout="", stderr="")

        mock_run.side_effect = side_effect

        result = runner.invoke(["custom-image", "validate", "test-image:latest"])

        assert result.exit_code == 1
        assert "VALIDATION FAILED" in result.output

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_validate_custom_image_not_found(self, mock_run, runner):
        """Test error when image is not found."""
        mock_run.return_value = mock.Mock(
            returncode=1,
            stdout="",
            stderr="No such image: nonexistent:latest",
        )

        result = runner.invoke(["custom-image", "validate", "nonexistent:latest"])

        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_validate_custom_image_docker_not_installed(self, runner):
        """Test error when Docker is not installed."""
        import subprocess

        original_run = subprocess.run

        def docker_not_found(*args, **kwargs):
            cmd = args[0] if args else kwargs.get("args", [])
            if cmd and cmd[0] == "docker":
                raise FileNotFoundError()
            return original_run(*args, **kwargs)

        mock_run.side_effect = docker_not_found

        result = runner.invoke(["custom-image", "validate", "test-image:latest"])

        assert result.exit_code == 1
        assert "Docker is not installed" in result.output

    def test_validate_custom_image_missing_argument(self, runner):
        """Test error when image argument is missing."""
        result = runner.invoke(["custom-image", "validate"])

        assert result.exit_code == 2
        assert "Missing argument" in result.output

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_validate_custom_image_entrypoint_mismatch(self, mock_run, runner):
        """Test detection of wrong entrypoint."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/wrong/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        pip_list_response = make_pip_list_response(
            [
                {"name": "snowflake-ml-python", "version": "1.0"},
                {"name": "ray", "version": "2.0"},
                {"name": "ipykernel", "version": "6.0"},
                {"name": "sqlparse", "version": "0.5"},
                {"name": "jinja2", "version": "3.0"},
                {"name": "notebook", "version": "7.0"},
                {"name": "ipython", "version": "8.0"},
                {"name": "psutil", "version": "5.0"},
                {"name": "snowflake-snowpark-python", "version": "1.0"},
                {"name": "jupyter-server", "version": "2.0"},
                {"name": "lightgbm-ray", "version": "0.1"},
                {"name": "xgboost-ray", "version": "0.1"},
                {"name": "snowflake", "version": "1.0"},
                {"name": "snowflake.core", "version": "1.0"},
                {"name": "snowflake-connector-python", "version": "3.0"},
            ]
        )

        def side_effect(*args, **kwargs):
            cmd = args[0]
            cmd_str = " ".join(cmd)
            if cmd[0] == "docker":
                if "inspect" in cmd:
                    return mock.Mock(returncode=0, stdout=inspect_response, stderr="")
                elif "run" in cmd:
                    if "pip list" in cmd_str:
                        return mock.Mock(
                            returncode=0, stdout=pip_list_response, stderr=""
                        )
                    elif "pip check" in cmd_str:
                        return mock.Mock(returncode=0, stdout="", stderr="")
            return mock.Mock(returncode=0, stdout="", stderr="")

        mock_run.side_effect = side_effect

        result = runner.invoke(["custom-image", "validate", "test-image:latest"])

        assert result.exit_code == 1
        assert "[FAIL] entrypoint" in result.output
        assert "mismatch" in result.output.lower()

    def test_help_text(self, runner):
        """Test that help text is displayed correctly."""
        result = runner.invoke(["custom-image", "validate", "--help"])

        assert result.exit_code == 0
        assert "Validates a Docker image" in result.output
        assert "image" in result.output.lower()

    def test_command_registered(self, runner):
        """Test that the command is properly registered."""
        result = runner.invoke(["--help"])

        assert result.exit_code == 0
        assert "custom-image" in result.output
