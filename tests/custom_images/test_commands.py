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

from tests.custom_images.test_helpers import (
    FULL_PACKAGE_LIST,
    create_mock_side_effect,
    make_docker_inspect_response,
    make_pip_list_response,
)


class TestValidateCustomImageCommand:
    """Tests for the custom-image validate CLI command."""

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_validate_custom_image_success(self, mock_run, runner):
        """Test successful validation with all checks passing."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        pip_list_response = make_pip_list_response(FULL_PACKAGE_LIST)

        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list_response,
            pip_check_result=(0, "No broken requirements found."),
        )

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

        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list_response,
        )

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

        with mock.patch(
            "snowflake.cli._plugins.custom_images.manager.subprocess.run",
            side_effect=docker_not_found,
        ):
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
        pip_list_response = make_pip_list_response(FULL_PACKAGE_LIST)

        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list_response,
        )

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
