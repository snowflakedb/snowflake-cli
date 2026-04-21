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


class TestRegisterCustomImageCommand:
    """Tests for the custom-image register CLI command."""

    REGISTRY = (
        "org-acct.registry.snowflakecomputing.com/mydb/myschema/myrepo/myimage:latest"
    )
    BASE_IMAGE_TYPE = "cpu"

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    @mock.patch(
        "snowflake.cli._plugins.custom_images.manager.CustomImageManager.execute_query"
    )
    def test_register_success_with_cre(self, mock_execute_query, mock_run, runner):
        """Test successful register: push then create CRE."""
        mock_run.side_effect = create_mock_side_effect()

        result = runner.invoke(
            [
                "custom-image",
                "register",
                "test-image:latest",
                self.REGISTRY,
                "--base-image-type",
                self.BASE_IMAGE_TYPE,
            ]
        )

        assert result.exit_code == 0, result.output
        assert "Successfully pushed" in result.output
        assert "Custom Runtime Environment" in result.output
        mock_execute_query.assert_called_once()
        sql = mock_execute_query.call_args[0][0]
        assert "CREATE CUSTOM RUNTIME ENVIRONMENT" in sql
        assert "/mydb/myschema/myrepo/myimage:latest" in sql
        assert self.BASE_IMAGE_TYPE in sql

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_register_skip_validation_only_pushes(self, mock_run, runner):
        """Test register with --skip-validation: only tag and push, no CRE."""
        mock_run.return_value = mock.Mock(returncode=0, stdout="", stderr="")

        result = runner.invoke(
            [
                "custom-image",
                "register",
                "test-image:latest",
                self.REGISTRY,
                "--skip-validation",
            ]
        )

        assert result.exit_code == 0, result.output
        assert "Successfully pushed" in result.output
        assert "Custom Runtime Environment" not in result.output

        calls = [call.args[0] for call in mock_run.call_args_list]
        assert any("tag" in cmd for cmd in calls)
        assert any("push" in cmd for cmd in calls)

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_register_missing_base_image_type_fails(self, mock_run, runner):
        """Test that missing --base-image-type fails when not using --skip-validation."""
        mock_run.side_effect = create_mock_side_effect()

        result = runner.invoke(
            [
                "custom-image",
                "register",
                "test-image:latest",
                self.REGISTRY,
            ]
        )

        assert result.exit_code == 1
        assert "base-image-type" in result.output.lower()

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    @mock.patch(
        "snowflake.cli._plugins.custom_images.manager.CustomImageManager.execute_query"
    )
    def test_register_custom_cre_name(self, mock_execute_query, mock_run, runner):
        """Test that a custom CRE name is used when provided."""
        mock_run.side_effect = create_mock_side_effect()

        result = runner.invoke(
            [
                "custom-image",
                "register",
                "test-image:latest",
                self.REGISTRY,
                "--base-image-type",
                self.BASE_IMAGE_TYPE,
                "--name",
                "my_custom_cre",
            ]
        )

        assert result.exit_code == 0, result.output
        assert "my_custom_cre" in result.output
        sql = mock_execute_query.call_args[0][0]
        assert "my_custom_cre" in sql

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    @mock.patch(
        "snowflake.cli._plugins.custom_images.manager.CustomImageManager.execute_query"
    )
    def test_register_auto_generated_cre_name(
        self, mock_execute_query, mock_run, runner
    ):
        """Test that a CRE name is auto-generated when not provided."""
        mock_run.side_effect = create_mock_side_effect()

        result = runner.invoke(
            [
                "custom-image",
                "register",
                "test-image:latest",
                self.REGISTRY,
                "--base-image-type",
                self.BASE_IMAGE_TYPE,
            ]
        )

        assert result.exit_code == 0, result.output
        sql = mock_execute_query.call_args[0][0]
        assert "mlruntimes_" in sql

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_register_tag_failure(self, mock_run, runner):
        """Test error when docker tag fails."""
        mock_run.side_effect = create_mock_side_effect(
            tag_result=(1, "invalid reference format"),
        )

        result = runner.invoke(
            [
                "custom-image",
                "register",
                "test-image:latest",
                "bad::registry",
                "--skip-validation",
            ]
        )

        assert result.exit_code == 1
        assert "Failed to tag" in result.output

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_register_push_failure(self, mock_run, runner):
        """Test error when docker push fails."""
        mock_run.side_effect = create_mock_side_effect(
            push_result=(1, "unauthorized: authentication required"),
        )

        result = runner.invoke(
            [
                "custom-image",
                "register",
                "test-image:latest",
                self.REGISTRY,
                "--skip-validation",
            ]
        )

        assert result.exit_code == 1
        assert "Failed to push" in result.output

    def test_register_missing_arguments(self, runner):
        """Test error when required arguments are missing."""
        result = runner.invoke(["custom-image", "register"])
        assert result.exit_code == 2
        assert "Missing argument" in result.output

        result = runner.invoke(["custom-image", "register", "test-image:latest"])
        assert result.exit_code == 2
        assert "Missing argument" in result.output

    def test_register_help_text(self, runner):
        """Test that help text is displayed correctly."""
        result = runner.invoke(["custom-image", "register", "--help"])

        assert result.exit_code == 0
        assert "registry" in result.output.lower()
        assert "--skip-validation" in result.output
        assert "--name" in result.output

    def test_register_invalid_base_image_type(self, runner):
        """Test that an invalid --base-image-type value is rejected."""
        result = runner.invoke(
            [
                "custom-image",
                "register",
                "test-image:latest",
                self.REGISTRY,
                "--base-image-type",
                "invalid_type",
            ]
        )

        assert result.exit_code == 2
        assert "invalid" in result.output.lower()

    def test_register_connection_flags_in_help(self, runner):
        """requires_connection=True means --connection and friends appear in register --help."""
        result = runner.invoke(["custom-image", "register", "--help"])

        assert result.exit_code == 0
        assert "--connection" in result.output

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    @mock.patch(
        "snowflake.cli._plugins.custom_images.manager.CustomImageManager.execute_query"
    )
    def test_register_cre_name_with_spaces_is_quoted(
        self, mock_execute_query, mock_run, runner
    ):
        """CRE names containing spaces are double-quoted in the SQL statement."""
        mock_run.side_effect = create_mock_side_effect()

        result = runner.invoke(
            [
                "custom-image",
                "register",
                "test-image:latest",
                self.REGISTRY,
                "--base-image-type",
                self.BASE_IMAGE_TYPE,
                "--name",
                "my cre name",
            ]
        )

        assert result.exit_code == 0, result.output
        sql = mock_execute_query.call_args[0][0]
        assert '"my cre name"' in sql

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    @mock.patch(
        "snowflake.cli._plugins.custom_images.manager.CustomImageManager.execute_query"
    )
    def test_register_bare_registry_no_slash_fails(
        self, mock_execute_query, mock_run, runner
    ):
        """A registry with no slash raises a clear error instead of producing bogus SQL."""
        mock_run.return_value = mock.Mock(returncode=0, stdout="", stderr="")

        result = runner.invoke(
            [
                "custom-image",
                "register",
                "test-image:latest",
                "barehostname",
                "--base-image-type",
                self.BASE_IMAGE_TYPE,
            ]
        )

        assert result.exit_code == 1
        assert "Invalid registry format" in result.output
