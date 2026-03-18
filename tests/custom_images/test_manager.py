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
from pathlib import Path
from unittest import mock

import pytest
from click import ClickException
from snowflake.cli._plugins.custom_images.manager import CustomImageManager


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


def create_mock_side_effect(
    inspect_response: str | None = None,
    pip_list_response: str | None = None,
    pip_check_result: tuple[int, str] = (0, ""),
    grype_result: tuple[int, str] = (0, ""),
    grype_error: Exception | None = None,
):
    """Helper to create a mock side_effect function for subprocess.run."""
    if inspect_response is None:
        inspect_response = make_docker_inspect_response()
    if pip_list_response is None:
        pip_list_response = make_pip_list_response([])

    def side_effect(*args, **kwargs):
        cmd = args[0]
        cmd_str = " ".join(cmd)
        if cmd[0] == "docker":
            if "inspect" in cmd:
                return mock.Mock(returncode=0, stdout=inspect_response, stderr="")
            elif "run" in cmd:
                if "pip list" in cmd_str:
                    return mock.Mock(returncode=0, stdout=pip_list_response, stderr="")
                elif "pip check" in cmd_str:
                    return mock.Mock(
                        returncode=pip_check_result[0],
                        stdout=pip_check_result[1],
                        stderr="",
                    )
        elif cmd[0] == "grype":
            if grype_error:
                raise grype_error
            return mock.Mock(
                returncode=grype_result[0], stdout=grype_result[1], stderr=""
            )
        return mock.Mock(returncode=0, stdout="", stderr="")

    return side_effect


class TestCustomImageManager:
    """Tests for CustomImageManager validation."""

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
  dependency_health: true
  vulnerability_scan: true
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


@mock.patch("snowflake.cli._plugins.custom_images.manager.subprocess.run")
def test_vulnerability_scan_failure_shows_table(mock_run, tmp_path):
    """On Grype failure, message is a readable table, not raw JSON."""
    config_file = tmp_path / "cfg.yaml"
    config_file.write_text(
        """
version: "1.0"
checks:
  entrypoint: "/usr/local/bin/entrypoint.sh"
  vulnerability_scan: true
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

    def side_effect(*args, **kwargs):
        cmd = args[0]
        if cmd[0] == "docker" and "inspect" in cmd:
            return mock.Mock(
                returncode=0,
                stdout=make_docker_inspect_response(
                    entrypoint=["/usr/local/bin/entrypoint.sh"]
                ),
                stderr="",
            )
        if cmd[0] == "grype":
            return mock.Mock(returncode=1, stdout=grype_out, stderr="")
        return mock.Mock(returncode=0, stdout="", stderr="")

    mock_run.side_effect = side_effect
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
