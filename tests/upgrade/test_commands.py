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

from __future__ import annotations

import json

import pytest
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource
from snowflake.cli._plugins.upgrade import manager
from snowflake.cli._plugins.upgrade.manager import (
    INSTALL_PS1_COMMAND,
    INSTALL_SH_COMMAND,
    STATUS_ALREADY_CURRENT,
    STATUS_DRY_RUN,
    STATUS_NEW_MAJOR,
    STATUS_REFUSED,
    UnimplementedVersionSource,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.feature_flags import FeatureFlag

from tests_common.feature_flag_utils import with_feature_flags


class _FixedVersionSource:
    def __init__(self, version: str) -> None:
        self._version = version

    def latest_version(self) -> str:
        return self._version


@pytest.fixture(autouse=True)
def _reset_version_source():
    manager.set_version_source(UnimplementedVersionSource())
    yield
    manager.set_version_source(UnimplementedVersionSource())


def _parse_json(output: str) -> dict:
    return json.loads(output)


def test_upgrade_hidden_from_root_help_when_flag_off(runner):
    result = runner.invoke(["--help"])
    assert result.exit_code == 0, result.output
    assert "| upgrade " not in result.output


def test_upgrade_listed_in_root_help_when_flag_on(runner):
    with with_feature_flags({FeatureFlag.ENABLE_SNOW_UPGRADE: True}):
        result = runner.invoke(["--help"])
    assert result.exit_code == 0, result.output
    assert "| upgrade " in result.output


def test_upgrade_help_is_callable_when_hidden(runner):
    result = runner.invoke(["upgrade", "--help"])
    assert result.exit_code == 0, result.output
    assert "--dry-run" in result.output
    assert "snowflake-managed" in result.output


def test_refuse_pypi_default_install(runner, monkeypatch):
    monkeypatch.setattr(manager.platform, "system", lambda: "Linux")
    result = runner.invoke(["upgrade"])
    assert result.exit_code == 0, result.output
    assert "snowflake-managed distribution" in result.output
    assert "This install is pypi." in result.output
    assert INSTALL_SH_COMMAND in result.output


def test_refuse_pypi_json_schema(runner, monkeypatch):
    monkeypatch.setattr(manager.platform, "system", lambda: "Linux")
    result = runner.invoke(["upgrade", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["channel"] == "pypi"
    assert payload["status"] == STATUS_REFUSED
    assert payload["install_command"] == INSTALL_SH_COMMAND
    assert set(payload) == {"channel", "status", "install_command"}


def test_refuse_binary_json(runner, monkeypatch):
    monkeypatch.setattr(__about__, "INSTALLATION_SOURCE", CLIInstallationSource.BINARY)
    result = runner.invoke(["upgrade", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["channel"] == "binary"
    assert payload["status"] == STATUS_REFUSED
    assert "from" not in payload


def test_refuse_windows_install_command(runner, monkeypatch):
    monkeypatch.setattr(manager.platform, "system", lambda: "Windows")
    result = runner.invoke(["upgrade", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["install_command"] == INSTALL_PS1_COMMAND
    assert "This install is pypi." in runner.invoke(["upgrade"]).output


def test_already_current(runner, monkeypatch):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    manager.set_version_source(_FixedVersionSource("3.12.0"))
    result = runner.invoke(["upgrade", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["channel"] == "snowflake-managed"
    assert payload["status"] == STATUS_ALREADY_CURRENT
    assert payload["from"] == "3.12.0"
    assert payload["to"] == "3.12.0"
    human = runner.invoke(["upgrade"])
    assert human.exit_code == 0, human.output
    assert "Already current (3.12.0)." in human.output


def test_new_major_is_reported_not_installed(runner, monkeypatch):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    manager.set_version_source(_FixedVersionSource("4.0.0"))
    result = runner.invoke(["upgrade", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == STATUS_NEW_MAJOR
    assert payload["from"] == "3.12.0"
    assert payload["to"] == "4.0.0"
    human = runner.invoke(["upgrade"])
    assert "same-major" in human.output
    assert "4.0.0" in human.output


def test_dry_run_newer_same_major(runner, monkeypatch):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    manager.set_version_source(_FixedVersionSource("3.13.1"))
    result = runner.invoke(["upgrade", "--dry-run", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == STATUS_DRY_RUN
    assert payload["from"] == "3.12.0"
    assert payload["to"] == "3.13.1"
    human = runner.invoke(["upgrade", "--dry-run"])
    assert "Would upgrade 3.12.0 → 3.13.1." in human.output


def test_managed_newer_without_dry_run_is_preview_error(runner, monkeypatch):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    manager.set_version_source(_FixedVersionSource("3.13.1"))
    result = runner.invoke(["upgrade"])
    assert result.exit_code == 1
    assert "not available in this" in result.output


def test_dry_run_does_not_bypass_refuse(runner):
    result = runner.invoke(["upgrade", "--dry-run", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == STATUS_REFUSED


def test_version_unchanged_by_upgrade_command(runner):
    result = runner.invoke(["--version"])
    assert result.exit_code == 0, result.output
    assert result.output.startswith("Snowflake CLI version: 0.0.0-test_patched")
    assert "snowflake-managed" not in result.output
    assert "pypi" not in result.output


def test_unimplemented_source_errors_on_managed(runner, monkeypatch):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    manager.set_version_source(UnimplementedVersionSource())
    result = runner.invoke(["upgrade"])
    assert result.exit_code == 1
    assert "Fetching the latest snowflake-managed version" in result.output


def test_invalid_latest_version_errors(monkeypatch):
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", "3.12.0")
    manager.set_version_source(_FixedVersionSource("not-a-version"))
    with pytest.raises(CliError, match="Invalid Snowflake CLI version"):
        manager.plan_upgrade(dry_run=True)
