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

import hashlib
import json
import subprocess
import tarfile
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from pytest_httpserver import HTTPServer
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource
from snowflake.cli._plugins.upgrade import manager
from snowflake.cli._plugins.upgrade.layout import (
    MANAGED_HOME_ENV,
    ManagedLayout,
    posix_shim_contents,
    windows_cmd_contents,
)
from snowflake.cli._plugins.upgrade.manager import (
    STATUS_ALREADY_CURRENT,
    STATUS_DRY_RUN,
    STATUS_NEW_MAJOR,
    STATUS_UPGRADED,
    UnimplementedVersionSource,
)
from snowflake.cli._plugins.upgrade.repo import (
    DEFAULT_REPO_BASE,
    MANIFEST_NAME,
    POINTER_NAME,
    REPO_BASE_ENV,
    HttpRepo,
    _download_file,
    default_repo_base,
    repo_arch,
    repo_os,
)
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.secure_path import SecurePath

from tests_common import IS_WINDOWS


@pytest.fixture(autouse=True)
def _reset_version_source():
    manager.set_version_source(UnimplementedVersionSource())
    yield
    manager.set_version_source(UnimplementedVersionSource())


@pytest.fixture
def managed_home(tmp_path, monkeypatch):
    root = tmp_path / "snowflake-cli"
    monkeypatch.setenv(MANAGED_HOME_ENV, str(root))
    return root


def _write_fake_binary(path: Path, version: str) -> Path:
    SecurePath(path.parent).mkdir(parents=True, exist_ok=True)
    if IS_WINDOWS:
        SecurePath(path).write_text(f"@echo off\necho {version}\n")
    else:
        SecurePath(path).write_text(f"#!/bin/sh\necho {version}\n")
        path.chmod(0o755)
    return path


def _tarball_bytes(tmp_path: Path, version: str, binary_name: str) -> tuple[bytes, str]:
    payload = tmp_path / f"payload-{version}"
    payload.mkdir()
    binary = payload / binary_name
    _write_fake_binary(binary, version)
    archive = tmp_path / f"{version}.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(binary, arcname=binary_name)
    data = archive.read_bytes()
    return data, hashlib.sha256(data).hexdigest()


def _package_filename(version: str) -> str:
    return f"snowflake-cli-{version}-{repo_os()}-{repo_arch()}.tar.gz"


def _serve_pointer(httpserver: HTTPServer, version: str) -> None:
    httpserver.expect_request(f"/{POINTER_NAME}").respond_with_data(f"{version}\n")


def _serve_release(
    httpserver: HTTPServer,
    tmp_path: Path,
    version: str,
    *,
    checksum: Optional[str] = None,
    tarball: Optional[bytes] = None,
) -> str:
    binary_name = ManagedLayout().binary_name()
    data, digest = _tarball_bytes(tmp_path, version, binary_name)
    if tarball is not None:
        data = tarball
        digest = hashlib.sha256(data).hexdigest()
    filename = _package_filename(version)
    _serve_pointer(httpserver, version)
    httpserver.expect_request(f"/{version}/{MANIFEST_NAME}").respond_with_json(
        {
            "packages": {
                repo_os(): {
                    repo_arch(): {
                        "name": filename,
                        "checksum": checksum if checksum is not None else digest,
                    }
                }
            }
        }
    )
    httpserver.expect_request(f"/{version}/{filename}").respond_with_data(
        data, content_type="application/gzip"
    )
    return digest


def _enable_managed(monkeypatch, version: str = "3.12.0") -> None:
    monkeypatch.setattr(
        __about__, "INSTALLATION_SOURCE", CLIInstallationSource.SNOWFLAKE_MANAGED
    )
    monkeypatch.setattr(__about__, "VERSION", version)


def _use_repo(httpserver: HTTPServer) -> HttpRepo:
    repo = HttpRepo(base_url=httpserver.url_for("/").rstrip("/"))
    manager.set_version_source(repo)
    return repo


def _seed_current(managed_home: Path, version: str) -> ManagedLayout:
    layout = ManagedLayout(managed_home)
    source = managed_home.parent / f"src-{version}"
    _write_fake_binary(source, version)
    layout.install_binary(version, source)
    layout.retarget(version)
    return layout


def _assert_shim(layout: ManagedLayout, version: str) -> None:
    binary = layout.binary_path(version)
    assert binary.is_file()
    if IS_WINDOWS:
        # Fake payload is a batch script named snow.exe; CreateProcess rejects it.
        assert layout.windows_cmd_shim.read_text() == windows_cmd_contents(binary)
        assert layout.unix_shim.read_text() == posix_shim_contents(binary)
        assert version in binary.read_text()
        return
    completed = subprocess.run(
        [str(layout.unix_shim)],
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.strip() == version


def _parse_json(output: str) -> dict:
    return json.loads(output)


def test_repo_os_arch_mapping():
    assert repo_os(system="Darwin") == "darwin"
    assert repo_os(system="Linux") == "linux"
    assert repo_os(system="Windows") == "windows"
    assert repo_arch(machine="x86_64") == "amd64"
    assert repo_arch(machine="amd64") == "amd64"
    assert repo_arch(machine="aarch64") == "arm64"
    assert repo_arch(machine="arm64") == "arm64"


def test_default_repo_base_without_override(monkeypatch):
    monkeypatch.delenv(REPO_BASE_ENV, raising=False)
    assert default_repo_base() == DEFAULT_REPO_BASE


def test_default_repo_base_allows_https_sfc_repo_path(monkeypatch):
    monkeypatch.setenv(
        REPO_BASE_ENV, "https://sfc-repo.snowflakecomputing.com/snowflake-cli-staging"
    )
    assert (
        default_repo_base()
        == "https://sfc-repo.snowflakecomputing.com/snowflake-cli-staging"
    )


@pytest.mark.parametrize(
    "value",
    [
        "http://sfc-repo.snowflakecomputing.com/snowflake-cli",
        "https://evil.example/snowflake-cli",
        "https://127.0.0.1/snowflake-cli",
        "https://user@sfc-repo.snowflakecomputing.com/snowflake-cli",
        "https://sfc-repo.snowflakecomputing.com:8443/snowflake-cli",
        "file:///tmp/repo",
    ],
)
def test_default_repo_base_rejects_untrusted_override(monkeypatch, value):
    monkeypatch.setenv(REPO_BASE_ENV, value)
    with pytest.raises(CliError, match=REPO_BASE_ENV):
        default_repo_base()


def test_bad_managed_repo_env_does_not_fail_unrelated_commands(runner, monkeypatch):
    monkeypatch.setenv(REPO_BASE_ENV, "https://evil.example/snowflake-cli")
    manager.set_version_source(None)

    class _Boom:
        def __init__(self, *args, **kwargs):
            raise AssertionError("HttpRepo must not be constructed for help or refuse")

    monkeypatch.setattr(manager, "HttpRepo", _Boom)
    help_result = runner.invoke(["--help"])
    assert help_result.exit_code == 0, help_result.output
    sql_help = runner.invoke(["sql", "--help"])
    assert sql_help.exit_code == 0, sql_help.output
    refuse = runner.invoke(["upgrade"])
    assert refuse.exit_code == 0, refuse.output


def test_bad_managed_repo_env_fails_only_when_source_is_used(monkeypatch):
    monkeypatch.setenv(REPO_BASE_ENV, "https://evil.example/snowflake-cli")
    manager.set_version_source(None)
    with pytest.raises(CliError, match=REPO_BASE_ENV):
        manager.get_version_source()


def test_upgrade_happy_path_retargets_shim(
    runner, monkeypatch, managed_home, tmp_path, httpserver
):
    _enable_managed(monkeypatch, "3.12.0")
    layout = _seed_current(managed_home, "3.12.0")
    _serve_release(httpserver, tmp_path, "3.13.1")
    _use_repo(httpserver)

    result = runner.invoke(["upgrade", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["channel"] == "snowflake-managed"
    assert payload["status"] == STATUS_UPGRADED
    assert payload["from"] == "3.12.0"
    assert payload["to"] == "3.13.1"
    assert Path(payload["shim_target"]) == layout.binary_path("3.13.1")

    assert layout.current_version() == "3.13.1"
    assert layout.previous_version() == "3.12.0"
    _assert_shim(layout, "3.13.1")


def test_checksum_mismatch_does_not_write(
    runner, monkeypatch, managed_home, tmp_path, httpserver
):
    _enable_managed(monkeypatch)
    layout = _seed_current(managed_home, "3.12.0")
    _serve_release(httpserver, tmp_path, "3.13.1", checksum="0" * 64)
    _use_repo(httpserver)

    result = runner.invoke(["upgrade"])
    assert result.exit_code == 1
    assert "Checksum mismatch" in result.output
    assert layout.current_version() == "3.12.0"
    assert not layout.version_dir("3.13.1").exists()
    _assert_shim(layout, "3.12.0")


def test_truncated_body_fails_closed(tmp_path):
    dest = tmp_path / "pkg.tar.gz"
    response = MagicMock()
    response.headers = {"Content-Length": "100"}
    response.url = "http://example.test/pkg.tar.gz"
    response.raise_for_status.return_value = None
    response.iter_content.return_value = [b"partial"]
    response.__enter__.return_value = response
    response.__exit__.return_value = False

    with patch(
        "snowflake.cli._plugins.upgrade.repo.requests.get", return_value=response
    ):
        with pytest.raises(CliError, match="truncated"):
            _download_file("http://example.test/pkg.tar.gz", dest)
    assert not dest.exists()


def test_new_major_from_pointer_does_not_download(
    runner, monkeypatch, managed_home, httpserver
):
    _enable_managed(monkeypatch)
    layout = _seed_current(managed_home, "3.12.0")
    _serve_pointer(httpserver, "4.0.0")
    _use_repo(httpserver)

    result = runner.invoke(["upgrade", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == STATUS_NEW_MAJOR
    assert payload["to"] == "4.0.0"
    assert layout.current_version() == "3.12.0"
    assert not layout.version_dir("4.0.0").exists()


def test_already_current_from_pointer_does_not_download(
    runner, monkeypatch, managed_home, httpserver
):
    _enable_managed(monkeypatch, "3.12.0")
    layout = _seed_current(managed_home, "3.12.0")
    _serve_pointer(httpserver, "3.12.0")
    _use_repo(httpserver)

    result = runner.invoke(["upgrade", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == STATUS_ALREADY_CURRENT
    assert layout.current_version() == "3.12.0"


def test_dry_run_does_not_write(
    runner, monkeypatch, managed_home, tmp_path, httpserver
):
    _enable_managed(monkeypatch)
    layout = _seed_current(managed_home, "3.12.0")
    _serve_pointer(httpserver, "3.13.1")
    _use_repo(httpserver)

    result = runner.invoke(["upgrade", "--dry-run", "--format", "JSON"])
    assert result.exit_code == 0, result.output
    payload = _parse_json(result.output)
    assert payload["status"] == STATUS_DRY_RUN
    assert payload["from"] == "3.12.0"
    assert payload["to"] == "3.13.1"
    assert layout.current_version() == "3.12.0"
    assert not layout.version_dir("3.13.1").exists()
    _assert_shim(layout, "3.12.0")
