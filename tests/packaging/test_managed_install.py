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
import os
import platform
import shutil
import stat
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest
from pytest_httpserver import HTTPServer

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="install.sh needs a POSIX shell; Windows uses install.ps1.",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
INSTALL_SH = REPO_ROOT / "scripts" / "packaging" / "snowflake-managed" / "install.sh"
INSTALL_PS1 = REPO_ROOT / "scripts" / "packaging" / "snowflake-managed" / "install.ps1"
PATH_BEGIN = "# snowflake-cli snowflake-managed PATH begin"


def _repo_os_arch() -> tuple[str, str]:
    os_name = platform.system().lower()
    machine = platform.machine().lower()
    if machine in {"x86_64", "amd64"}:
        arch = "amd64"
    elif machine in {"aarch64", "arm64"}:
        arch = "arm64"
    else:
        arch = machine
    return os_name, arch


def _write_fake_snow(path: Path, version: str) -> None:
    path.write_text(f"#!/bin/sh\necho 'Snowflake CLI version: {version}'\n")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def _tarball_bytes(tmp_path: Path, version: str) -> tuple[bytes, str]:
    payload = tmp_path / f"payload-{version}"
    payload.mkdir()
    _write_fake_snow(payload / "snow", version)
    archive = tmp_path / f"{version}.tar.gz"
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(payload / "snow", arcname="snow")
    data = archive.read_bytes()
    return data, hashlib.sha256(data).hexdigest()


def _serve_release(
    httpserver: HTTPServer, tmp_path: Path, version: str, *, checksum: str | None = None
) -> str:
    os_name, arch = _repo_os_arch()
    data, digest = _tarball_bytes(tmp_path, version)
    filename = f"snowflake-cli-{version}-{os_name}-{arch}.tar.gz"
    httpserver.expect_request("/stable_version.txt").respond_with_data(f"{version}\n")
    httpserver.expect_request(f"/{version}/manifest.json").respond_with_data(
        json.dumps(
            {
                "packages": {
                    os_name: {
                        arch: {
                            "name": filename,
                            "checksum": checksum if checksum is not None else digest,
                        }
                    }
                }
            }
        ),
        content_type="application/json",
    )
    httpserver.expect_request(f"/{version}/{filename}").respond_with_data(
        data, content_type="application/gzip"
    )
    return digest


def _run_install(
    tmp_path: Path,
    httpserver: HTTPServer,
    *,
    extra_env: dict[str, str] | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["HOME"] = str(home)
    env["SHELL"] = "/bin/bash"
    env["NON_INTERACTIVE"] = "1"
    env["SNOWFLAKE_CLI_MANAGED_REPO"] = httpserver.url_for("/").rstrip("/")
    env.pop("SNOWFLAKE_CLI_MANAGED_HOME", None)
    env.pop("XDG_DATA_HOME", None)
    env.pop("SKIP_PATH_PROMPT", None)
    env.pop("DOCKER_ENV", None)
    if extra_env:
        env.update(extra_env)
    result = subprocess.run(
        ["sh", str(INSTALL_SH)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )
    if check and result.returncode != 0:
        raise AssertionError(
            f"install.sh exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def test_install_sh_exists_and_is_executable():
    assert INSTALL_SH.is_file()
    assert os.access(INSTALL_SH, os.X_OK)


def test_install_sh_shellcheck():
    shellcheck = shutil.which("shellcheck")
    if shellcheck is None:
        pytest.skip("shellcheck is not installed")
    result = subprocess.run(
        [shellcheck, "-s", "dash", "-e", "SC3043", str(INSTALL_SH)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_install_sh_against_fake_repo_leaves_working_shim(
    tmp_path: Path, httpserver: HTTPServer
):
    _serve_release(httpserver, tmp_path, "3.13.1")
    result = _run_install(tmp_path, httpserver)

    home = tmp_path / "home"
    root = home / ".local" / "share" / "snowflake-cli"
    binary = root / "3.13.1" / "snow"
    shim = root / "bin" / "snow"
    assert binary.is_file()
    assert shim.is_symlink()
    assert Path(os.readlink(shim)) == binary
    leftover = list((root / "bin").glob(".*.tmp"))
    assert leftover == []

    invoked = subprocess.run(
        [str(shim), "--version"], check=False, capture_output=True, text=True
    )
    assert invoked.returncode == 0, invoked.stderr
    assert "Snowflake CLI version: 3.13.1" in invoked.stdout

    assert (root / ".current").read_text().strip() == "3.13.1"

    bashrc = home / ".bashrc"
    assert bashrc.is_file()
    text = bashrc.read_text()
    assert PATH_BEGIN in text
    bin_dir = str(root / "bin")
    assert bin_dir in text
    assert str(home / ".local" / "bin") not in text
    assert f'export PATH="{bin_dir}:$PATH"' in text
    assert f'export PATH="{bin_dir}:$PATH"' in result.stdout or bin_dir in result.stdout


def test_install_sh_prints_export_path_when_skip_path_prompt(
    tmp_path: Path, httpserver: HTTPServer
):
    _serve_release(httpserver, tmp_path, "3.13.1")
    result = _run_install(tmp_path, httpserver, extra_env={"SKIP_PATH_PROMPT": "1"})
    home = tmp_path / "home"
    bin_dir = home / ".local" / "share" / "snowflake-cli" / "bin"
    assert f'export PATH="{bin_dir}:$PATH"' in result.stdout
    assert not (home / ".bashrc").exists()
    assert "SKIP_PATH_PROMPT" in result.stdout


@pytest.mark.parametrize(
    "version",
    ["../../.bashrc", "../evil", "bin", "3.13.1/../../tmp", "3.13.1\\win"],
)
def test_install_sh_rejects_unsanitized_version(
    tmp_path: Path, httpserver: HTTPServer, version: str
):
    httpserver.expect_request("/stable_version.txt").respond_with_data(f"{version}\n")
    result = _run_install(tmp_path, httpserver, check=False)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Invalid snowflake-managed version" in combined
    home = tmp_path / "home"
    assert not (home / ".bashrc").exists()
    root = home / ".local" / "share" / "snowflake-cli"
    assert not (root / "bin" / "snow").exists()


def test_install_sh_rejects_path_traversal_package_name(
    tmp_path: Path, httpserver: HTTPServer
):
    os_name, arch = _repo_os_arch()
    httpserver.expect_request("/stable_version.txt").respond_with_data("3.13.1\n")
    httpserver.expect_request("/3.13.1/manifest.json").respond_with_data(
        json.dumps(
            {
                "packages": {
                    os_name: {
                        arch: {
                            "name": "../../evil.tar.gz",
                            "checksum": "0" * 64,
                        }
                    }
                }
            }
        ),
        content_type="application/json",
    )
    result = _run_install(tmp_path, httpserver, check=False)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Invalid package name" in combined
    home = tmp_path / "home"
    assert not (home / ".local" / "share" / "snowflake-cli" / "bin" / "snow").exists()


def test_install_sh_checksum_mismatch_fail_closed(
    tmp_path: Path, httpserver: HTTPServer
):
    _serve_release(httpserver, tmp_path, "3.13.1", checksum="0" * 64)
    result = _run_install(tmp_path, httpserver, check=False)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Checksum mismatch" in combined
    home = tmp_path / "home"
    shim = home / ".local" / "share" / "snowflake-cli" / "bin" / "snow"
    assert not shim.exists()


def test_install_ps1_is_snowflake_managed_not_cortex():
    text = INSTALL_PS1.read_text()
    assert "snowflake-cli" in text
    assert 'BinaryName = "snow.exe"' in text
    assert "LOCALAPPDATA" in text
    assert r"snowflake-cli\bin" in text or "snowflake-cli" in text
    assert "cortex-code-cli" not in text
    assert "snow.exe" in text
    assert "Assert-ManagedVersion" in text
    assert "Assert-PackageName" in text
    assert "native" not in text.lower()
    assert "self-managed" not in text.lower()
