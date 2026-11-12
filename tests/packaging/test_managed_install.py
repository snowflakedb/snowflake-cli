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
import tomlkit
from pytest_httpserver import HTTPServer
from snowflake.cli._plugins.upgrade.trust import MANIFEST_SIG_NAME, sign_manifest

from tests.upgrade.manifest_signing import generate_test_rsa_keypair

requires_posix_shell = pytest.mark.skipif(
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


def _serve_version_package(
    httpserver: HTTPServer,
    tmp_path: Path,
    version: str,
    private_pem: bytes,
    *,
    checksum: str | None = None,
    serve_sig: bool = True,
    package_name: str | None = None,
) -> str:
    os_name, arch = _repo_os_arch()
    data, digest = _tarball_bytes(tmp_path, version)
    filename = package_name or f"snowflake-cli-{version}-{os_name}-{arch}.tar.gz"
    body = (
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
        )
    ).encode()
    httpserver.expect_request(f"/{version}/manifest.json").respond_with_data(
        body, content_type="application/json"
    )
    if serve_sig:
        httpserver.expect_request(f"/{version}/{MANIFEST_SIG_NAME}").respond_with_data(
            sign_manifest(body, private_pem),
            content_type="application/octet-stream",
        )
    else:
        httpserver.expect_request(f"/{version}/{MANIFEST_SIG_NAME}").respond_with_data(
            b"", status=404
        )
    if package_name is None or "/" not in package_name:
        httpserver.expect_request(f"/{version}/{filename}").respond_with_data(
            data, content_type="application/gzip"
        )
    return digest


def _serve_release(
    httpserver: HTTPServer,
    tmp_path: Path,
    version: str,
    private_pem: bytes,
    *,
    checksum: str | None = None,
    serve_sig: bool = True,
    package_name: str | None = None,
) -> str:
    httpserver.expect_request("/stable_version.txt").respond_with_data(f"{version}\n")
    return _serve_version_package(
        httpserver,
        tmp_path,
        version,
        private_pem,
        checksum=checksum,
        serve_sig=serve_sig,
        package_name=package_name,
    )


def _test_keys(tmp_path: Path) -> tuple[bytes, Path]:
    private_pem, public_pem = generate_test_rsa_keypair()
    pub_file = tmp_path / "test-manifest.pub.pem"
    pub_file.write_bytes(public_pem)
    return private_pem, pub_file


def _run_install(
    tmp_path: Path,
    httpserver: HTTPServer,
    pubkey_file: Path,
    *,
    extra_env: dict[str, str] | None = None,
    args: list[str] | None = None,
    check: bool = True,
    cwd: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    home = tmp_path / "home"
    home.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["HOME"] = str(home)
    env["SHELL"] = "/bin/bash"
    env["NON_INTERACTIVE"] = "1"
    env["SNOWFLAKE_CLI_MANAGED_REPO"] = httpserver.url_for("/").rstrip("/")
    env["SNOWFLAKE_CLI_MANAGED_MANIFEST_PUBKEY_FILE"] = str(pubkey_file)
    env.pop("SNOWFLAKE_CLI_MANAGED_HOME", None)
    env.pop("XDG_DATA_HOME", None)
    # The pytest session sets SNOWFLAKE_HOME to an isolated temp dir. The
    # installer must see the fake HOME, not that fixture.
    env.pop("SNOWFLAKE_HOME", None)
    env.pop("XDG_CONFIG_HOME", None)
    env.pop("SKIP_PATH_PROMPT", None)
    env.pop("DOCKER_ENV", None)
    env.pop("SNOWFLAKE_CLI_VERSION", None)
    if extra_env:
        env.update(extra_env)
    result = subprocess.run(
        ["sh", str(INSTALL_SH), *(args or [])],
        check=False,
        cwd=cwd,
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


def _powershell_bin() -> str | None:
    return shutil.which("pwsh") or shutil.which("powershell")


@requires_posix_shell
def test_install_sh_exists_and_is_executable():
    assert INSTALL_SH.is_file()
    assert os.access(INSTALL_SH, os.X_OK)


@requires_posix_shell
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


@requires_posix_shell
def test_install_sh_against_fake_repo_leaves_working_shim(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    result = _run_install(tmp_path, httpserver, pub_file)

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

    bin_dir = str(root / "bin")
    assert f'export PATH="{bin_dir}:$PATH"' in result.stdout
    assert str(home / ".local" / "bin") not in result.stdout
    for name in (".bashrc", ".bash_profile", ".bash_login", ".profile"):
        assert not (home / name).exists()


@requires_posix_shell
def test_install_sh_prints_export_path_when_skip_path_prompt(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    result = _run_install(
        tmp_path, httpserver, pub_file, extra_env={"SKIP_PATH_PROMPT": "1"}
    )
    home = tmp_path / "home"
    bin_dir = home / ".local" / "share" / "snowflake-cli" / "bin"
    assert f'export PATH="{bin_dir}:$PATH"' in result.stdout
    assert not (home / ".bashrc").exists()
    assert "SKIP_PATH_PROMPT" in result.stdout


def _assert_path_block(path: Path, bin_dir: Path, *, present: bool) -> None:
    if not present:
        if path.exists() and not path.is_symlink():
            assert PATH_BEGIN not in path.read_text()
        return
    text = path.read_text()
    assert PATH_BEGIN in text
    assert f'export PATH="{bin_dir}:$PATH"' in text
    assert str(path.parent / ".local" / "bin") not in text


@requires_posix_shell
@pytest.mark.parametrize("name", [".bash_profile", ".bash_login", ".profile"])
def test_install_sh_updates_existing_bash_login_profile(
    tmp_path: Path, httpserver: HTTPServer, name: str
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    home.mkdir()
    login = home / name
    login.write_text("# existing\n")
    _run_install(tmp_path, httpserver, pub_file)

    bin_dir = home / ".local" / "share" / "snowflake-cli" / "bin"
    _assert_path_block(login, bin_dir, present=True)
    for other in (".bashrc", ".bash_profile", ".bash_login", ".profile"):
        if other != name:
            assert not (home / other).exists()


@requires_posix_shell
def test_install_sh_updates_bashrc_when_it_already_exists(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    home.mkdir()
    profile = home / ".profile"
    bashrc = home / ".bashrc"
    profile.write_text("# profile\n")
    bashrc.write_text("# bashrc\n")
    _run_install(tmp_path, httpserver, pub_file)

    bin_dir = home / ".local" / "share" / "snowflake-cli" / "bin"
    _assert_path_block(profile, bin_dir, present=True)
    _assert_path_block(bashrc, bin_dir, present=True)
    assert not (home / ".bash_profile").exists()
    assert not (home / ".bash_login").exists()


@requires_posix_shell
def test_install_sh_bash_profile_symlink_to_profile_edits_target(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    home.mkdir()
    profile = home / ".profile"
    profile.write_text("# profile\n")
    bash_profile = home / ".bash_profile"
    bash_profile.symlink_to(".profile")
    _run_install(tmp_path, httpserver, pub_file)

    assert bash_profile.is_symlink()
    assert os.readlink(bash_profile) == ".profile"
    bin_dir = home / ".local" / "share" / "snowflake-cli" / "bin"
    _assert_path_block(profile, bin_dir, present=True)
    assert not (home / ".bashrc").exists()


@requires_posix_shell
def test_install_sh_skips_bash_profile_symlink_outside_login_chain(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    home.mkdir()
    outside = tmp_path / "outside"
    outside.write_text("# outside\n")
    bash_profile = home / ".bash_profile"
    bash_profile.symlink_to(outside)
    profile = home / ".profile"
    profile.write_text("# profile\n")
    bashrc = home / ".bashrc"
    bashrc.write_text("# bashrc\n")
    _run_install(tmp_path, httpserver, pub_file)

    assert bash_profile.is_symlink()
    assert outside.read_text() == "# outside\n"
    bin_dir = home / ".local" / "share" / "snowflake-cli" / "bin"
    _assert_path_block(profile, bin_dir, present=False)
    _assert_path_block(bashrc, bin_dir, present=True)


@requires_posix_shell
def test_install_sh_dangling_bash_profile_uses_next_login_profile(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    home.mkdir()
    (home / ".bash_profile").symlink_to("missing-profile")
    login = home / ".bash_login"
    login.write_text("# login\n")
    _run_install(tmp_path, httpserver, pub_file)

    bin_dir = home / ".local" / "share" / "snowflake-cli" / "bin"
    _assert_path_block(login, bin_dir, present=True)
    assert not (home / ".bashrc").exists()


@requires_posix_shell
def test_install_sh_creates_zshrc(tmp_path: Path, httpserver: HTTPServer):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    result = _run_install(
        tmp_path, httpserver, pub_file, extra_env={"SHELL": "/bin/zsh"}
    )
    home = tmp_path / "home"
    zshrc = home / ".zshrc"
    bin_dir = home / ".local" / "share" / "snowflake-cli" / "bin"
    _assert_path_block(zshrc, bin_dir, present=True)
    assert f"source {zshrc}" in result.stdout


@requires_posix_shell
@pytest.mark.parametrize(
    "version",
    ["../../.bashrc", "../evil", "bin", "3.13.1/../../tmp", "3.13.1\\win"],
)
def test_install_sh_rejects_unsanitized_version(
    tmp_path: Path, httpserver: HTTPServer, version: str
):
    _, pub_file = _test_keys(tmp_path)
    httpserver.expect_request("/stable_version.txt").respond_with_data(f"{version}\n")
    result = _run_install(tmp_path, httpserver, pub_file, check=False)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Invalid snowflake-managed version" in combined
    home = tmp_path / "home"
    assert not (home / ".bashrc").exists()
    root = home / ".local" / "share" / "snowflake-cli"
    assert not (root / "bin" / "snow").exists()


@requires_posix_shell
def test_install_sh_rejects_path_traversal_package_name(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(
        httpserver,
        tmp_path,
        "3.13.1",
        private_pem,
        package_name="../../evil.tar.gz",
        checksum="0" * 64,
    )
    result = _run_install(tmp_path, httpserver, pub_file, check=False)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Invalid package name" in combined
    home = tmp_path / "home"
    assert not (home / ".local" / "share" / "snowflake-cli" / "bin" / "snow").exists()


@requires_posix_shell
def test_install_sh_checksum_mismatch_fail_closed(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem, checksum="0" * 64)
    result = _run_install(tmp_path, httpserver, pub_file, check=False)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Checksum mismatch" in combined
    home = tmp_path / "home"
    shim = home / ".local" / "share" / "snowflake-cli" / "bin" / "snow"
    assert not shim.exists()


@requires_posix_shell
def test_install_sh_missing_signature_fail_closed(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem, serve_sig=False)
    result = _run_install(tmp_path, httpserver, pub_file, check=False)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "unsigned snowflake-managed package" in combined
    home = tmp_path / "home"
    shim = home / ".local" / "share" / "snowflake-cli" / "bin" / "snow"
    assert not shim.exists()


def _mode(path: Path) -> int:
    return path.stat().st_mode & 0o777


@requires_posix_shell
def test_install_sh_restricts_loose_config_files_owned_by_the_user(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    snowflake_home = home / ".snowflake"
    snowflake_home.mkdir(parents=True)
    connections = snowflake_home / "connections.toml"
    config = snowflake_home / "config.toml"
    connections.write_text('password = "secret"\n', encoding="utf-8")
    config.write_text("", encoding="utf-8")
    connections.chmod(0o644)
    config.chmod(0o640)
    untouched = home / ".config" / "snowflake" / "connections.toml"
    untouched.parent.mkdir(parents=True)
    untouched.write_text("other = true\n", encoding="utf-8")
    untouched.chmod(0o644)

    result = _run_install(tmp_path, httpserver, pub_file)

    assert _mode(connections) == 0o600
    assert _mode(config) == 0o600
    assert _mode(untouched) == 0o644
    assert f"Restricted permissions on {connections}" in result.stdout
    assert f"Restricted permissions on {config}" in result.stdout


@requires_posix_shell
def test_install_sh_does_not_follow_config_symlink(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    snowflake_home = home / ".snowflake"
    snowflake_home.mkdir(parents=True)
    target = home / "outside.toml"
    target.write_text('password = "secret"\n', encoding="utf-8")
    target.chmod(0o644)
    (snowflake_home / "connections.toml").symlink_to(target)

    result = _run_install(tmp_path, httpserver, pub_file)

    assert _mode(target) == 0o644
    assert (snowflake_home / "connections.toml").is_symlink()
    assert "Restricted permissions" not in result.stdout


@requires_posix_shell
def test_install_sh_leaves_already_strict_config_files(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    snowflake_home = home / ".snowflake"
    snowflake_home.mkdir(parents=True)
    connections = snowflake_home / "connections.toml"
    connections.write_text("", encoding="utf-8")
    connections.chmod(0o600)

    result = _run_install(tmp_path, httpserver, pub_file)

    assert _mode(connections) == 0o600
    assert "Restricted permissions" not in result.stdout


@requires_posix_shell
def test_install_sh_restricts_platformdirs_config_when_snowflake_home_is_absent(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    system = platform.system()
    if system == "Darwin":
        config_dir = home / "Library" / "Application Support" / "snowflake"
    elif system == "Linux":
        config_dir = home / ".config" / "snowflake"
    else:
        pytest.skip(f"no platformdirs path covered for {system}")
    config_dir.mkdir(parents=True)
    connections = config_dir / "connections.toml"
    connections.write_text('password = "secret"\n', encoding="utf-8")
    connections.chmod(0o644)

    result = _run_install(tmp_path, httpserver, pub_file)

    assert _mode(connections) == 0o600
    assert f"Restricted permissions on {connections}" in result.stdout


@requires_posix_shell
def test_install_sh_pins_version_from_flag(tmp_path: Path, httpserver: HTTPServer):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    _serve_version_package(httpserver, tmp_path, "3.12.0", private_pem)
    _run_install(tmp_path, httpserver, pub_file, args=["--version", "3.12.0"])

    home = tmp_path / "home"
    root = home / ".local" / "share" / "snowflake-cli"
    assert (root / ".current").read_text().strip() == "3.12.0"
    shim = root / "bin" / "snow"
    invoked = subprocess.run(
        [str(shim), "--version"], check=False, capture_output=True, text=True
    )
    assert invoked.returncode == 0, invoked.stderr
    assert "Snowflake CLI version: 3.12.0" in invoked.stdout


@requires_posix_shell
def test_install_sh_pins_version_from_env(tmp_path: Path, httpserver: HTTPServer):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    _serve_version_package(httpserver, tmp_path, "3.12.0", private_pem)
    _run_install(
        tmp_path,
        httpserver,
        pub_file,
        extra_env={"SNOWFLAKE_CLI_VERSION": "3.12.0"},
    )
    home = tmp_path / "home"
    root = home / ".local" / "share" / "snowflake-cli"
    assert (root / ".current").read_text().strip() == "3.12.0"


@requires_posix_shell
def test_install_sh_flag_version_overrides_env(tmp_path: Path, httpserver: HTTPServer):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    _serve_version_package(httpserver, tmp_path, "3.12.0", private_pem)
    _serve_version_package(httpserver, tmp_path, "3.11.0", private_pem)
    _run_install(
        tmp_path,
        httpserver,
        pub_file,
        extra_env={"SNOWFLAKE_CLI_VERSION": "3.11.0"},
        args=["--version=3.12.0"],
    )
    home = tmp_path / "home"
    root = home / ".local" / "share" / "snowflake-cli"
    assert (root / ".current").read_text().strip() == "3.12.0"


@requires_posix_shell
def test_install_sh_no_auto_upgrade_writes_config(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    result = _run_install(
        tmp_path,
        httpserver,
        pub_file,
        args=["--version", "3.13.1", "--no-auto-upgrade"],
    )
    home = tmp_path / "home"
    if platform.system() == "Darwin":
        config_dir = home / "Library" / "Application Support" / "snowflake"
    else:
        config_dir = home / ".config" / "snowflake"
    config = config_dir / "config.toml"
    assert not (home / ".snowflake").exists()
    assert config.is_file()
    text = config.read_text(encoding="utf-8")
    assert "[cli]" in text
    assert "auto_upgrade = false" in text
    assert _mode(config) == 0o600
    assert "cli.auto_upgrade = false" in result.stdout


@requires_posix_shell
def test_install_sh_no_auto_upgrade_updates_existing_config(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    snowflake_home = home / ".snowflake"
    snowflake_home.mkdir(parents=True)
    config = snowflake_home / "config.toml"
    config.write_text(
        'default_connection_name = "ci"\n\n[cli]\nauto_upgrade = true\n',
        encoding="utf-8",
    )
    config.chmod(0o600)
    _run_install(
        tmp_path,
        httpserver,
        pub_file,
        args=["--no-auto-upgrade"],
    )
    text = config.read_text(encoding="utf-8")
    assert 'default_connection_name = "ci"' in text
    assert "auto_upgrade = false" in text
    assert "auto_upgrade = true" not in text


CONFIG_SHAPES = {
    "cli_header_with_comment": "[cli] # mine\nauto_upgrade = true\n",
    "cli_header_with_spaces": "[ cli ]\nauto_upgrade = true\n",
    "root_dotted_key": "cli.auto_upgrade = true\n\n[connections.a]\nuser = 'x'\n",
    "cli_logs_without_cli": "[cli.logs]\nsave_logs = true\n",
    "cli_then_subtable": "[cli]\nfoo = 1\n[cli.logs]\nsave_logs = true\n",
    "connections_only": "[connections.a]\nuser = 'x'\n",
    "crlf": "[cli]\r\nfoo = 1\r\n[connections.a]\r\nuser = 'x'\r\n",
    "no_trailing_newline": "[connections.a]\nuser = 'x'",
    "uppercase_table": "[CLI]\nx = 1\n",
    "nested_array_value": "[cli]\nv = [\n  [1, 2],\n]\n",
    "quoted_header_symbols": (
        "[cli]\nfoo = 1\n[connections.\"me@acme/x#1\"]\nuser = 'x'\n"
    ),
    "quoted_header_non_ascii": "[cli]\nfoo = 1\n[connections.\"zażółć\"]\nuser = 'x'\n",
    "single_quoted_header": "[cli]\nfoo = 1\n[connections.'my conn']\nuser = 'x'\n",
    "single_quoted_cli": "['cli']\nauto_upgrade = true\n",
}

UNSAFE_CONFIG_SHAPES = {
    "array_of_tables": "[[cli]]\nx = 1\n",
    "root_dotted_other_key": "cli.foo = 1\n",
}


def _assert_auto_upgrade_disabled(config: Path, original: str) -> None:
    raw = config.read_bytes()
    assert not raw.startswith(b"\xef\xbb\xbf")
    doc = tomlkit.loads(raw.decode("utf-8")).unwrap()
    assert doc["cli"]["auto_upgrade"] is False
    original_doc = tomlkit.loads(original).unwrap()
    for key, value in original_doc.items():
        if key != "cli":
            assert doc[key] == value
    for key, value in original_doc.get("cli", {}).items():
        if key != "auto_upgrade":
            assert doc["cli"][key] == value


def _write_user_config(tmp_path: Path, content: str) -> Path:
    snowflake_home = tmp_path / "home" / ".snowflake"
    snowflake_home.mkdir(parents=True)
    config = snowflake_home / "config.toml"
    config.write_bytes(content.encode("utf-8"))
    config.chmod(0o600)
    return config


@requires_posix_shell
@pytest.mark.parametrize("content", CONFIG_SHAPES.values(), ids=CONFIG_SHAPES.keys())
def test_install_sh_no_auto_upgrade_handles_config_shapes(
    tmp_path: Path, httpserver: HTTPServer, content: str
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    config = _write_user_config(tmp_path, content)
    _run_install(tmp_path, httpserver, pub_file, args=["--no-auto-upgrade"])
    _assert_auto_upgrade_disabled(config, content)
    assert _mode(config) == 0o600
    assert [p.name for p in config.parent.iterdir()] == ["config.toml"]


@requires_posix_shell
@pytest.mark.parametrize(
    "content", UNSAFE_CONFIG_SHAPES.values(), ids=UNSAFE_CONFIG_SHAPES.keys()
)
def test_install_sh_no_auto_upgrade_refuses_unsafe_config(
    tmp_path: Path, httpserver: HTTPServer, content: str
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    config = _write_user_config(tmp_path, content)
    result = _run_install(
        tmp_path, httpserver, pub_file, args=["--no-auto-upgrade"], check=False
    )
    assert result.returncode != 0
    assert "Could not set cli.auto_upgrade" in result.stderr
    assert config.read_text(encoding="utf-8") == content
    assert [p.name for p in config.parent.iterdir()] == ["config.toml"]
    root = tmp_path / "home" / ".local" / "share" / "snowflake-cli"
    assert not (root / "bin" / "snow").exists()


@requires_posix_shell
def test_install_sh_no_auto_upgrade_refuses_symlinked_config(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    target = tmp_path / "dotfiles" / "config.toml"
    target.parent.mkdir()
    target.write_text("[cli]\nauto_upgrade = true\n", encoding="utf-8")
    snowflake_home = tmp_path / "home" / ".snowflake"
    snowflake_home.mkdir(parents=True)
    (snowflake_home / "config.toml").symlink_to(target)
    result = _run_install(
        tmp_path, httpserver, pub_file, args=["--no-auto-upgrade"], check=False
    )
    assert result.returncode != 0
    assert "is a symlink" in result.stderr
    assert (snowflake_home / "config.toml").is_symlink()
    assert "auto_upgrade = true" in target.read_text(encoding="utf-8")


def _platform_default_config_dir(home: Path) -> Path:
    # platformdirs 4.3.8 ignores XDG_CONFIG_HOME on macOS.
    if platform.system() == "Darwin":
        return home / "Library" / "Application Support" / "snowflake"
    return home / ".config" / "snowflake"


@requires_posix_shell
def test_install_sh_no_auto_upgrade_uses_absolute_xdg_config_home(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    xdg = tmp_path / "xdg"
    (xdg / "snowflake").mkdir(parents=True)
    home = tmp_path / "home"
    _run_install(
        tmp_path,
        httpserver,
        pub_file,
        extra_env={
            "SNOWFLAKE_HOME": str(tmp_path / "missing"),
            "XDG_CONFIG_HOME": str(xdg),
        },
        args=["--no-auto-upgrade"],
    )
    assert not (tmp_path / "missing").exists()
    if platform.system() == "Darwin":
        config = _platform_default_config_dir(home) / "config.toml"
    else:
        config = xdg / "snowflake" / "config.toml"
    assert "auto_upgrade = false" in config.read_text()


@requires_posix_shell
def test_install_sh_no_auto_upgrade_relative_xdg_config_home_matches_platformdirs(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    work = tmp_path / "work"
    work.mkdir()
    home = tmp_path / "home"
    _run_install(
        tmp_path,
        httpserver,
        pub_file,
        extra_env={"XDG_CONFIG_HOME": "relative"},
        args=["--no-auto-upgrade"],
        cwd=work,
    )
    if platform.system() == "Darwin":
        config = _platform_default_config_dir(home) / "config.toml"
    else:
        # platformdirs 4.3.8 uses any non-blank value, relative to the cwd.
        config = work / "relative" / "snowflake" / "config.toml"
    assert "auto_upgrade = false" in config.read_text()


@requires_posix_shell
def test_install_sh_no_auto_upgrade_blank_xdg_config_home_uses_default(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    home = tmp_path / "home"
    _run_install(
        tmp_path,
        httpserver,
        pub_file,
        extra_env={"XDG_CONFIG_HOME": "   "},
        args=["--no-auto-upgrade"],
    )
    config = _platform_default_config_dir(home) / "config.toml"
    assert "auto_upgrade = false" in config.read_text()


@requires_posix_shell
@pytest.mark.parametrize("args", [["--version="], ["--version", " "]])
def test_install_sh_rejects_empty_version_flag(
    tmp_path: Path, httpserver: HTTPServer, args: list[str]
):
    _, pub_file = _test_keys(tmp_path)
    result = _run_install(tmp_path, httpserver, pub_file, args=args, check=False)
    assert result.returncode != 0
    assert "--version requires a value" in result.stderr


@requires_posix_shell
def test_install_sh_blank_version_env_installs_stable(
    tmp_path: Path, httpserver: HTTPServer
):
    private_pem, pub_file = _test_keys(tmp_path)
    _serve_release(httpserver, tmp_path, "3.13.1", private_pem)
    result = _run_install(
        tmp_path, httpserver, pub_file, extra_env={"SNOWFLAKE_CLI_VERSION": " "}
    )
    assert "(pinned)" not in result.stdout
    root = tmp_path / "home" / ".local" / "share" / "snowflake-cli"
    assert (root / ".current").read_text().strip() == "3.13.1"


@requires_posix_shell
def test_install_sh_rejects_unknown_option(tmp_path: Path, httpserver: HTTPServer):
    _, pub_file = _test_keys(tmp_path)
    result = _run_install(
        tmp_path,
        httpserver,
        pub_file,
        args=["--auto-upgrade=false"],
        check=False,
    )
    assert result.returncode != 0
    assert "Unknown option: --auto-upgrade=false" in (result.stdout + result.stderr)
    home = tmp_path / "home"
    assert not (home / ".snowflake" / "config.toml").exists()


@requires_posix_shell
def test_install_sh_help_exits_zero_without_installing(
    tmp_path: Path, httpserver: HTTPServer
):
    _, pub_file = _test_keys(tmp_path)
    result = _run_install(tmp_path, httpserver, pub_file, args=["--help"])
    assert "Usage: install.sh" in result.stdout
    home = tmp_path / "home"
    assert not (home / ".local" / "share" / "snowflake-cli" / "bin" / "snow").exists()


def test_install_ps1_is_snowflake_managed_not_cortex():
    text = INSTALL_PS1.read_text(encoding="utf-8")
    assert "snowflake-cli" in text
    assert 'BinaryName = "snow.exe"' in text
    assert "LOCALAPPDATA" in text
    assert r"snowflake-cli\bin" in text or "snowflake-cli" in text
    assert "cortex-code-cli" not in text
    assert "snow.exe" in text
    assert "Assert-ManagedVersion" in text
    assert "Assert-PackageName" in text
    assert "Test-ManifestSignature" in text
    assert "Protect-SnowflakeConfigFiles" in text
    assert "connections.toml" in text
    assert "config.toml" in text
    assert "ManifestSigName" in text
    assert "Resolve-InstallVersion" in text
    assert "Disable-AutoUpgradeConfig" in text
    assert "-NoAutoUpgrade" in text
    assert "SNOWFLAKE_CLI_VERSION" in text
    assert "native" not in text.lower()
    assert "self-managed" not in text.lower()


def test_install_ps1_disables_auto_upgrade_before_installing():
    text = INSTALL_PS1.read_text(encoding="utf-8")
    call = text.index("\n            Disable-AutoUpgradeConfig\n")
    assert text.index("Test-Checksum $tarPath") < call
    assert call < text.index("$installDir = Join-Path $InstallBaseDir $version")


def test_install_ps1_parses():
    shell = _powershell_bin()
    if shell is None:
        pytest.skip("PowerShell is not installed")
    ps_path = "'" + str(INSTALL_PS1).replace("'", "''") + "'"
    script = (
        "$errors = $null\n"
        f"$null = [System.Management.Automation.Language.Parser]::ParseFile("
        f"{ps_path}, [ref]$null, [ref]$errors)\n"
        "if ($errors) {\n"
        "  $errors | ForEach-Object { $_.ToString() }\n"
        "  exit 1\n"
        "}\n"
    )
    result = subprocess.run(
        [shell, "-NoProfile", "-NonInteractive", "-Command", script],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def _require_powershell() -> str:
    shell = _powershell_bin()
    if shell is None:
        pytest.skip("PowerShell is not installed")
    assert shell is not None
    return shell


def _run_ps1_functions(
    shell: str, body: str, user_profile: Path, args: list[str] | None = None
) -> subprocess.CompletedProcess[str]:
    ps_path = "'" + str(INSTALL_PS1).replace("'", "''") + "'"
    loader = (
        "$tokens = $null; $errors = $null\n"
        f"$ast = [System.Management.Automation.Language.Parser]::ParseFile("
        f"{ps_path}, [ref]$tokens, [ref]$errors)\n"
        "foreach ($f in $ast.FindAll({ param($n) "
        "$n -is [System.Management.Automation.Language.FunctionDefinitionAst] "
        "}, $false)) {\n"
        "  . ([scriptblock]::Create($f.Extent.Text))\n"
        "}\n"
        "function Protect-SnowflakeConfigFile { param($Path) }\n"
        "$script:RequestedVersion = $null; $script:DisableAutoUpgrade = $false\n"
    )
    script_file = user_profile.parent / "run.ps1"
    script_file.write_text(loader + body, encoding="utf-8")
    env = os.environ.copy()
    env.pop("SNOWFLAKE_HOME", None)
    env["USERPROFILE"] = str(user_profile)
    env["LOCALAPPDATA"] = str(user_profile / "AppData" / "Local")
    return subprocess.run(
        [shell, "-NoProfile", "-NonInteractive", "-File", str(script_file)]
        + (args or []),
        check=False,
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
    )


@pytest.mark.parametrize("content", CONFIG_SHAPES.values(), ids=CONFIG_SHAPES.keys())
def test_install_ps1_disable_auto_upgrade_handles_config_shapes(
    tmp_path: Path, content: str
):
    shell = _require_powershell()
    profile = tmp_path / "profile"
    config = profile / ".snowflake" / "config.toml"
    config.parent.mkdir(parents=True)
    config.write_bytes(content.encode("utf-8"))
    result = _run_ps1_functions(shell, "Disable-AutoUpgradeConfig\n", profile)
    assert result.returncode == 0, result.stdout + result.stderr
    _assert_auto_upgrade_disabled(config, content)
    assert [p.name for p in config.parent.iterdir()] == ["config.toml"]


@pytest.mark.parametrize(
    "content", UNSAFE_CONFIG_SHAPES.values(), ids=UNSAFE_CONFIG_SHAPES.keys()
)
def test_install_ps1_disable_auto_upgrade_refuses_unsafe_config(
    tmp_path: Path, content: str
):
    shell = _require_powershell()
    profile = tmp_path / "profile"
    config = profile / ".snowflake" / "config.toml"
    config.parent.mkdir(parents=True)
    config.write_bytes(content.encode("utf-8"))
    result = _run_ps1_functions(shell, "Disable-AutoUpgradeConfig\n", profile)
    assert result.returncode != 0
    assert "Could not set cli.auto_upgrade" in result.stdout + result.stderr
    assert config.read_bytes() == content.encode("utf-8")


def test_install_ps1_disable_auto_upgrade_preserves_non_ascii(tmp_path: Path):
    shell = _require_powershell()
    profile = tmp_path / "profile"
    config = profile / ".snowflake" / "config.toml"
    config.parent.mkdir(parents=True)
    original = "[connections.a]\npassword = 'zażółć'\n".encode("utf-8")
    config.write_bytes(original)
    result = _run_ps1_functions(shell, "Disable-AutoUpgradeConfig\n", profile)
    assert result.returncode == 0, result.stdout + result.stderr
    assert config.read_bytes().startswith(original)


@pytest.mark.parametrize(
    "args, expected",
    [
        (["-Version", "3.29.0", "-NoAutoUpgrade"], "version=[3.29.0] noau=True"),
        (["--version=3.30.0", "--no-auto-upgrade"], "version=[3.30.0] noau=True"),
    ],
)
def test_install_ps1_parses_install_args(
    tmp_path: Path, args: list[str], expected: str
):
    shell = _require_powershell()
    body = (
        "Parse-InstallArgs -ArgList $args\n"
        '"version=[$script:RequestedVersion] noau=$script:DisableAutoUpgrade"\n'
    )
    result = _run_ps1_functions(shell, body, tmp_path / "profile", args)
    assert result.returncode == 0, result.stdout + result.stderr
    assert expected in result.stdout


@pytest.mark.parametrize("args", [["--version="], ["-Version", ""], ["--bogus"]])
def test_install_ps1_rejects_bad_install_args(tmp_path: Path, args: list[str]):
    shell = _require_powershell()
    result = _run_ps1_functions(
        shell, "Parse-InstallArgs -ArgList $args\n", tmp_path / "profile", args
    )
    assert result.returncode != 0
