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
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
HELPER_PATH = REPO_ROOT / "scripts" / "packaging" / "install_verified_rust.py"
DARWIN_SCRIPT = REPO_ROOT / "scripts" / "packaging" / "build_darwin_package.sh"
LINUX_SCRIPT = REPO_ROOT / "scripts" / "packaging" / "build_binaries.sh"
WINDOWS_SCRIPT = REPO_ROOT / "scripts" / "packaging" / "win" / "build_snowflake_cli.bat"

FIXTURE_BYTES = b"synthetic-rustup-init"
FIXTURE_SHA256 = hashlib.sha256(FIXTURE_BYTES).hexdigest()


@pytest.fixture
def helper():
    spec = importlib.util.spec_from_file_location("install_verified_rust", HELPER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("system", "machine", "target"),
    [
        ("Darwin", "arm64", "aarch64-apple-darwin"),
        ("darwin", "x86_64", "x86_64-apple-darwin"),
        ("Linux", "x86_64", "x86_64-unknown-linux-gnu"),
        ("linux", "aarch64", "aarch64-unknown-linux-gnu"),
        ("Windows", "AMD64", "x86_64-pc-windows-msvc"),
        ("Windows", "ARM64", "aarch64-pc-windows-msvc"),
    ],
)
def test_rustup_target_maps_packaging_hosts(helper, system, machine, target):
    assert helper.rustup_target(system, machine) == target


def test_rustup_target_rejects_unknown_host(helper):
    with pytest.raises(helper.UnsupportedHostError):
        helper.rustup_target("Darwin", "ppc64")


def test_rustup_init_url_stays_on_pinned_archive(helper):
    url = helper.rustup_init_url("x86_64-unknown-linux-gnu")
    assert url.startswith(helper.RUSTUP_ARCHIVE_PREFIX)
    assert url.endswith("/rustup-init")
    assert helper.RUSTUP_VERSION in url


def test_verify_sha256_accepts_matching_file(helper, tmp_path):
    path = tmp_path / "rustup-init"
    path.write_bytes(FIXTURE_BYTES)
    helper.verify_sha256(path, FIXTURE_SHA256)


def test_verify_sha256_rejects_mismatch_and_empty(helper, tmp_path):
    path = tmp_path / "rustup-init"
    path.write_bytes(b"other-bytes")
    with pytest.raises(helper.ChecksumMismatchError):
        helper.verify_sha256(path, FIXTURE_SHA256)
    empty = tmp_path / "empty"
    empty.write_bytes(b"")
    with pytest.raises(helper.ChecksumMismatchError):
        helper.verify_sha256(empty, FIXTURE_SHA256)
    missing = tmp_path / "missing"
    with pytest.raises(helper.ChecksumMismatchError):
        helper.verify_sha256(missing, FIXTURE_SHA256)


@pytest.mark.parametrize(
    "url",
    [
        "https://example.invalid/rustup-init",
        "http://static.rust-lang.org/rustup/archive/1.29.1/x86_64-unknown-linux-gnu/rustup-init",
        "https://static.rust-lang.org/rustup/archive/1.29.1/../../other/rustup-init",
        "https://static.rust-lang.org/rustup/archive/1.29.1/x86_64-unknown-linux-gnu/rustup-init?x=1",
        "https://static.rust-lang.org/rustup/archive/1.29.1/not-a-target/rustup-init",
    ],
)
def test_assert_archive_url_rejects_unpinned_locations(helper, url):
    with pytest.raises(ValueError, match="pinned rustup archive"):
        helper.assert_archive_url(url)


def test_assert_archive_url_accepts_pinned_artifact(helper):
    helper.assert_archive_url(helper.rustup_init_url("x86_64-unknown-linux-gnu"))


def test_download_https_rejects_non_archive_url(helper, tmp_path):
    dest = tmp_path / "rustup-init"
    with pytest.raises(ValueError, match="pinned rustup archive"):
        helper.download_https("https://example.invalid/rustup-init", dest)
    assert not dest.exists()


def test_redirect_handler_rejects_off_archive_location(helper):
    handler = helper.ArchiveRedirectHandler()
    request = helper.urllib.request.Request(
        helper.rustup_init_url("x86_64-unknown-linux-gnu")
    )
    with pytest.raises(ValueError, match="pinned rustup archive"):
        handler.redirect_request(
            request,
            fp=None,
            code=302,
            msg="Found",
            headers={},
            newurl="https://example.invalid/rustup-init",
        )


def _write_fixture(_url: str, dest: Path) -> None:
    dest.write_bytes(FIXTURE_BYTES)


def test_install_runs_only_after_matching_digest(helper, tmp_path, monkeypatch):
    commands: list[list[str]] = []

    def run_fn(cmd, **_kwargs):
        commands.append(list(cmd))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(helper, "expected_sha256", lambda _target: FIXTURE_SHA256)
    helper.install_verified_rust(
        system="Linux",
        machine="x86_64",
        dest_dir=tmp_path,
        download_fn=_write_fixture,
        run_fn=run_fn,
    )
    assert commands[0][0].endswith("rustup-init")
    assert commands[0][1:] == ["-y", "--default-toolchain", helper.RUST_TOOLCHAIN]
    assert commands[1][-2:] == ["default", helper.RUST_TOOLCHAIN]
    assert not (tmp_path / "rustup-init").exists()


def test_install_uses_windows_installer_name(helper, tmp_path, monkeypatch):
    commands: list[list[str]] = []

    def run_fn(cmd, **_kwargs):
        commands.append(list(cmd))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(helper, "expected_sha256", lambda _target: FIXTURE_SHA256)
    helper.install_verified_rust(
        system="Windows",
        machine="AMD64",
        dest_dir=tmp_path,
        download_fn=_write_fixture,
        run_fn=run_fn,
    )
    assert commands[0][0].endswith("rustup-init.exe")
    assert commands[0][1:] == ["-y", "--default-toolchain", helper.RUST_TOOLCHAIN]


def test_install_forwards_no_modify_path(helper, tmp_path, monkeypatch):
    commands: list[list[str]] = []

    def run_fn(cmd, **_kwargs):
        commands.append(list(cmd))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(helper, "expected_sha256", lambda _target: FIXTURE_SHA256)
    helper.install_verified_rust(
        system="Darwin",
        machine="x86_64",
        no_modify_path=True,
        dest_dir=tmp_path,
        download_fn=_write_fixture,
        run_fn=run_fn,
    )
    assert "--no-modify-path" in commands[0]


def test_install_does_not_run_on_digest_mismatch(helper, tmp_path, monkeypatch):
    def run_fn(cmd, **_kwargs):
        raise AssertionError(f"installer must not run: {cmd}")

    monkeypatch.setattr(helper, "expected_sha256", lambda _target: FIXTURE_SHA256)
    with pytest.raises(helper.ChecksumMismatchError):
        helper.install_verified_rust(
            system="Linux",
            machine="x86_64",
            dest_dir=tmp_path,
            download_fn=lambda _url, dest: dest.write_bytes(b"tampered"),
            run_fn=run_fn,
        )


def test_main_returns_error_for_unsupported_host(helper, monkeypatch):
    monkeypatch.setattr(helper.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(helper.platform, "machine", lambda: "ppc64")
    assert helper.main([]) == 1


def test_packaging_scripts_call_verified_installer():
    darwin = DARWIN_SCRIPT.read_text()
    linux = LINUX_SCRIPT.read_text()
    windows = WINDOWS_SCRIPT.read_text()
    assert "install_verified_rust.py" in darwin
    assert "install_verified_rust.py" in linux
    assert "install_verified_rust.py" in windows
    windows_lines = [line.strip().lower() for line in windows.splitlines()]
    helper_idx = next(
        i for i, line in enumerate(windows_lines) if "install_verified_rust.py" in line
    )
    assert "errorlevel" in windows_lines[helper_idx + 1]
    assert "exit" in windows_lines[helper_idx + 1]
    assert "sh.rustup.rs" not in darwin
    assert "sh.rustup.rs" not in linux
    assert "win.rustup.rs" not in windows
    assert "rustup default stable" not in darwin
