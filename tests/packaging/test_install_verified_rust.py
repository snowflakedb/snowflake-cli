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


class _OsNameProxy:
    """Fake os.name without mutating the real os module (pathlib breaks on macOS)."""

    def __init__(self, real_os, name: str):
        object.__setattr__(self, "_real", real_os)
        object.__setattr__(self, "name", name)

    def __getattr__(self, item):
        return getattr(self._real, item)


@pytest.fixture
def helper():
    spec = importlib.util.spec_from_file_location("install_verified_rust", HELPER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _set_os_name(helper, monkeypatch, name: str) -> None:
    monkeypatch.setattr(helper, "os", _OsNameProxy(helper.os, name))


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
        helper.download_https_urllib("https://example.invalid/rustup-init", dest)
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


def test_download_https_curl_rejects_non_archive_url(helper, tmp_path, monkeypatch):
    dest = tmp_path / "rustup-init.exe"
    monkeypatch.setattr(helper, "resolve_windows_curl", lambda: "curl.exe")

    def run_fn(cmd, **_kwargs):
        raise AssertionError(f"curl must not run for unpinned URL: {cmd}")

    with pytest.raises(ValueError, match="pinned rustup archive"):
        helper.download_https_curl(
            "https://example.invalid/rustup-init.exe",
            dest,
            run_fn=run_fn,
        )
    assert not dest.exists()


def test_download_https_curl_writes_bytes_and_verifies_flags(
    helper, tmp_path, monkeypatch
):
    dest = tmp_path / "rustup-init.exe"
    url = helper.rustup_init_url("x86_64-pc-windows-msvc")
    curl_path = str(tmp_path / "System32" / "curl.exe")
    seen_cmds: list[list[str]] = []
    seen_envs: list[dict[str, str]] = []
    seen_timeouts: list[object] = []

    def run_fn(cmd, **kwargs):
        seen_cmds.append(list(cmd))
        seen_envs.append(dict(kwargs.get("env") or {}))
        seen_timeouts.append(kwargs.get("timeout"))
        dest.write_bytes(FIXTURE_BYTES)
        return SimpleNamespace(returncode=0, stdout=f"200 {url}", stderr="")

    monkeypatch.setattr(helper, "resolve_windows_curl", lambda: curl_path)
    helper.download_https_curl(url, dest, run_fn=run_fn)
    assert dest.read_bytes() == FIXTURE_BYTES
    assert len(seen_cmds) == 1
    cmd = seen_cmds[0]
    assert cmd[0] == curl_path
    assert "--fail" in cmd
    assert "--location" in cmd
    assert "--proto" in cmd and cmd[cmd.index("--proto") + 1] == "=https"
    assert "--proto-redir" in cmd and cmd[cmd.index("--proto-redir") + 1] == "=https"
    assert "--connect-timeout" in cmd
    assert cmd[cmd.index("--connect-timeout") + 1] == str(helper.CURL_CONNECT_TIMEOUT_S)
    assert "--max-time" in cmd
    assert cmd[cmd.index("--max-time") + 1] == str(helper.CURL_MAX_TIME_S)
    assert "--write-out" in cmd
    assert "-k" not in cmd
    assert "--insecure" not in cmd
    assert url in cmd
    assert seen_timeouts == [helper.CURL_SUBPROCESS_TIMEOUT_S]
    for key in helper.CURL_TRUST_ENV_BLOCKLIST:
        assert key not in seen_envs[0]


def test_windows_system32_curl_prefers_system_root(helper, tmp_path):
    system_root = tmp_path / "Windows"
    system32 = system_root / "System32"
    system32.mkdir(parents=True)
    curl = system32 / "curl.exe"
    curl.write_bytes(b"")
    assert helper.windows_system32_curl(str(system_root)) == str(curl)
    assert helper.windows_system32_curl(None) is None
    assert helper.windows_system32_curl(str(tmp_path / "missing")) is None


def test_windows_system32_curl_prefers_sysnative(helper, tmp_path):
    system_root = tmp_path / "Windows"
    sysnative = system_root / "Sysnative"
    system32 = system_root / "System32"
    sysnative.mkdir(parents=True)
    system32.mkdir(parents=True)
    (sysnative / "curl.exe").write_bytes(b"")
    (system32 / "curl.exe").write_bytes(b"")
    assert helper.windows_system32_curl(str(system_root)) == str(sysnative / "curl.exe")


def test_resolve_windows_curl_fails_closed_without_system32(helper, monkeypatch):
    _set_os_name(helper, monkeypatch, "nt")
    monkeypatch.setattr(helper, "windows_system32_curl", lambda _root: None)
    monkeypatch.setenv("SystemRoot", r"C:\Windows")
    with pytest.raises(OSError, match="System32 curl.exe"):
        helper.resolve_windows_curl()


def test_resolve_windows_curl_rejects_posix(helper, monkeypatch):
    _set_os_name(helper, monkeypatch, "posix")
    with pytest.raises(OSError, match="Windows-only"):
        helper.resolve_windows_curl()


def test_download_https_curl_strips_ca_bundle_env(helper, tmp_path, monkeypatch):
    dest = tmp_path / "rustup-init.exe"
    url = helper.rustup_init_url("x86_64-pc-windows-msvc")
    monkeypatch.setattr(helper, "resolve_windows_curl", lambda: "curl.exe")
    monkeypatch.setenv("CURL_CA_BUNDLE", r"C:\broken\cacert.pem")
    monkeypatch.setenv("SSL_CERT_FILE", r"C:\broken\cacert.pem")
    monkeypatch.setenv("SSL_CERT_DIR", r"C:\broken\certs")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", r"C:\broken\cacert.pem")

    def run_fn(cmd, **kwargs):
        env = kwargs.get("env") or {}
        for key in helper.CURL_TRUST_ENV_BLOCKLIST:
            assert key not in env
        dest.write_bytes(FIXTURE_BYTES)
        return SimpleNamespace(returncode=0, stdout=f"200 {url}", stderr="")

    helper.download_https_curl(url, dest, run_fn=run_fn)
    assert dest.read_bytes() == FIXTURE_BYTES


def test_download_https_curl_rejects_off_archive_effective_url(
    helper, tmp_path, monkeypatch
):
    dest = tmp_path / "rustup-init.exe"
    url = helper.rustup_init_url("x86_64-pc-windows-msvc")
    monkeypatch.setattr(helper, "resolve_windows_curl", lambda: "curl.exe")

    def run_fn(cmd, **_kwargs):
        dest.write_bytes(FIXTURE_BYTES)
        return SimpleNamespace(
            returncode=0,
            stdout="200 https://example.invalid/rustup-init.exe",
            stderr="",
        )

    with pytest.raises(ValueError, match="pinned rustup archive"):
        helper.download_https_curl(url, dest, run_fn=run_fn)
    assert not dest.exists()


def test_download_https_curl_rejects_empty_effective_url(helper, tmp_path, monkeypatch):
    dest = tmp_path / "rustup-init.exe"
    url = helper.rustup_init_url("x86_64-pc-windows-msvc")
    monkeypatch.setattr(helper, "resolve_windows_curl", lambda: "curl.exe")

    def run_fn(cmd, **_kwargs):
        dest.write_bytes(FIXTURE_BYTES)
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    with pytest.raises(OSError, match="missing http_code/url_effective"):
        helper.download_https_curl(url, dest, run_fn=run_fn)
    assert not dest.exists()


def test_download_https_curl_hard_fails_on_nonzero(helper, tmp_path, monkeypatch):
    dest = tmp_path / "rustup-init.exe"
    url = helper.rustup_init_url("x86_64-pc-windows-msvc")
    monkeypatch.setattr(
        helper, "resolve_windows_curl", lambda: r"C:\Windows\System32\curl.exe"
    )

    def run_fn(cmd, **_kwargs):
        dest.write_bytes(b"partial")
        return SimpleNamespace(
            returncode=60,
            stdout="",
            stderr="SSL certificate problem: unable to get local issuer certificate",
        )

    with pytest.raises(OSError, match=r"path=.*curl\.exe.*rc=60"):
        helper.download_https_curl(url, dest, run_fn=run_fn)
    assert not dest.exists()


def test_download_https_curl_hard_fails_on_empty(helper, tmp_path, monkeypatch):
    dest = tmp_path / "rustup-init.exe"
    url = helper.rustup_init_url("x86_64-pc-windows-msvc")
    monkeypatch.setattr(helper, "resolve_windows_curl", lambda: "curl.exe")

    def run_fn(cmd, **_kwargs):
        dest.write_bytes(b"")
        return SimpleNamespace(returncode=0, stdout=f"200 {url}", stderr="")

    with pytest.raises(OSError, match="empty rustup-init"):
        helper.download_https_curl(url, dest, run_fn=run_fn)
    assert not dest.exists()


def test_download_https_curl_times_out(helper, tmp_path, monkeypatch):
    dest = tmp_path / "rustup-init.exe"
    url = helper.rustup_init_url("x86_64-pc-windows-msvc")
    monkeypatch.setattr(helper, "resolve_windows_curl", lambda: "curl.exe")

    def run_fn(cmd, **_kwargs):
        dest.write_bytes(b"partial")
        raise helper.subprocess.TimeoutExpired(cmd=cmd, timeout=150)

    with pytest.raises(OSError, match="timed out"):
        helper.download_https_curl(url, dest, run_fn=run_fn)
    assert not dest.exists()


def test_download_https_dispatches_to_curl_on_windows(helper, tmp_path, monkeypatch):
    dest = tmp_path / "rustup-init.exe"
    url = helper.rustup_init_url("x86_64-pc-windows-msvc")
    called: list[str] = []

    def fake_curl(download_url, download_dest, *, run_fn=None):
        called.append("curl")
        assert download_url == url
        download_dest.write_bytes(FIXTURE_BYTES)

    def fake_urllib(download_url, download_dest):
        called.append("urllib")
        raise AssertionError("urllib path must not run on Windows")

    _set_os_name(helper, monkeypatch, "nt")
    monkeypatch.setattr(helper, "download_https_curl", fake_curl)
    monkeypatch.setattr(helper, "download_https_urllib", fake_urllib)
    helper.download_https(url, dest)
    assert called == ["curl"]
    assert dest.read_bytes() == FIXTURE_BYTES


def test_download_https_dispatches_to_urllib_on_posix(helper, tmp_path, monkeypatch):
    dest = tmp_path / "rustup-init"
    url = helper.rustup_init_url("x86_64-unknown-linux-gnu")
    called: list[str] = []

    def fake_curl(download_url, download_dest, *, run_fn=None):
        called.append("curl")
        raise AssertionError("curl path must not run on posix default")

    def fake_urllib(download_url, download_dest):
        called.append("urllib")
        download_dest.write_bytes(FIXTURE_BYTES)

    _set_os_name(helper, monkeypatch, "posix")
    monkeypatch.setattr(helper, "download_https_curl", fake_curl)
    monkeypatch.setattr(helper, "download_https_urllib", fake_urllib)
    helper.download_https(url, dest)
    assert called == ["urllib"]
    assert dest.read_bytes() == FIXTURE_BYTES


def test_log_trust_diagnostics_noop_on_posix(helper, capsys, monkeypatch):
    _set_os_name(helper, monkeypatch, "posix")
    helper.log_trust_diagnostics()
    assert capsys.readouterr().out == ""


def test_log_trust_diagnostics_survives_curl_timeout(helper, capsys, monkeypatch):
    _set_os_name(helper, monkeypatch, "nt")
    monkeypatch.setattr(
        helper, "resolve_windows_curl", lambda: r"C:\Windows\System32\curl.exe"
    )
    monkeypatch.setattr(helper.shutil, "which", lambda _name: None)

    def run_fn(cmd, **_kwargs):
        raise helper.subprocess.TimeoutExpired(cmd=cmd, timeout=5)

    helper.log_trust_diagnostics(run_fn=run_fn)
    captured = capsys.readouterr().out
    assert "probe-failed:" in captured
    assert "rustc=not-on-path" in captured


def test_curl_download_env_strips_blocklist(helper, monkeypatch):
    monkeypatch.setenv("CURL_CA_BUNDLE", "x")
    monkeypatch.setenv("SSL_CERT_FILE", "y")
    monkeypatch.setenv("SSL_CERT_DIR", "z")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "w")
    monkeypatch.setenv("PATH", "/bin")
    env = helper.curl_download_env()
    for key in helper.CURL_TRUST_ENV_BLOCKLIST:
        assert key not in env
    assert env.get("PATH") == "/bin"


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
        log_diagnostics=False,
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
        log_diagnostics=False,
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
        log_diagnostics=False,
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
            log_diagnostics=False,
        )


def test_install_skips_diagnostics_on_posix_by_default(
    helper, tmp_path, monkeypatch, capsys
):
    _set_os_name(helper, monkeypatch, "posix")
    monkeypatch.setattr(helper, "expected_sha256", lambda _target: FIXTURE_SHA256)
    called: list[str] = []
    monkeypatch.setattr(
        helper, "log_trust_diagnostics", lambda **_k: called.append("diag")
    )
    helper.install_verified_rust(
        system="Linux",
        machine="x86_64",
        dest_dir=tmp_path,
        download_fn=_write_fixture,
        run_fn=lambda *_a, **_k: SimpleNamespace(returncode=0),
    )
    assert called == []
    assert "diag-marker" not in capsys.readouterr().out


def test_install_logs_diagnostics_on_windows_by_default(
    helper, tmp_path, monkeypatch, capsys
):
    _set_os_name(helper, monkeypatch, "nt")
    monkeypatch.setattr(helper, "expected_sha256", lambda _target: FIXTURE_SHA256)
    monkeypatch.setattr(
        helper, "log_trust_diagnostics", lambda **_k: print("diag-marker")
    )
    helper.install_verified_rust(
        system="Windows",
        machine="AMD64",
        dest_dir=tmp_path,
        download_fn=_write_fixture,
        run_fn=lambda *_a, **_k: SimpleNamespace(returncode=0),
    )
    assert "diag-marker" in capsys.readouterr().out


def test_install_does_not_skip_when_wrong_rustc_on_path(
    helper, tmp_path, monkeypatch, capsys
):
    downloaded: list[str] = []

    def download_fn(url: str, dest: Path) -> None:
        downloaded.append(url)
        dest.write_bytes(FIXTURE_BYTES)

    _set_os_name(helper, monkeypatch, "nt")
    monkeypatch.setattr(helper, "expected_sha256", lambda _target: FIXTURE_SHA256)
    monkeypatch.setattr(
        helper, "resolve_windows_curl", lambda: r"C:\Windows\System32\curl.exe"
    )
    monkeypatch.setattr(helper.shutil, "which", lambda name: r"C:\rustc.exe")

    def run_fn(cmd, **_kwargs):
        if cmd and str(cmd[0]).endswith("rustc.exe"):
            return SimpleNamespace(returncode=0, stdout="rustc 1.89.0", stderr="")
        if cmd and str(cmd[0]).endswith("curl.exe"):
            return SimpleNamespace(
                returncode=0, stdout="curl 8.0.0 (Windows) schannel", stderr=""
            )
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    helper.install_verified_rust(
        system="Windows",
        machine="AMD64",
        dest_dir=tmp_path,
        download_fn=download_fn,
        run_fn=run_fn,
    )
    assert downloaded
    out = capsys.readouterr().out
    assert "1.89.0" in out
    assert "curl_pin=system32" in out


def test_main_returns_error_for_unsupported_host(helper, monkeypatch):
    monkeypatch.setattr(helper.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(helper.platform, "machine", lambda: "ppc64")
    assert helper.main([]) == 1


def test_main_returns_error_for_curl_oserror(helper, monkeypatch):
    monkeypatch.setattr(helper.platform, "system", lambda: "Windows")
    monkeypatch.setattr(helper.platform, "machine", lambda: "AMD64")

    def boom(**_kwargs):
        raise OSError("curl rustup-init download failed (rc=60): SSL")

    monkeypatch.setattr(helper, "install_verified_rust", boom)
    assert helper.main([]) == 1


def test_main_returns_error_for_timeout_expired(helper, monkeypatch):
    monkeypatch.setattr(helper.platform, "system", lambda: "Windows")
    monkeypatch.setattr(helper.platform, "machine", lambda: "AMD64")

    def boom(**_kwargs):
        raise helper.subprocess.TimeoutExpired(cmd=["curl"], timeout=150)

    monkeypatch.setattr(helper, "install_verified_rust", boom)
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
