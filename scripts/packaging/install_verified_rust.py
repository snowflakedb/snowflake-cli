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

"""Install a pinned rustup-init after verifying its SHA-256, then select a pinned toolchain.

Checksums are the published rustup 1.29.1 archive files at
https://static.rust-lang.org/rustup/archive/1.29.1/<target>/rustup-init[.exe].sha256
The toolchain pin is rustc 1.98.1.

On Windows packaging agents, Python urllib often lacks corporate CA roots
(CERTIFICATE_VERIFY_FAILED). Downloads use curl.exe + schannel with verify on;
integrity still hard-fails on SHA-256 mismatch. Soft-skip / --insecure are banned.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import platform
import shutil
import stat
import subprocess
import sys
import tempfile
import urllib.request
from collections.abc import Callable
from pathlib import Path
from urllib.parse import urlparse

RUSTUP_VERSION = "1.29.1"
RUST_TOOLCHAIN = "1.98.1"
RUSTUP_ARCHIVE_PREFIX = f"https://static.rust-lang.org/rustup/archive/{RUSTUP_VERSION}/"

# Published SHA-256 of rustup-init 1.29.1 for each packaging host.
RUSTUP_INIT_SHA256 = {
    "aarch64-apple-darwin": (
        "ec1b9233e7f72990ecd8e62063fa7f6c3dfc2bec8e97f88bff165f9100ac696a"
    ),
    "x86_64-apple-darwin": (
        "259e2b84274434085163fe8d556510571772cda2aa6d87ca6aa664f57bc644e3"
    ),
    "x86_64-unknown-linux-gnu": (
        "dda7234360b7f578ca8b0ddcb80145646fa61a67c1720a5abc7051b35c9fcb71"
    ),
    "aarch64-unknown-linux-gnu": (
        "15f6e4ce9f583b929c996c91562bad6d4454f3281de858b02cdfdef615fac433"
    ),
    "x86_64-pc-windows-msvc": (
        "6f4bef66261261fcb43131be8720bab817d403a09edec7455c371974b90bdb7e"
    ),
    "aarch64-pc-windows-msvc": (
        "01aa49cf9574a8bd0ae52005d7de2590e8f27181ded6748236e702c92aef826d"
    ),
}

DownloadFn = Callable[[str, Path], None]
RunFn = Callable[..., subprocess.CompletedProcess[str]]


class ChecksumMismatchError(ValueError):
    """The downloaded rustup-init digest did not match the pin."""


class UnsupportedHostError(ValueError):
    """No rustup-init pin exists for this operating system and machine."""


def normalize_machine(machine: str) -> str:
    lowered = machine.lower()
    if lowered == "amd64":
        return "x86_64"
    return lowered


def rustup_target(system: str, machine: str) -> str:
    system_key = system.lower()
    machine_key = normalize_machine(machine)
    mapping = {
        ("darwin", "arm64"): "aarch64-apple-darwin",
        ("darwin", "aarch64"): "aarch64-apple-darwin",
        ("darwin", "x86_64"): "x86_64-apple-darwin",
        ("linux", "x86_64"): "x86_64-unknown-linux-gnu",
        ("linux", "aarch64"): "aarch64-unknown-linux-gnu",
        ("linux", "arm64"): "aarch64-unknown-linux-gnu",
        ("windows", "x86_64"): "x86_64-pc-windows-msvc",
        ("windows", "arm64"): "aarch64-pc-windows-msvc",
        ("windows", "aarch64"): "aarch64-pc-windows-msvc",
    }
    try:
        return mapping[(system_key, machine_key)]
    except KeyError as exc:
        raise UnsupportedHostError(
            f"unsupported rustup host: {system} {machine}"
        ) from exc


def rustup_init_filename(target: str) -> str:
    return "rustup-init.exe" if "windows" in target else "rustup-init"


def rustup_init_url(target: str) -> str:
    return f"{RUSTUP_ARCHIVE_PREFIX}{target}/{rustup_init_filename(target)}"


def expected_sha256(target: str) -> str:
    try:
        return RUSTUP_INIT_SHA256[target]
    except KeyError as exc:
        raise UnsupportedHostError(f"no rustup-init digest for {target}") from exc


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_sha256(path: Path, expected: str) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise ChecksumMismatchError(f"rustup-init is missing or empty: {path}")
    actual = file_sha256(path)
    expected_norm = expected.lower().strip()
    if len(actual) != len(expected_norm) or not hmac.compare_digest(
        actual, expected_norm
    ):
        raise ChecksumMismatchError(
            f"rustup-init checksum mismatch: expected {expected_norm}, got {actual}"
        )


def assert_archive_url(url: str) -> None:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    if (
        parsed.scheme != "https"
        or parsed.netloc.lower() != "static.rust-lang.org"
        or parsed.params
        or parsed.query
        or parsed.fragment
        or parsed.username
        or parsed.password
        or len(parts) != 5
        or parts[0] != "rustup"
        or parts[1] != "archive"
        or parts[2] != RUSTUP_VERSION
        or parts[3] not in RUSTUP_INIT_SHA256
        or parts[4] not in ("rustup-init", "rustup-init.exe")
        or ".." in parts
    ):
        raise ValueError("rustup-init URL is not the pinned rustup archive")


class ArchiveRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        assert_archive_url(newurl)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


CURL_TRUST_ENV_BLOCKLIST = (
    "CURL_CA_BUNDLE",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
)
CURL_CONNECT_TIMEOUT_S = 30
CURL_MAX_TIME_S = 120
CURL_MAX_REDIRS = 5
CURL_SUBPROCESS_TIMEOUT_S = CURL_MAX_TIME_S + 30
DIAG_PROBE_TIMEOUT_S = 5


def curl_download_env(base: dict[str, str] | None = None) -> dict[str, str]:
    """Env for curl downloads: drop Python/CA-bundle overrides so schannel wins."""
    env = dict(os.environ if base is None else base)
    for key in CURL_TRUST_ENV_BLOCKLIST:
        env.pop(key, None)
    return env


def log_trust_diagnostics(*, run_fn: RunFn = subprocess.run) -> None:
    """Emit Windows curl trust context before download (no-op off Windows)."""
    if os.name != "nt":
        return
    print(
        "rustup-trust:"
        f" python={sys.version.split()[0]}"
        f" SSL_CERT_FILE={os.environ.get('SSL_CERT_FILE', '')!r}"
        f" CURL_CA_BUNDLE={os.environ.get('CURL_CA_BUNDLE', '')!r}"
    )
    try:
        curl = resolve_windows_curl()
        probe = run_fn(
            [curl, "--version"],
            check=False,
            text=True,
            capture_output=True,
            timeout=DIAG_PROBE_TIMEOUT_S,
            env=curl_download_env(),
        )
        version_line = (probe.stdout or probe.stderr or "").splitlines()
        version = version_line[0] if version_line else f"rc={probe.returncode}"
        print(
            "rustup-trust:"
            f" download_backend=curl curl_pin=system32 path={curl!r}"
            f" version={version!r}"
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        print(f"rustup-trust: download_backend=curl probe-failed:{exc}")
    rustc = shutil.which("rustc")
    if rustc is None:
        print("rustup-trust: rustc=not-on-path")
        return
    try:
        probe = run_fn(
            [rustc, "--version"],
            check=False,
            text=True,
            capture_output=True,
            timeout=DIAG_PROBE_TIMEOUT_S,
        )
        version = (
            probe.stdout or probe.stderr or ""
        ).strip() or f"rc={probe.returncode}"
    except (OSError, subprocess.TimeoutExpired) as exc:
        version = f"probe-failed:{exc}"
    print(f"rustup-trust: rustc={rustc!r} version={version!r}")


def download_https_urllib(url: str, dest: Path) -> None:
    assert_archive_url(url)
    request = urllib.request.Request(url, method="GET")
    opener = urllib.request.build_opener(ArchiveRedirectHandler)
    with opener.open(request, timeout=60) as response:
        dest.write_bytes(response.read())


def windows_system32_curl(system_root: str | None) -> str | None:
    """Resolve System32/Sysnative curl.exe (schannel). Never PATH OpenSSL curl."""
    if not system_root:
        return None
    root = Path(system_root)
    for leaf in ("Sysnative", "System32"):
        candidate = root / leaf / "curl.exe"
        if candidate.is_file():
            return str(candidate)
    return None


def resolve_windows_curl() -> str:
    """Return System32/Sysnative curl.exe; never PATH OpenSSL curl."""
    if os.name != "nt":
        raise OSError("curl download path is Windows-only")
    system_root = os.environ.get("SystemRoot") or os.environ.get("SYSTEMROOT")
    pinned = windows_system32_curl(system_root)
    if pinned is not None:
        return pinned
    raise OSError(
        "System32 curl.exe (schannel) is required for rustup-init download; "
        f"not found under SystemRoot={system_root!r}. "
        "PATH OpenSSL curl (e.g. Git for Windows) is not used."
    )


def download_https_curl(
    url: str, dest: Path, *, run_fn: RunFn = subprocess.run
) -> None:
    """Download via System32 curl.exe with TLS verify on (Windows schannel).

    Argv never includes -k / --insecure. CA-bundle env overrides are stripped
    so corporate Python SSL_CERT_FILE cannot force a broken verify path.
    """
    assert_archive_url(url)
    if dest.exists():
        dest.unlink()
    curl = resolve_windows_curl()
    # Verify stays on: never pass -k / --insecure.
    cmd = [
        curl,
        "--fail",
        "--silent",
        "--show-error",
        "--location",
        "--max-redirs",
        str(CURL_MAX_REDIRS),
        "--proto",
        "=https",
        "--proto-redir",
        "=https",
        "--connect-timeout",
        str(CURL_CONNECT_TIMEOUT_S),
        "--max-time",
        str(CURL_MAX_TIME_S),
        "--output",
        str(dest),
        "--write-out",
        "%{http_code} %{url_effective}",
        "--",
        url,
    ]
    try:
        result = run_fn(
            cmd,
            check=False,
            text=True,
            capture_output=True,
            env=curl_download_env(),
            timeout=CURL_SUBPROCESS_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as exc:
        if dest.exists():
            dest.unlink()
        raise OSError(
            f"curl rustup-init download timed out after "
            f"{CURL_SUBPROCESS_TIMEOUT_S}s path={curl!r}"
        ) from exc
    write_out = (result.stdout or "").strip()
    if result.returncode != 0:
        if dest.exists():
            dest.unlink()
        stderr = (result.stderr or "").strip()
        raise OSError(
            f"curl rustup-init download failed path={curl!r} "
            f"rc={result.returncode} write_out={write_out!r}: {stderr}"
        )
    parts = write_out.split(None, 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        if dest.exists():
            dest.unlink()
        raise OSError(
            f"curl rustup-init download missing http_code/url_effective "
            f"path={curl!r} write_out={write_out!r}"
        )
    http_code, effective = parts[0], parts[1]
    if http_code != "200":
        if dest.exists():
            dest.unlink()
        raise OSError(
            f"curl rustup-init download unexpected HTTP {http_code} "
            f"path={curl!r} url={effective!r}"
        )
    try:
        assert_archive_url(effective)
    except ValueError:
        if dest.exists():
            dest.unlink()
        raise
    if not dest.is_file() or dest.stat().st_size == 0:
        if dest.exists():
            dest.unlink()
        raise OSError(f"curl left empty rustup-init download path={curl!r}: {dest}")


def download_https(url: str, dest: Path) -> None:
    if os.name == "nt":
        download_https_curl(url, dest)
    else:
        download_https_urllib(url, dest)


def _make_executable(path: Path) -> None:
    if os.name == "nt":
        return
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def cargo_bin_dir() -> Path:
    return Path.home() / ".cargo" / "bin"


def rustup_executable() -> Path:
    name = "rustup.exe" if os.name == "nt" else "rustup"
    return cargo_bin_dir() / name


def install_verified_rust(
    *,
    system: str,
    machine: str,
    no_modify_path: bool = False,
    dest_dir: Path | None = None,
    download_fn: DownloadFn = download_https,
    run_fn: RunFn = subprocess.run,
    log_diagnostics: bool | None = None,
) -> None:
    target = rustup_target(system, machine)
    url = rustup_init_url(target)
    expected = expected_sha256(target)
    # Default: diagnostics only on Windows (curl trust path); keep Linux/Mac quiet.
    if log_diagnostics is None:
        log_diagnostics = os.name == "nt"
    if log_diagnostics:
        log_trust_diagnostics(run_fn=run_fn)
        print(f"rustup-trust: download_url={url} target={target}")
    if dest_dir is None:
        temp_dir = tempfile.TemporaryDirectory()
        work_dir = Path(temp_dir.name)
    else:
        temp_dir = None
        work_dir = Path(dest_dir)
    installer = work_dir / rustup_init_filename(target)
    try:
        download_fn(url, installer)
        verify_sha256(installer, expected)
        _make_executable(installer)
        installer_cmd = [
            str(installer),
            "-y",
            "--default-toolchain",
            RUST_TOOLCHAIN,
        ]
        if no_modify_path:
            installer_cmd.append("--no-modify-path")
        run_fn(installer_cmd, check=True, text=True)
        rustup = rustup_executable()
        rustup_cmd = [str(rustup), "default", RUST_TOOLCHAIN]
        env = os.environ.copy()
        env["PATH"] = str(cargo_bin_dir()) + os.pathsep + env.get("PATH", "")
        run_fn(rustup_cmd, check=True, text=True, env=env)
    finally:
        try:
            if installer.exists():
                installer.unlink()
        except OSError as exc:
            print(f"rustup-trust: installer cleanup failed: {exc}", file=sys.stderr)
        if temp_dir is not None:
            temp_dir.cleanup()


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download, verify, and run a pinned rustup-init."
    )
    parser.add_argument(
        "--no-modify-path",
        action="store_true",
        help="Pass --no-modify-path through to rustup-init.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        install_verified_rust(
            system=platform.system(),
            machine=platform.machine(),
            no_modify_path=args.no_modify_path,
        )
    except (
        ChecksumMismatchError,
        UnsupportedHostError,
        OSError,
        RuntimeError,
        ValueError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
    ) as exc:
        print(exc, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
