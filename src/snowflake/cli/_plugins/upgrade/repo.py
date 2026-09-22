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
import logging
import os
import platform
import tarfile
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
from snowflake.cli._plugins.upgrade.layout import ManagedLayout, validate_version
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)

DEFAULT_REPO_BASE = "https://sfc-repo.snowflakecomputing.com/snowflake-cli"
REPO_BASE_ENV = "SNOWFLAKE_CLI_MANAGED_REPO"
ALLOWED_REPO_HOST = urlparse(DEFAULT_REPO_BASE).hostname
POINTER_NAME = "stable_version.txt"
MANIFEST_NAME = "manifest.json"

POINTER_TIMEOUT_SECONDS = 30
DOWNLOAD_TIMEOUT_SECONDS = 300
DOWNLOAD_CHUNK_SIZE = 64 * 1024


def repo_os(*, system: Optional[str] = None) -> str:
    name = system if system is not None else platform.system()
    mapping = {"Darwin": "darwin", "Linux": "linux", "Windows": "windows"}
    mapped = mapping.get(name)
    if mapped is None:
        raise CliError(f"snow upgrade does not support this operating system ({name}).")
    return mapped


def repo_arch(*, machine: Optional[str] = None) -> str:
    name = (machine if machine is not None else platform.machine()).lower()
    if name in {"x86_64", "amd64"}:
        return "amd64"
    if name in {"aarch64", "arm64"}:
        return "arm64"
    raise CliError(f"snow upgrade does not support this architecture ({name}).")


def default_repo_base() -> str:
    override = os.environ.get(REPO_BASE_ENV)
    if override is None or override.strip() == "":
        return DEFAULT_REPO_BASE
    base = _validated_repo_base(override)
    log.warning(
        "%s overrides the snowflake-managed repo base to %s",
        REPO_BASE_ENV,
        base,
    )
    return base


def _validated_repo_base(value: str) -> str:
    """Pin env overrides to HTTPS sfc-repo. Tests inject via HttpRepo(base_url=)."""
    base = value.strip().rstrip("/")
    parsed = urlparse(base)
    host = (parsed.hostname or "").lower()
    if (
        parsed.scheme != "https"
        or host != ALLOWED_REPO_HOST
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port not in {None, 443}
    ):
        raise CliError(
            f"{REPO_BASE_ENV} must be an https URL on {ALLOWED_REPO_HOST} "
            f"(got {value!r})."
        )
    return base


class HttpRepo:
    """sfc-repo client: pointer, manifest, tarball, SHA-256, extract."""

    def __init__(self, base_url: Optional[str] = None) -> None:
        self.base_url = (base_url or default_repo_base()).rstrip("/")

    def latest_version(self) -> str:
        url = f"{self.base_url}/{POINTER_NAME}"
        text = _http_get_text(url, timeout=POINTER_TIMEOUT_SECONDS)
        version = text.strip()
        if not version:
            raise CliError(f"{POINTER_NAME} from {self.base_url} is empty.")
        return validate_version(version)

    def materialize(self, version: str, layout: ManagedLayout) -> Path:
        """Download, verify, and extract ``version`` into a new version directory.

        Never writes into the currently running version directory.
        """
        version = validate_version(version)
        os_name = repo_os()
        arch = repo_arch()
        with SecurePath.temporary_directory() as tmp:
            tarball = tmp.path / "package.tar.gz"
            self._download_verified_tarball(version, os_name, arch, tarball)
            extract_dir = tmp.path / "extract"
            SecurePath(extract_dir).mkdir()
            _safe_extract(tarball, extract_dir)
            payload = _unwrap_payload(extract_dir, layout.binary_name())
            return _place_version(layout, version, payload)

    def _download_verified_tarball(
        self, version: str, os_name: str, arch: str, dest: Path
    ) -> None:
        package = self._package_for(version, os_name, arch)
        name = package["name"]
        expected = package["checksum"]
        url = f"{self.base_url}/{version}/{name}"
        digest = _download_file(url, dest)
        if digest != expected:
            dest.unlink(missing_ok=True)
            raise CliError(
                f"Checksum mismatch for {name}: expected {expected}, got {digest}. "
                "Refusing to install."
            )
        log.info("Verified SHA-256 for %s", name)

    def _package_for(self, version: str, os_name: str, arch: str) -> dict:
        url = f"{self.base_url}/{version}/{MANIFEST_NAME}"
        try:
            manifest = json.loads(_http_get_text(url, timeout=POINTER_TIMEOUT_SECONDS))
        except json.JSONDecodeError as exc:
            raise CliError(f"Invalid {MANIFEST_NAME} at {url}.") from exc
        return _package_entry(manifest, os_name, arch)


def _package_entry(manifest: object, os_name: str, arch: str) -> dict:
    if not isinstance(manifest, dict):
        raise CliError(f"{MANIFEST_NAME} must be a JSON object.")
    packages = manifest.get("packages")
    if not isinstance(packages, dict):
        raise CliError(f"{MANIFEST_NAME} is missing packages.")
    os_entry = packages.get(os_name)
    if not isinstance(os_entry, dict):
        raise CliError(f"{MANIFEST_NAME} has no package for {os_name}/{arch}.")
    arch_entry = os_entry.get(arch)
    if not isinstance(arch_entry, dict):
        raise CliError(f"{MANIFEST_NAME} has no package for {os_name}/{arch}.")
    name = arch_entry.get("name")
    checksum = arch_entry.get("checksum")
    if not name or not checksum:
        raise CliError(
            f"{MANIFEST_NAME} package for {os_name}/{arch} is missing name or checksum."
        )
    filename = str(name)
    if (
        "/" in filename
        or "\\" in filename
        or filename in {".", ".."}
        or ".." in Path(filename).parts
    ):
        raise CliError(f"Invalid package name in {MANIFEST_NAME}: {filename!r}.")
    return {"name": filename, "checksum": _normalize_checksum(str(checksum))}


def _normalize_checksum(value: str) -> str:
    checksum = value.strip().lower()
    prefix = "sha256:"
    if checksum.startswith(prefix):
        checksum = checksum[len(prefix) :]
    if len(checksum) != 64 or any(c not in "0123456789abcdef" for c in checksum):
        raise CliError(f"Invalid SHA-256 checksum in {MANIFEST_NAME}.")
    return checksum


def _http_get_text(url: str, timeout: float) -> str:
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise CliError(f"Failed to fetch {url}: {exc}") from exc
    return response.text


def _download_file(url: str, dest: Path) -> str:
    """Stream ``url`` to ``dest``. Fail closed on a truncated body. Return SHA-256."""
    hasher = hashlib.sha256()
    received = 0
    try:
        with requests.get(
            url, timeout=DOWNLOAD_TIMEOUT_SECONDS, stream=True
        ) as response:
            response.raise_for_status()
            expected = _content_length(response)
            dest_secure = SecurePath(dest)
            dest_secure.parent.mkdir(parents=True, exist_ok=True)
            with dest_secure.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    hasher.update(chunk)
                    received += len(chunk)
    except requests.RequestException as exc:
        dest.unlink(missing_ok=True)
        raise CliError(f"Failed to download {url}: {exc}") from exc
    except Exception:
        dest.unlink(missing_ok=True)
        raise

    if expected is not None and received != expected:
        dest.unlink(missing_ok=True)
        raise CliError(
            f"Download of {url} was truncated ({received} of {expected} bytes)."
        )
    return hasher.hexdigest()


def _content_length(response: requests.Response) -> Optional[int]:
    raw = response.headers.get("Content-Length")
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise CliError(f"Invalid Content-Length from {response.url}.") from exc


def _assert_safe_member(member: tarfile.TarInfo, dest: Path) -> None:
    if member.issym() or member.islnk():
        raise CliError("Refusing tarball with symbolic or hard links.")
    if not (member.isfile() or member.isdir()):
        raise CliError(f"Refusing tarball member of type {member.type!r}.")
    target = (dest / member.name).resolve()
    dest_resolved = dest.resolve()
    if not target.is_relative_to(dest_resolved):
        raise CliError(f"Tarball path escapes extract directory: {member.name!r}.")


def _safe_extract(tarball: Path, dest: Path) -> None:
    try:
        with tarfile.open(tarball, "r:gz") as tar:
            for member in tar.getmembers():
                _assert_safe_member(member, dest)
                if hasattr(tarfile, "data_filter"):
                    tar.extract(member, path=dest, filter="data")
                else:
                    tar.extract(member, path=dest)
    except CliError:
        raise
    except tarfile.TarError as exc:
        raise CliError(f"Failed to extract snowflake-managed tarball: {exc}") from exc


def _unwrap_payload(extract_dir: Path, binary_name: str) -> Path:
    direct = extract_dir / binary_name
    if direct.is_file():
        return extract_dir
    children = [path for path in extract_dir.iterdir() if path.name not in {".", ".."}]
    if len(children) == 1 and children[0].is_dir():
        nested = children[0] / binary_name
        if nested.is_file():
            return children[0]
    raise CliError(
        f"Tarball does not contain {binary_name} at the root or in a single top-level directory."
    )


def _place_version(layout: ManagedLayout, version: str, payload: Path) -> Path:
    dest = layout.version_dir(version)
    binary_name = layout.binary_name()
    source_binary = payload / binary_name
    if not source_binary.is_file():
        raise CliError(f"Tarball does not contain {binary_name}.")

    current_target = layout.current_shim_target()
    if (
        current_target is not None
        and dest.exists()
        and dest.resolve() == current_target.resolve().parent
    ):
        raise CliError(
            f"Refusing to overwrite the running snowflake-managed binary at {current_target}. "
            "Install into a new version directory, then retarget the shim."
        )

    if dest.exists() or dest.is_symlink():
        SecurePath(dest).rmdir(recursive=True)
    SecurePath(dest.parent).mkdir(parents=True, exist_ok=True)
    SecurePath(payload).copy(dest)
    installed = dest / binary_name
    if platform.system() != "Windows":
        installed.chmod(0o755)
    log.info("Installed snowflake-managed version %s at %s", version, installed)
    return installed
