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
import json
import subprocess
from pathlib import Path

import pytest

PLATFORMS = (
    ("linux", "amd64"),
    ("linux", "arm64"),
    ("darwin", "amd64"),
    ("darwin", "arm64"),
    ("windows", "amd64"),
)


def _load_assemble_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "packaging"
        / "snowflake-managed"
        / "assemble_channel.py"
    )
    spec = importlib.util.spec_from_file_location("assemble_channel", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_fragment(
    directory: Path,
    os_name: str,
    arch: str,
    version: str,
    *,
    checksum: str | None = None,
    name: str | None = None,
    tarball_bytes: bytes | None = None,
    write_tarball: bool = True,
) -> str:
    package_name = name or f"snowflake-cli-{version}-{os_name}-{arch}.tar.gz"
    body = (
        tarball_bytes if tarball_bytes is not None else f"{os_name}-{arch}\n".encode()
    )
    digest = hashlib.sha256(body).hexdigest()
    if write_tarball:
        (directory / Path(package_name).name).write_bytes(body)
    payload = {
        "packages": {
            os_name: {
                arch: {
                    "name": package_name,
                    "checksum": digest if checksum is None else checksum,
                }
            }
        }
    }
    (directory / f"manifest-{os_name}-{arch}.json").write_text(
        json.dumps(payload) + "\n", encoding="utf-8"
    )
    return digest


def _write_all_fragments(
    directory: Path, version: str = "3.13.1", *, checksum_prefix: bool = False
) -> None:
    for os_name, arch in PLATFORMS:
        body = f"{os_name}-{arch}\n".encode()
        checksum = (
            f"sha256:{hashlib.sha256(body).hexdigest()}" if checksum_prefix else None
        )
        _write_fragment(
            directory, os_name, arch, version, checksum=checksum, tarball_bytes=body
        )


def _rsa_key(tmp_path: Path) -> Path:
    key = tmp_path / "managed.key"
    subprocess.check_call(
        ["openssl", "genrsa", "-out", str(key), "2048"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return key


def _install_scripts(tmp_path: Path) -> tuple[Path, Path]:
    install_sh = tmp_path / "install.sh"
    install_ps1 = tmp_path / "install.ps1"
    install_sh.write_text("#!/bin/sh\n", encoding="utf-8")
    install_ps1.write_text("Write-Host hi\n", encoding="utf-8")
    return install_sh, install_ps1


def test_assemble_writes_signed_channel(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    output = tmp_path / "out"
    key = _rsa_key(tmp_path)

    version = assemble.assemble(fragments, output, install_sh, install_ps1, key)

    assert version == "3.13.1"
    manifest_bytes = (output / "manifest.json").read_bytes()
    assert b"\r\n" not in manifest_bytes
    assert manifest_bytes.endswith(b"\n")
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    assert set(manifest["packages"]) == {"linux", "darwin", "windows"}
    windows = manifest["packages"]["windows"]["amd64"]
    assert windows["name"] == "snowflake-cli-3.13.1-windows-amd64.tar.gz"
    assert windows["checksum"] == hashlib.sha256(b"windows-amd64\n").hexdigest()
    assert (output / "manifest.json.sig").stat().st_size > 0
    assert (output / "install.sh").read_text(encoding="utf-8") == "#!/bin/sh\n"
    assert (output / "install.ps1").read_text(encoding="utf-8") == "Write-Host hi\n"
    stable = (output / "stable_version.txt").read_bytes()
    assert stable == b"3.13.1\n"
    pub = tmp_path / "managed.pub"
    subprocess.check_call(
        ["openssl", "rsa", "-in", str(key), "-pubout", "-out", str(pub)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    subprocess.check_call(
        [
            "openssl",
            "dgst",
            "-sha256",
            "-verify",
            str(pub),
            "-signature",
            str(output / "manifest.json.sig"),
            str(output / "manifest.json"),
        ]
    )


def test_assemble_accepts_sha256_checksum_prefix(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments, checksum_prefix=True)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    version = assemble.assemble(
        fragments, tmp_path / "out", install_sh, install_ps1, _rsa_key(tmp_path)
    )
    assert version == "3.13.1"


def test_assemble_rejects_missing_tarball(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    for os_name, arch in PLATFORMS:
        write_tarball = (os_name, arch) != ("windows", "amd64")
        _write_fragment(fragments, os_name, arch, "3.13.1", write_tarball=write_tarball)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    with pytest.raises(SystemExit, match="tarball named in fragment is missing"):
        assemble.assemble(
            fragments, tmp_path / "out", install_sh, install_ps1, _rsa_key(tmp_path)
        )


def test_assemble_rejects_checksum_mismatch(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    for os_name, arch in PLATFORMS:
        checksum = "0" * 64 if os_name == "windows" else None
        _write_fragment(fragments, os_name, arch, "3.13.1", checksum=checksum)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    with pytest.raises(SystemExit, match="checksum mismatch"):
        assemble.assemble(
            fragments, tmp_path / "out", install_sh, install_ps1, _rsa_key(tmp_path)
        )


def test_assemble_rejects_null_package_name(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    path = fragments / "manifest-linux-amd64.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["packages"]["linux"]["amd64"]["name"] = None
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    install_sh, install_ps1 = _install_scripts(tmp_path)
    with pytest.raises(SystemExit, match="missing name"):
        assemble.assemble(
            fragments, tmp_path / "out", install_sh, install_ps1, _rsa_key(tmp_path)
        )


def test_assemble_rejects_path_in_package_name(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    for os_name, arch in PLATFORMS:
        name = (
            f"subdir/snowflake-cli-3.13.1-{os_name}-{arch}.tar.gz"
            if os_name == "linux" and arch == "amd64"
            else None
        )
        _write_fragment(fragments, os_name, arch, "3.13.1", name=name)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    with pytest.raises(SystemExit, match="single path segment"):
        assemble.assemble(
            fragments, tmp_path / "out", install_sh, install_ps1, _rsa_key(tmp_path)
        )


def test_assemble_rejects_missing_platform(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    for os_name, arch in PLATFORMS[:-1]:
        _write_fragment(fragments, os_name, arch, "3.13.1")
    install_sh, install_ps1 = _install_scripts(tmp_path)
    with pytest.raises(SystemExit, match="missing platforms"):
        assemble.assemble(
            fragments, tmp_path / "out", install_sh, install_ps1, _rsa_key(tmp_path)
        )


def test_assemble_rejects_mismatched_versions(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    for os_name, arch in PLATFORMS:
        version = "3.13.1" if os_name != "windows" else "3.13.0"
        _write_fragment(fragments, os_name, arch, version)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    with pytest.raises(SystemExit, match="versions do not match"):
        assemble.assemble(
            fragments, tmp_path / "out", install_sh, install_ps1, _rsa_key(tmp_path)
        )


def test_assemble_requires_signing_key(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    with pytest.raises(SystemExit, match="signing key is missing"):
        assemble.assemble(
            fragments,
            tmp_path / "out",
            install_sh,
            install_ps1,
            tmp_path / "no-such-key.pem",
        )
