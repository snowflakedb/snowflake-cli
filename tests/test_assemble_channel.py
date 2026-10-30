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
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from snowflake.cli._plugins.upgrade.rollout import RolloutPolicy, parse_rollout

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


FROZEN_NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
FROZEN_RELEASED_AT = "2026-09-28T12:00:00Z"


def _openssl_verify(output: Path, key: Path, tmp_path: Path) -> None:
    manifest_bytes = (output / "manifest.json").read_bytes()
    assert b"\r\n" not in manifest_bytes
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


def test_assemble_writes_signed_channel(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    output = tmp_path / "out"
    key = _rsa_key(tmp_path)

    version = assemble.assemble(
        fragments, output, install_sh, install_ps1, key, now=FROZEN_NOW
    )

    assert version == "3.13.1"
    manifest_bytes = (output / "manifest.json").read_bytes()
    assert b"\r\n" not in manifest_bytes
    assert manifest_bytes.endswith(b"\n")
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    assert set(manifest["packages"]) == {"linux", "darwin", "windows"}
    windows = manifest["packages"]["windows"]["amd64"]
    assert windows["name"] == "snowflake-cli-3.13.1-windows-amd64.tar.gz"
    assert windows["checksum"] == hashlib.sha256(b"windows-amd64\n").hexdigest()
    assert manifest["rollout"] == {
        "policy": "staged",
        "released_at": FROZEN_RELEASED_AT,
    }
    assert "fraction" not in manifest
    assert "fraction" not in manifest["rollout"]
    assert (output / "manifest.json.sig").stat().st_size > 0
    assert (output / "install.sh").read_text(encoding="utf-8") == "#!/bin/sh\n"
    assert (output / "install.ps1").read_text(encoding="utf-8") == "Write-Host hi\n"
    stable = (output / "stable_version.txt").read_bytes()
    assert stable == b"3.13.1\n"
    _openssl_verify(output, key, tmp_path)


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


@pytest.mark.parametrize("policy", ("immediate", "hold"))
def test_assemble_stamps_explicit_rollout_policy(tmp_path, policy):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    output = tmp_path / "out"
    key = _rsa_key(tmp_path)

    assemble.assemble(
        fragments,
        output,
        install_sh,
        install_ps1,
        key,
        rollout_policy=policy,
        now=FROZEN_NOW,
    )

    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["rollout"] == {
        "policy": policy,
        "released_at": FROZEN_RELEASED_AT,
    }
    assert "fraction" not in manifest["rollout"]
    _openssl_verify(output, key, tmp_path)


@pytest.mark.parametrize("policy", ("staged", "immediate", "hold"))
def test_assembled_manifest_parse_rollout_round_trip(tmp_path, policy):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    output = tmp_path / "out"
    key = _rsa_key(tmp_path)

    assemble.assemble(
        fragments,
        output,
        install_sh,
        install_ps1,
        key,
        rollout_policy=policy,
        now=FROZEN_NOW,
    )

    manifest = json.loads((output / "manifest.json").read_bytes().decode("utf-8"))
    assert manifest["rollout"] == {
        "policy": policy,
        "released_at": FROZEN_RELEASED_AT,
    }
    rollout = parse_rollout(manifest)
    assert rollout.policy is RolloutPolicy(policy)
    if policy == "staged":
        assert rollout.released_at == FROZEN_NOW
    _openssl_verify(output, key, tmp_path)


def test_rollout_policies_match_client_enum():
    assemble = _load_assemble_module()
    assert set(assemble.ROLLOUT_POLICIES) == {member.value for member in RolloutPolicy}


def test_assemble_rejects_unknown_rollout_policy_before_sign(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    output = tmp_path / "out"
    key = _rsa_key(tmp_path)

    output.mkdir()
    (output / "manifest.json").write_text("sentinel\n", encoding="utf-8")
    (output / "manifest.json.sig").write_bytes(b"old-sig")

    with pytest.raises(SystemExit, match="invalid rollout policy"):
        assemble.assemble(
            fragments,
            output,
            install_sh,
            install_ps1,
            key,
            rollout_policy="canary",
        )

    assert (output / "manifest.json").read_text(encoding="utf-8") == "sentinel\n"
    assert (output / "manifest.json.sig").read_bytes() == b"old-sig"


def test_assemble_ignores_fragment_fraction_and_rollout(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    path = fragments / "manifest-linux-amd64.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["fraction"] = 1
    payload["rollout"] = {"policy": "immediate", "fraction": 0.5}
    payload["packages"]["linux"]["amd64"]["fraction"] = 0.5
    payload["packages"]["linux"]["amd64"]["rollout"] = {
        "policy": "immediate",
        "fraction": 0.25,
    }
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    install_sh, install_ps1 = _install_scripts(tmp_path)
    output = tmp_path / "out"
    key = _rsa_key(tmp_path)

    assemble.assemble(fragments, output, install_sh, install_ps1, key, now=FROZEN_NOW)

    signed = (output / "manifest.json").read_bytes()
    assert b"fraction" not in signed
    merged_only, _ = assemble.merge_fragments(fragments)
    assert "rollout" not in merged_only
    assert b"fraction" not in json.dumps(merged_only).encode()
    manifest = json.loads(signed.decode("utf-8"))
    linux = manifest["packages"]["linux"]["amd64"]
    assert set(linux) == {"name", "checksum"}
    assert manifest["rollout"] == {
        "policy": "staged",
        "released_at": FROZEN_RELEASED_AT,
    }
    _openssl_verify(output, key, tmp_path)


def test_main_rollout_policy_immediate(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    output = tmp_path / "out"
    key = _rsa_key(tmp_path)

    rc = assemble.main(
        [
            "--fragments-dir",
            str(fragments),
            "--output-dir",
            str(output),
            "--install-sh",
            str(install_sh),
            "--install-ps1",
            str(install_ps1),
            "--sign-key",
            str(key),
            "--rollout-policy",
            "immediate",
        ]
    )

    assert rc == 0
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["rollout"]["policy"] == "immediate"
    assert manifest["rollout"]["released_at"].endswith("Z")
    assert "fraction" not in manifest["rollout"]
    _openssl_verify(output, key, tmp_path)


def test_main_defaults_to_staged(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    output = tmp_path / "out"
    key = _rsa_key(tmp_path)

    rc = assemble.main(
        [
            "--fragments-dir",
            str(fragments),
            "--output-dir",
            str(output),
            "--install-sh",
            str(install_sh),
            "--install-ps1",
            str(install_ps1),
            "--sign-key",
            str(key),
        ]
    )

    assert rc == 0
    signed = (output / "manifest.json").read_bytes()
    manifest = json.loads(signed.decode("utf-8"))
    assert manifest["rollout"]["policy"] == "staged"
    released = datetime.strptime(
        manifest["rollout"]["released_at"], "%Y-%m-%dT%H:%M:%SZ"
    ).replace(tzinfo=timezone.utc)
    assert abs((released - datetime.now(timezone.utc)).total_seconds()) < 300
    assert "fraction" not in manifest
    _openssl_verify(output, key, tmp_path)


def test_released_at_iso_converts_offset_and_naive_to_z():
    assemble = _load_assemble_module()
    offset = timezone(timedelta(hours=2))
    assert (
        assemble.released_at_iso(datetime(2026, 9, 28, 14, 0, tzinfo=offset))
        == FROZEN_RELEASED_AT
    )
    assert assemble.released_at_iso(datetime(2026, 9, 28, 12, 0)) == FROZEN_RELEASED_AT


def test_main_rejects_bad_rollout_policy_before_sign(tmp_path):
    assemble = _load_assemble_module()
    fragments = tmp_path / "fragments"
    fragments.mkdir()
    _write_all_fragments(fragments)
    install_sh, install_ps1 = _install_scripts(tmp_path)
    output = tmp_path / "out"
    key = _rsa_key(tmp_path)

    output.mkdir()
    (output / "manifest.json").write_text("sentinel\n", encoding="utf-8")
    (output / "manifest.json.sig").write_bytes(b"old-sig")

    with pytest.raises(SystemExit, match="invalid rollout policy"):
        assemble.main(
            [
                "--fragments-dir",
                str(fragments),
                "--output-dir",
                str(output),
                "--install-sh",
                str(install_sh),
                "--install-ps1",
                str(install_ps1),
                "--sign-key",
                str(key),
                "--rollout-policy",
                "canary",
            ]
        )

    assert (output / "manifest.json").read_text(encoding="utf-8") == "sentinel\n"
    assert (output / "manifest.json.sig").read_bytes() == b"old-sig"


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
