#!/usr/bin/env python3
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

"""Merge per-platform manifest fragments into the snowflake-managed channel.

Run after every platform job has staged ``manifest-<os>-<arch>.json`` and the
matching tarball on ``s3://sfc-eng-jenkins``. Output is what Releng
``copyArtifacts`` publishes from
``repository/snowflake-cli/<releaseType>/managed/<revision>/`` — this
script must not run inside ``ReleaseClientSnowflakeCLISelf``.

Fails before ``openssl dgst -sign`` unless each fragment ``name`` is a single
path segment ``snowflake-cli-<version>-<os>-<arch>.tar.gz`` whose bytes on
disk match ``checksum`` (optional ``sha256:`` prefix).
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

EXPECTED_PLATFORMS = frozenset(
    {
        ("linux", "amd64"),
        ("linux", "arm64"),
        ("darwin", "amd64"),
        ("darwin", "arm64"),
        ("windows", "amd64"),
    }
)
PACKAGE_NAME_RE = re.compile(
    r"^snowflake-cli-(?P<version>[A-Za-z0-9][A-Za-z0-9._-]*)"
    r"-(?P<os>linux|darwin|windows)-(?P<arch>amd64|arm64)\.tar\.gz$"
)
_SHA256_PREFIX = "sha256:"
ROLLOUT_POLICIES = ("staged", "immediate", "hold")
DEFAULT_ROLLOUT_POLICY = "staged"


def version_from_package_name(name: str, os_name: str, arch: str) -> str:
    if "/" in name or "\\" in name or Path(name).name != name:
        raise SystemExit(f"package name must be a single path segment: {name!r}")
    match = PACKAGE_NAME_RE.fullmatch(name)
    if match is None or match.group("os") != os_name or match.group("arch") != arch:
        raise SystemExit(f"unexpected package name {name!r} for {os_name}-{arch}")
    return match.group("version")


def normalize_checksum(value: str, os_name: str, arch: str) -> str:
    checksum = value.strip().lower()
    if checksum.startswith(_SHA256_PREFIX):
        checksum = checksum[len(_SHA256_PREFIX) :]
    if len(checksum) != 64 or any(c not in "0123456789abcdef" for c in checksum):
        raise SystemExit(f"invalid SHA-256 checksum for {os_name}-{arch}")
    return checksum


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def merge_fragments(fragments_dir: Path) -> tuple[dict, str]:
    merged: dict = {"packages": {}}
    versions: set[str] = set()
    fragment_paths = sorted(fragments_dir.glob("manifest-*.json"))
    if not fragment_paths:
        raise SystemExit(f"no manifest-*.json fragments in {fragments_dir}")
    for path in fragment_paths:
        fragment = json.loads(path.read_text(encoding="utf-8"))
        packages = fragment.get("packages")
        if not isinstance(packages, dict):
            raise SystemExit(f"{path.name} missing packages object")
        for os_name, arches in packages.items():
            if not isinstance(arches, dict):
                raise SystemExit(f"{path.name} has invalid arches for {os_name}")
            dest = merged["packages"].setdefault(os_name, {})
            for arch, info in arches.items():
                if not isinstance(info, dict) or not isinstance(info.get("name"), str):
                    raise SystemExit(f"missing name for {os_name}-{arch}")
                if arch in dest:
                    raise SystemExit(f"duplicate fragment for {os_name}-{arch}")
                dest[arch] = {
                    key: info[key] for key in ("name", "checksum") if key in info
                }
                versions.add(version_from_package_name(info["name"], os_name, arch))
    present = {
        (os_name, arch)
        for os_name, arches in merged["packages"].items()
        for arch in arches
    }
    missing = EXPECTED_PLATFORMS - present
    if missing:
        raise SystemExit(f"managed manifest missing platforms: {sorted(missing)}")
    extra = present - EXPECTED_PLATFORMS
    if extra:
        raise SystemExit(f"managed manifest unexpected platforms: {sorted(extra)}")
    if len(versions) != 1:
        raise SystemExit(f"fragment versions do not match: {sorted(versions)}")
    return merged, versions.pop()


def verify_package_artifacts(merged: dict, artifacts_dir: Path) -> None:
    for os_name, arches in merged["packages"].items():
        for arch, info in arches.items():
            name = info["name"]
            version_from_package_name(name, os_name, arch)
            checksum = info.get("checksum")
            if not isinstance(checksum, str) or not checksum.strip():
                raise SystemExit(f"missing checksum for {os_name}-{arch}")
            expected = normalize_checksum(checksum, os_name, arch)
            tarball = artifacts_dir / name
            if not tarball.is_file():
                raise SystemExit(f"tarball named in fragment is missing: {name}")
            actual = sha256_file(tarball)
            if not hmac.compare_digest(expected, actual):
                raise SystemExit(
                    f"checksum mismatch for {name}: fragment does not match tarball bytes"
                )


def validate_rollout_policy(policy: str) -> str:
    if policy not in ROLLOUT_POLICIES:
        allowed = ", ".join(ROLLOUT_POLICIES)
        raise SystemExit(
            f"invalid rollout policy {policy!r}; expected one of {allowed}"
        )
    return policy


def released_at_iso(now: datetime | None = None) -> str:
    current = now if now is not None else datetime.now(timezone.utc)
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    return current.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def sign_manifest(manifest_path: Path, sig_path: Path, key_path: Path) -> None:
    if not key_path.is_file():
        raise SystemExit(f"signing key is missing: {key_path}")
    subprocess.check_call(
        [
            "openssl",
            "dgst",
            "-sha256",
            "-sign",
            str(key_path),
            "-out",
            str(sig_path),
            str(manifest_path),
        ]
    )


def assemble(
    fragments_dir: Path,
    output_dir: Path,
    install_sh: Path,
    install_ps1: Path,
    sign_key: Path,
    artifacts_dir: Path | None = None,
    rollout_policy: str = DEFAULT_ROLLOUT_POLICY,
    now: datetime | None = None,
) -> str:
    policy = validate_rollout_policy(rollout_policy)
    if not install_sh.is_file() or not install_ps1.is_file():
        raise SystemExit(
            "snowflake-managed install.sh/install.ps1 missing. "
            "This train cannot assemble the managed channel."
        )
    merged, version = merge_fragments(fragments_dir)
    verify_package_artifacts(merged, artifacts_dir or fragments_dir)
    merged["rollout"] = {
        "policy": policy,
        "released_at": released_at_iso(now),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(merged, indent=2) + "\n", encoding="utf-8", newline="\n"
    )
    sign_manifest(manifest_path, output_dir / "manifest.json.sig", sign_key)
    shutil.copy2(install_sh, output_dir / "install.sh")
    shutil.copy2(install_ps1, output_dir / "install.ps1")
    (output_dir / "stable_version.txt").write_text(
        version + "\n", encoding="utf-8", newline="\n"
    )
    return version


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Assemble snowflake-managed channel artifacts for sfc-eng-jenkins."
    )
    parser.add_argument("--fragments-dir", type=Path, required=True)
    parser.add_argument(
        "--artifacts-dir",
        type=Path,
        default=None,
        help="Directory of this train's tarballs (defaults to --fragments-dir).",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--install-sh", type=Path, required=True)
    parser.add_argument("--install-ps1", type=Path, required=True)
    parser.add_argument("--sign-key", type=Path, required=True)
    parser.add_argument(
        "--rollout-policy",
        default=DEFAULT_ROLLOUT_POLICY,
        help=(
            "Auto-upgrade policy stamped into the signed manifest as "
            "rollout.policy (staged, immediate, or hold). released_at is "
            "assemble-clock UTC. The client ramps staged over 96h; Releng "
            "does not bump a fraction."
        ),
    )
    args = parser.parse_args(argv)
    version = assemble(
        args.fragments_dir,
        args.output_dir,
        args.install_sh,
        args.install_ps1,
        args.sign_key,
        args.artifacts_dir,
        rollout_policy=args.rollout_policy,
    )
    print(f"assembled snowflake-managed channel {version}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
