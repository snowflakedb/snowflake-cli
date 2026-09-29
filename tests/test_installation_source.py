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
import tarfile
from pathlib import Path

import pytest
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource


def _load_packaging_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "packaging"
        / "build_isolated_binary_with_hatch.py"
    )
    spec = importlib.util.spec_from_file_location(
        "build_isolated_binary_with_hatch", path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PYPI_ASSIGNMENT = "INSTALLATION_SOURCE = CLIInstallationSource.PYPI"


def test_git_tree_stays_pypi():
    assert __about__.INSTALLATION_SOURCE is CLIInstallationSource.PYPI
    assert CLIInstallationSource.SNOWFLAKE_MANAGED.value == "snowflake-managed"


def test_rewrite_defaults_to_binary():
    packaging = _load_packaging_module()
    rewritten = packaging.rewrite_installation_source_assignment(PYPI_ASSIGNMENT)
    assert rewritten == "INSTALLATION_SOURCE = CLIInstallationSource.BINARY"


def test_rewrite_can_stamp_snowflake_managed():
    packaging = _load_packaging_module()
    rewritten = packaging.rewrite_installation_source_assignment(
        PYPI_ASSIGNMENT, source="SNOWFLAKE_MANAGED"
    )
    assert rewritten == (
        "INSTALLATION_SOURCE = CLIInstallationSource.SNOWFLAKE_MANAGED"
    )


@pytest.mark.parametrize("source", ["PYPI", "NATIVE", "SELF_MANAGED", "native"])
def test_rewrite_rejects_unknown_stamps(source):
    packaging = _load_packaging_module()
    with pytest.raises(ValueError, match="installation source stamp"):
        packaging.rewrite_installation_source_assignment(PYPI_ASSIGNMENT, source=source)


def test_rewrite_requires_assignment():
    packaging = _load_packaging_module()
    with pytest.raises(RuntimeError, match="INSTALLATION_SOURCE"):
        packaging.rewrite_installation_source_assignment("VERSION = '1.0.0'")


def test_resolve_stamp_defaults_to_binary():
    packaging = _load_packaging_module()
    assert packaging.resolve_installation_source_stamp(env={}) == "BINARY"


def test_resolve_stamp_from_env():
    packaging = _load_packaging_module()
    assert (
        packaging.resolve_installation_source_stamp(
            env={"SNOWFLAKE_CLI_INSTALLATION_SOURCE": "SNOWFLAKE_MANAGED"}
        )
        == "SNOWFLAKE_MANAGED"
    )


def test_resolve_stamp_rejects_native():
    packaging = _load_packaging_module()
    with pytest.raises(ValueError, match="installation source stamp"):
        packaging.resolve_installation_source_stamp(
            env={"SNOWFLAKE_CLI_INSTALLATION_SOURCE": "native"}
        )


def test_env_stamp_rewrites_about_to_snowflake_managed():
    packaging = _load_packaging_module()
    about = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "snowflake"
        / "cli"
        / "__about__.py"
    ).read_text()
    source = packaging.resolve_installation_source_stamp(
        env={"SNOWFLAKE_CLI_INSTALLATION_SOURCE": "SNOWFLAKE_MANAGED"}
    )
    rewritten = packaging.rewrite_installation_source_assignment(about, source=source)
    assert "INSTALLATION_SOURCE = CLIInstallationSource.SNOWFLAKE_MANAGED" in rewritten
    assert "INSTALLATION_SOURCE = CLIInstallationSource.PYPI" not in rewritten
    binary = packaging.rewrite_installation_source_assignment(about)
    assert "INSTALLATION_SOURCE = CLIInstallationSource.BINARY" in binary


def test_should_pack_managed_tarball():
    packaging = _load_packaging_module()
    assert packaging.should_pack_managed_tarball("BINARY", env={}) is False
    assert packaging.should_pack_managed_tarball("SNOWFLAKE_MANAGED", env={}) is True
    assert (
        packaging.should_pack_managed_tarball(
            "SNOWFLAKE_MANAGED",
            env={"SNOWFLAKE_CLI_PACK_MANAGED_TARBALL": "0"},
        )
        is False
    )


@pytest.mark.parametrize(
    "system,machine,os_name,arch",
    [
        ("Linux", "x86_64", "linux", "amd64"),
        ("linux", "amd64", "linux", "amd64"),
        ("Darwin", "arm64", "darwin", "arm64"),
        ("darwin", "aarch64", "darwin", "arm64"),
        ("Windows", "AMD64", "windows", "amd64"),
    ],
)
def test_managed_platform_mapping(system, machine, os_name, arch):
    packaging = _load_packaging_module()
    assert packaging.managed_platform(system, machine) == (os_name, arch)


def test_pack_managed_tarball_unix_root_binary(tmp_path):
    packaging = _load_packaging_module()
    binary = tmp_path / "snow"
    binary.write_bytes(b"fake-snow")
    binary.chmod(0o755)
    dest = tmp_path / "dist"
    tarball, fragment_path, fragment = packaging.pack_managed_tarball(
        binary, dest, "3.13.1", "linux", "amd64"
    )
    assert tarball.name == "snowflake-cli-3.13.1-linux-amd64.tar.gz"
    assert fragment_path.name == "manifest-linux-amd64.json"
    checksum = hashlib.sha256(tarball.read_bytes()).hexdigest()
    assert fragment == {
        "packages": {
            "linux": {
                "amd64": {
                    "name": tarball.name,
                    "checksum": checksum,
                }
            }
        }
    }
    assert json.loads(fragment_path.read_text()) == fragment
    with tarfile.open(tarball, "r:gz") as tar:
        names = tar.getnames()
        assert "snow" in names
        extracted = tar.extractfile("snow")
        assert extracted is not None
        assert extracted.read() == b"fake-snow"


def test_pack_managed_tarball_windows_arcname(tmp_path):
    packaging = _load_packaging_module()
    binary = tmp_path / "snow.exe"
    binary.write_bytes(b"fake-snow-exe")
    dest = tmp_path / "dist"
    tarball, _fragment_path, fragment = packaging.pack_managed_tarball(
        binary, dest, "3.13.1", "windows", "amd64"
    )
    assert tarball.name == "snowflake-cli-3.13.1-windows-amd64.tar.gz"
    assert fragment["packages"]["windows"]["amd64"]["name"] == tarball.name
    with tarfile.open(tarball, "r:gz") as tar:
        assert "snow.exe" in tar.getnames()
        extracted = tar.extractfile("snow.exe")
        assert extracted is not None
        assert extracted.read() == b"fake-snow-exe"


def test_main_exits_when_hatch_produces_no_binary(monkeypatch):
    packaging = _load_packaging_module()
    monkeypatch.setattr(packaging, "build_isolated_binary", lambda: None)
    with pytest.raises(SystemExit) as excinfo:
        packaging.main([])
    assert excinfo.value.code == 1


def test_pack_tarball_cli(tmp_path):
    packaging = _load_packaging_module()
    binary = tmp_path / "snow"
    binary.write_bytes(b"cli-snow")
    dest = tmp_path / "out"
    packaging.main(
        [
            "--pack-tarball",
            str(binary),
            "--version",
            "3.12.0",
            "--os-name",
            "darwin",
            "--arch",
            "arm64",
            "--dest-dir",
            str(dest),
        ]
    )
    tarball = dest / "snowflake-cli-3.12.0-darwin-arm64.tar.gz"
    fragment = dest / "manifest-darwin-arm64.json"
    assert tarball.is_file()
    assert fragment.is_file()
    payload = json.loads(fragment.read_text())
    assert (
        payload["packages"]["darwin"]["arm64"]["checksum"]
        == hashlib.sha256(tarball.read_bytes()).hexdigest()
    )
