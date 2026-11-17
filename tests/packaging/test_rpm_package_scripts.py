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

import shlex
import subprocess
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="RPM scriptlets are bash and run on Linux",
)

REPO_ROOT = Path(__file__).resolve().parents[2]
CENTOS_SCRIPTS = REPO_ROOT / "scripts" / "packaging" / "centos"
BUILD_PACKAGES = REPO_ROOT / "scripts" / "packaging" / "build_packages.sh"

# %postun shipped before the $1 == 0 guard. Upgrades from those RPMs still
# run this script after the new %post.
_RELEASED_AFTER_REMOVE = """#!/bin/bash -e
set -o pipefail

SNOWFLAKE_CLI_LINK={link}
if [[ -L "${{SNOWFLAKE_CLI_LINK}}" ]]; then
  rm -f "${{SNOWFLAKE_CLI_LINK}}"
fi
"""


def _package_invocations() -> tuple[str, str]:
    text = BUILD_PACKAGES.read_text(encoding="utf-8")
    marker = "Building rpm for version"
    assert marker in text
    deb, rpm = text.split(marker, maxsplit=1)
    return deb, rpm


def _script(tmp_path: Path, name: str, link: Path) -> Path:
    source = (CENTOS_SCRIPTS / name).read_text(encoding="utf-8")
    old = "SNOWFLAKE_CLI_LINK=/usr/local/bin/snow"
    assert old in source
    rewritten = source.replace(old, "SNOWFLAKE_CLI_LINK=" + shlex.quote(str(link)), 1)
    path = tmp_path / name
    path.write_text(rewritten, encoding="utf-8", newline="\n")
    path.chmod(0o755)
    return path


def _released_remove_script(tmp_path: Path, link: Path) -> Path:
    path = tmp_path / "after_remove_released.sh"
    path.write_text(
        _RELEASED_AFTER_REMOVE.format(link=shlex.quote(str(link))),
        encoding="utf-8",
        newline="\n",
    )
    path.chmod(0o755)
    return path


def _symlink(link: Path) -> None:
    link.symlink_to("/usr/lib/snowflake/snowflake-cli/snow")


def test_after_remove_keeps_symlink_on_upgrade(tmp_path: Path) -> None:
    link = tmp_path / "snow"
    _symlink(link)
    script = _script(tmp_path, "after_remove.sh", link)

    subprocess.run(["bash", str(script), "1"], check=True)

    assert link.is_symlink()


def test_after_remove_deletes_symlink_on_erase(tmp_path: Path) -> None:
    link = tmp_path / "snow"
    _symlink(link)
    script = _script(tmp_path, "after_remove.sh", link)

    subprocess.run(["bash", str(script), "0"], check=True)

    assert not link.exists()


def test_after_remove_without_argument_keeps_symlink(tmp_path: Path) -> None:
    link = tmp_path / "snow"
    _symlink(link)
    script = _script(tmp_path, "after_remove.sh", link)

    subprocess.run(["bash", str(script)], check=True)

    assert link.is_symlink()


def test_after_remove_leaves_regular_file_on_erase(tmp_path: Path) -> None:
    link = tmp_path / "snow"
    link.write_text("not a symlink\n", encoding="utf-8")
    script = _script(tmp_path, "after_remove.sh", link)

    subprocess.run(["bash", str(script), "0"], check=True)

    assert link.is_file()


def test_after_install_creates_symlink(tmp_path: Path) -> None:
    link = tmp_path / "snow"
    script = _script(tmp_path, "after_install.sh", link)

    subprocess.run(["bash", str(script)], check=True)

    assert link.is_symlink()
    assert link.readlink() == Path("/usr/lib/snowflake/snowflake-cli/snow")


def test_posttrans_restores_symlink_removed_by_released_postun(tmp_path: Path) -> None:
    link = tmp_path / "snow"
    install = _script(tmp_path, "after_install.sh", link)
    released_remove = _released_remove_script(tmp_path, link)

    subprocess.run(["bash", str(install)], check=True)
    assert link.is_symlink()

    subprocess.run(["bash", str(released_remove)], check=True)
    assert not link.exists()

    subprocess.run(["bash", str(install)], check=True)
    assert link.is_symlink()
    assert link.readlink() == Path("/usr/lib/snowflake/snowflake-cli/snow")


def test_rpm_package_recreates_symlink_in_posttrans() -> None:
    deb_invocation, rpm_invocation = _package_invocations()

    assert "--rpm-posttrans" in rpm_invocation
    assert "centos/after_install.sh" in rpm_invocation
    assert "--rpm-posttrans" not in deb_invocation
