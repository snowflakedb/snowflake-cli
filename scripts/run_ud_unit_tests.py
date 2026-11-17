# Copyright (c) 2026 Snowflake Inc.
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

"""Install snowflake-connector-python 5.2.0 and run the unit-test allowlist.

Hatch restores the project pins (connector 4, Snowpark, protobuf < 6) before
each command, so this script does the swap itself. Snowpark and
snowflake-ml-python still require connector 4, so they are removed first.
"""

from __future__ import annotations

import shutil
import site
import subprocess
import sys
import sysconfig
from pathlib import Path

CONNECTOR_VERSION = "5.2.0"
PROTOBUF_VERSION = "6.32.1"
REPO_ROOT = Path(__file__).resolve().parents[1]
ALLOWLIST = REPO_ROOT / "tests" / "ud_unit_paths.txt"
UNINSTALL = (
    "snowflake-snowpark-python",
    "snowflake-ml-python",
    "snowflake-connector-python",
)


def read_allowlist(text: str) -> list[str]:
    """Return pytest paths from an allowlist file."""
    paths: list[str] = []
    for line in text.splitlines():
        path = line.split("#", 1)[0].strip().rstrip("/")
        if path:
            paths.append(path)
    return paths


def validate_allowlist(paths: list[str], root: Path) -> None:
    """Reject an empty list, paths outside tests/, and paths that are not on disk."""
    if not paths:
        raise SystemExit(f"{ALLOWLIST.name} is empty")
    missing: list[str] = []
    for path in paths:
        parts = Path(path).parts
        if Path(path).is_absolute() or ".." in parts or not path.startswith("tests/"):
            raise SystemExit(f"allowlist path must stay under tests/: {path}")
        if not (root / path).exists():
            missing.append(path)
    if missing:
        raise SystemExit("allowlist paths do not exist: " + ", ".join(missing))


def pytest_command(paths: list[str]) -> list[str]:
    """Pytest invocation for the allowlist.

    pyproject addopts already drop integration, performance, e2e, spcs, and
    loaded_modules. ``--maxfail=0`` overrides the default stop-after-10.
    """
    return [
        sys.executable,
        "-m",
        "pytest",
        "--maxfail=0",
        "-n",
        "auto",
        "--dist=worksteal",
        *paths,
    ]


def _connector_dirs() -> list[Path]:
    """Connector checkouts inside this interpreter's prefix."""
    prefix = Path(sys.prefix).resolve()
    candidates = [Path(entry) for entry in site.getsitepackages()]
    candidates.append(Path(sysconfig.get_path("purelib")))
    found: list[Path] = []
    seen: set[Path] = set()
    for site_dir in candidates:
        connector = site_dir / "snowflake" / "connector"
        if not connector.is_dir():
            continue
        resolved = connector.resolve()
        if prefix not in resolved.parents or resolved in seen:
            continue
        seen.add(resolved)
        found.append(resolved)
    return found


def install_connector() -> None:
    """Replace the pinned connector 4 install with connector 5.2.0."""
    subprocess.check_call([sys.executable, "-m", "pip", "uninstall", "-y", *UNINSTALL])
    # pip leaves the namespace directory behind, and a later import can mix
    # connector 4 modules with the 5.2.0 wheel. The UD integration job removes
    # the same directory.
    for connector_dir in _connector_dirs():
        shutil.rmtree(connector_dir)
    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            f"snowflake-connector-python=={CONNECTOR_VERSION}",
            f"protobuf=={PROTOBUF_VERSION}",
        ]
    )
    subprocess.check_call(
        [
            sys.executable,
            "-c",
            "import snowflake.connector as c; "
            f"assert c.__version__ == {CONNECTOR_VERSION!r}, c.__version__",
        ]
    )


def main() -> int:
    paths = read_allowlist(ALLOWLIST.read_text(encoding="utf-8"))
    validate_allowlist(paths, REPO_ROOT)
    install_connector()
    return subprocess.call(pytest_command(paths), cwd=REPO_ROOT)


if __name__ == "__main__":
    raise SystemExit(main())
