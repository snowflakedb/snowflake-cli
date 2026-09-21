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

"""Helpers to regenerate CLI command pages and copy them into snowflake-prod-docs.

Mapping: ``scripts/command_docs_paths.yaml``. Only listed pages are copied.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

import yaml

DOCS_REPO_ENV = "SNOWFLAKE_PROD_DOCS"
CLI_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MAPPING = Path(__file__).resolve().parent / "command_docs_paths.yaml"
PAGES_DIR = Path("gen_docs") / "pages"


def resolve_docs_repo(
    docs_repo_flag: Path | None,
    env: Mapping[str, str],
) -> Path:
    if docs_repo_flag is not None:
        raw = str(docs_repo_flag)
    else:
        raw = env.get(DOCS_REPO_ENV, "").strip()
    if not raw:
        _die(
            "Docs repo path is required. Pass --docs-repo PATH or set "
            f"{DOCS_REPO_ENV}."
        )
    path = Path(raw).expanduser().resolve()
    if not path.is_dir():
        _die(f"Docs repo is not a directory: {path}")
    if not (path / ".git").exists():
        _die(f"Docs repo is not a git checkout: {path}")
    return path


def load_mapping(path: Path) -> list[tuple[Path, Path]]:
    if not path.is_file():
        _die(f"Mapping file not found: {path}")
    loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    if loaded is None or loaded == {}:
        _die(f"Mapping file is empty: {path}")
    if not isinstance(loaded, dict):
        _die(f"Mapping file must be a YAML object of path pairs: {path}")

    mapping: list[tuple[Path, Path]] = []
    destinations: set[Path] = set()
    for key, value in loaded.items():
        if not isinstance(key, str) or not isinstance(value, str):
            _die(f"Mapping entries must be strings: {key!r} -> {value!r}")
        if not key.strip() or not value.strip():
            _die(f"Mapping entries must be non-empty strings: {key!r} -> {value!r}")
        source = _require_relative("generated page", key)
        dest = _require_relative("docs path", value)
        if dest in destinations:
            _die(f"Duplicate docs destination: {dest}")
        destinations.add(dest)
        mapping.append((source, dest))
    return mapping


def generate_pages(cli_root: Path) -> Path:
    env = os.environ.copy()
    src = str(cli_root / "src")
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src if not existing else src + os.pathsep + existing
    command = [sys.executable, "-m", "snowflake.cli._app", "--docs-pages"]
    completed = subprocess.run(
        command,
        cwd=cli_root,
        env=env,
        check=False,
    )
    if completed.returncode != 0:
        _die(
            "Failed to generate command-reference pages "
            f"(exit {completed.returncode}): {' '.join(command)}"
        )
    pages = cli_root / PAGES_DIR
    if not pages.is_dir():
        _die(f"Expected generated pages at {pages}")
    return pages


def copy_mapped_pages(
    pages_root: Path,
    docs_repo: Path,
    mapping: Sequence[tuple[Path, Path]],
    *,
    dry_run: bool,
) -> list[Path]:
    planned: list[tuple[Path, Path, Path, Path]] = []
    for source_rel, dest_rel in mapping:
        source = pages_root / source_rel
        dest = docs_repo / dest_rel
        if not source.is_file():
            _die(f"Generated page is missing: {source}")
        if not dest.parent.is_dir():
            _die(f"Docs destination directory does not exist: {dest.parent}")
        planned.append((source_rel, dest_rel, source, dest))

    changed: list[Path] = []
    for source_rel, dest_rel, source, dest in planned:
        if dest.is_file() and dest.read_bytes() == source.read_bytes():
            continue
        print(f"{source_rel} -> {dest_rel}")
        if not dry_run:
            shutil.copyfile(source, dest)
        changed.append(dest_rel)
    return changed


def _die(message: str, code: int = 1) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def _require_relative(label: str, value: str) -> Path:
    path = Path(value)
    if path.is_absolute() or path.anchor or ".." in path.parts or path.parts == ():
        _die(f"{label} must be a relative path without '..': {value!r}")
    return path
