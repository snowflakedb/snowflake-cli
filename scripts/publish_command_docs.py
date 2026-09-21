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

"""Regenerate CLI command pages and copy them into snowflake-prod-docs.

Release helper: run ``snow --docs-pages`` in this repo and copy mapped pages
into a local snowflake-prod-docs checkout.

Docs repo path: ``--docs-repo`` or the ``SNOWFLAKE_PROD_DOCS`` environment
variable (flag wins).

Mapping: ``scripts/command_docs_paths.yaml``. Only listed pages are copied.
"""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

import yaml

DOCS_REPO_ENV = "SNOWFLAKE_PROD_DOCS"
CLI_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MAPPING = Path(__file__).resolve().parent / "command_docs_paths.yaml"
PAGES_DIR = Path("gen_docs") / "pages"

GeneratePages = Callable[[Path], Path]


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Regenerate command-reference MDX and copy mapped pages into "
            "a local snowflake-prod-docs checkout."
        )
    )
    parser.add_argument(
        "--docs-repo",
        type=Path,
        help=f"Path to a snowflake-prod-docs git checkout. Overrides {DOCS_REPO_ENV}.",
    )
    parser.add_argument(
        "--mapping",
        type=Path,
        default=DEFAULT_MAPPING,
        help="YAML mapping of generated pages to docs-repo paths.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and print planned copies without writing the docs repo.",
    )
    return parser.parse_args(argv)


def run(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    generate: GeneratePages | None = None,
    cli_root: Path = CLI_ROOT,
) -> None:
    """Copy mapped command-reference pages into snowflake-prod-docs."""
    if generate is None:
        generate = generate_pages
    args = parse_args(argv)
    environ = env if env is not None else os.environ

    docs_repo = resolve_docs_repo(args.docs_repo, environ)
    mapping = load_mapping(args.mapping)
    pages_root = generate(cli_root)
    changed = copy_mapped_pages(pages_root, docs_repo, mapping, dry_run=args.dry_run)
    if not changed:
        print("No command-reference pages changed.")
        return
    if args.dry_run:
        print(f"Dry run: {len(changed)} file(s) would be updated.")
        return
    print(f"Copied {len(changed)} file(s).")


def main() -> None:
    run()


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


if __name__ == "__main__":
    main()
