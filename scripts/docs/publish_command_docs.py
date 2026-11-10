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

Release helper: run ``snow --docs`` in this repo and copy mapped pages
into a local snowflake-prod-docs checkout.

Writer-only: does not run git or open a PR. Use ``publish_prod_docs.py`` for
that.

Docs repo path: ``--docs-repo`` or the ``SNOWFLAKE_PROD_DOCS`` environment
variable (flag wins).

Mapping: ``scripts/docs/command_docs_paths.yaml``. Only listed pages are copied.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

import yaml
from prod_docs_common import (
    CLI_NAV_TREE_REL,
    COMMAND_REF_URL,
    COMMAND_REFERENCE_REL,
    command_group_dir,
    group_list_label,
    group_title,
    insert_nav_child,
    nav_has_path,
    nav_page_label,
    nav_parent_for_page,
    nav_path_for,
    read_nav_tree,
    render_template,
)

DOCS_REPO_ENV = "SNOWFLAKE_PROD_DOCS"
CLI_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MAPPING = Path(__file__).resolve().parent / "command_docs_paths.yaml"
GROUP_OVERVIEW_TEMPLATE_PATH = (
    Path(__file__).resolve().parent / "group_overview_template.mdx"
)
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
) -> list[Path]:
    """Copy mapped command-reference pages into snowflake-prod-docs."""
    if generate is None:
        generate = generate_pages
    args = parse_args(argv)
    environ = env if env is not None else os.environ

    docs_repo = resolve_docs_repo(args.docs_repo, environ)
    mapping = load_mapping(args.mapping)
    pages_root = generate(cli_root)
    dests = [dest for _, dest in mapping]
    changed = copy_mapped_pages(pages_root, docs_repo, mapping, dry_run=args.dry_run)
    overview_changed, new_overviews = ensure_group_overviews(
        docs_repo, dests, dry_run=args.dry_run
    )
    for path in overview_changed:
        if path not in changed:
            changed.append(path)
    written = [path for path in changed if path.suffix == ".mdx"]
    source_by_dest = {dest: source for source, dest in mapping}
    for path in ensure_command_nav(
        docs_repo,
        [*new_overviews, *dests],
        dry_run=args.dry_run,
        sources=source_by_dest,
        required=written,
    ):
        if path not in changed:
            changed.append(path)
    if not changed:
        print("No command-reference pages changed.")
        return []
    if args.dry_run:
        print(f"Dry run: {len(changed)} file(s) would be updated.")
    else:
        print(f"Copied {len(changed)} file(s).")
    return changed


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


def command_page_rel_path(command_path: Sequence[str]) -> Path:
    """Relative path under ``gen_docs/pages`` for a terminal command."""
    return Path(*command_path).with_suffix(".mdx")


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
    command = [sys.executable, "-m", "snowflake.cli._app", "--docs"]
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
        planned.append((source_rel, dest_rel, source, dest))

    changed: list[Path] = []
    for source_rel, dest_rel, source, dest in planned:
        if dest.is_file() and dest.read_bytes() == source.read_bytes():
            continue
        print(f"{source_rel} -> {dest_rel}")
        if not dry_run:
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, dest)
        changed.append(dest_rel)
    return changed


_TOP_OVERVIEW_BLOCK = """\
- {label}
  - []({command_ref}/{group}/overview)
"""

_TOP_OVERVIEW_LINK_RE = re.compile(
    rf"- \[\]\({re.escape(COMMAND_REF_URL)}/([^/]+)/overview\)"
)


def ensure_group_overviews(
    docs_repo: Path,
    dests: Sequence[Path],
    *,
    dry_run: bool,
) -> tuple[list[Path], list[Path]]:
    """Create missing group overview.mdx files. Existing overviews are not edited.

    Returns ``(changed, created)``. Both lists are the overviews (and top-index
    bullets) written for groups that had no overview file.
    """
    changed: list[Path] = []
    created: list[Path] = []
    by_group: dict[Path, list[Path]] = {}
    for dest_rel in dests:
        group = command_group_dir(dest_rel)
        if group is None:
            continue
        by_group.setdefault(group, []).append(dest_rel)
    for group, group_dests in by_group.items():
        overview_rel = group / "overview.mdx"
        if not _create_group_overview(
            docs_repo / overview_rel,
            group,
            group_dests,
            dry_run=dry_run,
        ):
            continue
        created.append(overview_rel)
        changed.append(overview_rel)
        top_rel = COMMAND_REFERENCE_REL / "overview.mdx"
        if _ensure_top_overview_bullet(docs_repo / top_rel, group, dry_run=dry_run):
            if top_rel not in changed:
                changed.append(top_rel)
    return changed, created


def _create_group_overview(
    path: Path, group: Path, dests: Sequence[Path], *, dry_run: bool
) -> bool:
    if path.is_file():
        return False
    body = render_group_overview_template(
        title=group_title(group), bullets=_group_command_bullets(dests)
    )
    if not dry_run:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(body, encoding="utf-8")
    print(
        f"group overview created -> {group.name}/overview.mdx "
        f"(from {GROUP_OVERVIEW_TEMPLATE_PATH.name})"
    )
    return True


def render_group_overview_template(*, title: str, bullets: str) -> str:
    """Fill the in-repo group overview template for ``title`` and ``bullets``."""
    return render_template(
        GROUP_OVERVIEW_TEMPLATE_PATH,
        "Group overview",
        {"__TITLE__": title, "__BULLETS__": bullets},
    )


def _group_command_bullets(dests: Sequence[Path]) -> str:
    lines: list[str] = []
    for dest in sorted(
        {d for d in dests if d.name != "overview.mdx"}, key=lambda p: p.as_posix()
    ):
        url = "/" + dest.relative_to("content/en").with_suffix("").as_posix()
        lines.append(f"- []({url})")
    return "\n".join(lines)


def _ensure_top_overview_bullet(path: Path, group: Path, *, dry_run: bool) -> bool:
    if not path.is_file():
        return False
    content = path.read_text(encoding="utf-8")
    if f"{COMMAND_REF_URL}/{group.name}/" in content:
        return False
    block = _TOP_OVERVIEW_BLOCK.format(
        label=group_list_label(group),
        command_ref=COMMAND_REF_URL,
        group=group.name,
    )
    updated = _insert_top_overview_block(content, group.name, block)
    if updated == content:
        return False
    if not dry_run:
        path.write_text(updated, encoding="utf-8")
    print(f"command-reference overview -> + {group.name}")
    return True


def _insert_top_overview_block(content: str, group_name: str, block: str) -> str:
    block = block.rstrip() + "\n"
    last = None
    for match in _TOP_OVERVIEW_LINK_RE.finditer(content):
        if match.group(1) > group_name:
            # Insert before this group's bullet (the "- Label" line two lines up).
            line_start = content.rfind("\n", 0, match.start())
            bullet_start = content.rfind("\n", 0, line_start) + 1
            return content[:bullet_start] + block + "\n" + content[bullet_start:]
        last = match
    if last is None:
        return content.rstrip() + "\n\n" + block
    after = content.find("\n", last.end())
    if after < 0:
        return content.rstrip() + "\n\n" + block
    after += 1
    if after < len(content) and content[after] == "\n":
        after += 1
    return content[:after] + "\n" + block + content[after:]


_COMMAND_REF_NAV_PARENT = "developer-guide/snowflake-cli/command-reference/overview"


def ensure_command_nav(
    docs_repo: Path,
    dests: Sequence[Path],
    *,
    dry_run: bool,
    sources: Mapping[Path, Path] | None = None,
    required: Sequence[Path] = (),
) -> list[Path]:
    """Insert PAGE nodes for dests whose path is not already in snowflake-cli-v3.ts.

    Does not edit snowflake-cli.ts. Skips (prints) when the tree file or a parent
    node is missing. ``required`` dests (pages written this run) must have a node
    afterwards or publish dies. Command-page labels come from ``sources``
    (dest → generated page path) when that dest is mapped.
    """
    tree = docs_repo / CLI_NAV_TREE_REL
    content = read_nav_tree(docs_repo, CLI_NAV_TREE_REL)
    original = content
    if not dests:
        _require_nav_nodes(content, required)
        return []
    if content is None:
        _require_nav_nodes(content, required)
        return []
    overviews: list[tuple[Path, Path]] = []
    pages: list[tuple[Path, Path | None]] = []
    for dest_rel in dests:
        if dest_rel.suffix != ".mdx":
            continue
        group = command_group_dir(dest_rel)
        if group is not None and dest_rel.name == "overview.mdx":
            overviews.append((dest_rel, group))
        else:
            pages.append((dest_rel, group))
    for dest_rel, group in overviews:
        content = insert_nav_child(
            content,
            parent_path=_COMMAND_REF_NAV_PARENT,
            path=nav_path_for(dest_rel),
            label=group_list_label(group),
            translated=True,
            children="\n",
        )

    def _page_label(dest_rel: Path) -> str:
        source = None if sources is None else sources.get(dest_rel)
        if source is not None:
            return nav_page_label(source)
        return dest_rel.stem

    for dest_rel, group in pages:
        page_path = nav_path_for(dest_rel)
        if group is None:
            content = insert_nav_child(
                content,
                parent_path=_COMMAND_REF_NAV_PARENT,
                path=page_path,
                label=_page_label(dest_rel),
                translated=True,
            )
            continue
        parent = nav_parent_for_page(dest_rel, content)
        if parent is None:
            print(f"nav insert skipped: no overview ancestor for {page_path!r}")
            continue
        content = insert_nav_child(
            content,
            parent_path=parent,
            path=page_path,
            label=_page_label(dest_rel),
            translated=False,
        )
    changed: list[Path] = []
    if content != original:
        if not dry_run:
            tree.write_text(content, encoding="utf-8")
        print(f"cli nav -> {CLI_NAV_TREE_REL}")
        changed = [CLI_NAV_TREE_REL]
    _require_nav_nodes(content, required)
    return changed


def _require_nav_nodes(content: str | None, required: Sequence[Path]) -> None:
    missing = [
        dest
        for dest in required
        if dest.suffix == ".mdx"
        and (content is None or not nav_has_path(content, nav_path_for(dest)))
    ]
    if missing:
        paths = ", ".join(nav_path_for(dest) for dest in missing)
        _die(f"CLI nav is missing nodes for pages written this run: {paths}")


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
