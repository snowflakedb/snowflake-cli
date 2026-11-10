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

"""Shared helpers for prod-docs publish scripts."""

from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path
from typing import NoReturn

RELEASE_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")

# English month abbreviations used in snowflake-prod-docs (not locale-dependent).
_ENGLISH_MONTHS = (
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
)

# Full English month names used in snowflake-prod-docs (not locale-dependent).
_ENGLISH_MONTH_NAMES = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


def die(message: str, code: int = 1) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(code)


def render_template(template: Path, label: str, replacements: dict[str, str]) -> str:
    """Fill ``__PLACEHOLDER__`` keys in an in-repo template; die if any remain."""
    if not template.is_file():
        die(f"{label} template does not exist: {template}")
    rendered = template.read_text(encoding="utf-8")
    for placeholder, value in replacements.items():
        rendered = rendered.replace(placeholder, value)
    if any(placeholder in rendered for placeholder in replacements):
        die(f"{label} template still has unsubstituted placeholders.")
    return rendered


def resolve_release_version() -> str:
    """Return ``VERSION`` when it is a plain release number (not ``.dev0``)."""
    from snowflake.cli.__about__ import VERSION

    if not RELEASE_VERSION_RE.fullmatch(VERSION):
        die(
            f"VERSION is {VERSION!r}; run from the release branch or tag after the "
            "version bump, not from main with a dev suffix."
        )
    return VERSION


def parse_release_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError:
        die(f"Invalid --release-date {raw!r}; expected YYYY-MM-DD.")


def format_version_heading_date(release_date: date) -> str:
    month = _ENGLISH_MONTHS[release_date.month - 1]
    return f"{month} {release_date.day:02d}, {release_date.year}"


def format_monthly_table_date(release_date: date) -> str:
    month = _ENGLISH_MONTHS[release_date.month - 1]
    return f"{release_date.day:02d}-{month}-{release_date.year}"


COMMAND_REFERENCE_REL = Path(
    "content/en/developer-guide/snowflake-cli/command-reference"
)
CLI_NAV_TREE_REL = Path("webserver/trees/develop/snowflake-cli-v3.ts")
RELEASE_NAV_TREE_REL = Path(
    "webserver/trees/releases/clients/clients-drivers-libraries.ts"
)
CLI_YEAR_INDEX_REL = Path("content/en/release-notes/clients-drivers/snowflake-cli.mdx")
COMMAND_REF_URL = "/developer-guide/snowflake-cli/command-reference"


def command_group_dir(dest_rel: Path) -> Path | None:
    """Return the ``*-commands`` directory under command-reference, if any."""
    try:
        relative = dest_rel.relative_to(COMMAND_REFERENCE_REL)
    except ValueError:
        return None
    if len(relative.parts) < 2:
        return None
    group = COMMAND_REFERENCE_REL / relative.parts[0]
    if not group.name.endswith("-commands"):
        return None
    return group


def group_words(group_dir: Path) -> str:
    """``feedback-commands`` -> ``feedback commands``."""
    name = group_dir.name
    if name.endswith("-commands"):
        name = name[: -len("-commands")] + " commands"
    return name.replace("-", " ")


def group_title(group_dir: Path) -> str:
    """``feedback-commands`` -> ``snow feedback commands``."""
    return f"snow {group_words(group_dir)}"


def group_list_label(group_dir: Path) -> str:
    """``feedback-commands`` -> ``Feedback commands``."""
    words = group_words(group_dir)
    return words[:1].upper() + words[1:] if words else words


def nav_path_for(dest_rel: Path) -> str:
    """Docs-repo path to the nav ``path`` value (no ``content/en``, no suffix)."""
    return dest_rel.relative_to("content/en").with_suffix("").as_posix()


def nav_page_label(source: Path) -> str:
    """Nav label from the ``snow --docs`` source path.

    Joins path parts with spaces and keeps hyphens, so
    ``spcs/compute-pool/stop-all.mdx`` is ``spcs compute-pool stop-all``.
    """
    return " ".join(source.with_suffix("").parts)


def read_nav_tree(docs_repo: Path, tree_rel: Path) -> str | None:
    """Return the nav tree text, or None (after printing) when the file is missing."""
    tree = docs_repo / tree_rel
    if not tree.is_file():
        print(f"nav insert skipped: {tree_rel} does not exist")
        return None
    return tree.read_text(encoding="utf-8")


_PATH_FIELD_RE = re.compile(r"path:\s*'([^']+)'")
_COMMAND_REF_PREFIX = "developer-guide/snowflake-cli/command-reference/"
_GROUP_OVERVIEW_PATH_RE = re.compile(
    rf"^{re.escape(_COMMAND_REF_PREFIX)}([^/]+-commands)/overview$"
)


def nav_has_path(content: str, path: str) -> bool:
    return f"path: '{path}'" in content or f'path: "{path}"' in content


def nav_parent_for_page(dest_rel: Path, content: str) -> str | None:
    """Deepest ``…/overview`` node already in the tree under ``dest_rel``'s dirs.

    Does not fall back to ``command-reference/overview``. Missing ancestors are
    the caller's skip, not a hard failure.
    """
    try:
        relative = dest_rel.relative_to(COMMAND_REFERENCE_REL)
    except ValueError:
        return None
    dirs = relative.parts[:-1]
    for i in range(len(dirs), 0, -1):
        overview = nav_path_for(
            COMMAND_REFERENCE_REL.joinpath(*dirs[:i]) / "overview.mdx"
        )
        if nav_has_path(content, overview):
            return overview
    return None


def insert_nav_child(
    content: str,
    *,
    parent_path: str,
    path: str,
    label: str,
    translated: bool,
    children: str | None = None,
    place: str = "sorted",
) -> str:
    """Insert a PAGE node into ``parent_path``'s children. No-op if ``path`` exists.

    If the parent node or its ``children`` array is missing, print a skip message
    and return ``content`` unchanged so publish can continue.
    """
    if nav_has_path(content, path):
        return content
    array = _children_array_span(content, parent_path)
    if array is None:
        print(
            f"nav insert skipped: no children array for parent {parent_path!r} "
            f"(page {path!r})"
        )
        return content
    start, end, inner_indent = array
    inner = content[start:end]
    node = _format_nav_node(
        path=path,
        label=label,
        translated=translated,
        indent=inner_indent,
        children=children,
    )
    insert_at = 0 if place == "start" else _nav_insert_offset(inner, path)
    insert_at = _nav_line_insert_at(inner, insert_at)
    prefix = content[: start + insert_at]
    if prefix and not prefix.endswith("\n"):
        node = "\n" + node
    suffix = content[start + insert_at :]
    if not inner.strip() and suffix.startswith("]"):
        closer = inner_indent[:-2] if len(inner_indent) >= 2 else inner_indent
        suffix = closer + suffix
    return prefix + node + suffix


def _children_array_span(content: str, parent_path: str) -> tuple[int, int, str] | None:
    needle = f"path: '{parent_path}'"
    path_at = content.find(needle)
    if path_at < 0:
        needle = f'path: "{parent_path}"'
        path_at = content.find(needle)
    if path_at < 0:
        return None
    children_at = content.find("children: [", path_at)
    if children_at < 0:
        return None
    open_at = content.find("[", children_at)
    close_at = _matching_bracket(content, open_at)
    if close_at < 0:
        return None
    inner_start = open_at + 1
    inner_end = close_at
    inner = content[inner_start:inner_end]
    indent_match = re.search(r"\n([ \t]+)\{", inner)
    if indent_match:
        indent = indent_match.group(1)
    else:
        line_start = content.rfind("\n", 0, open_at) + 1
        line = content[line_start:open_at]
        leading = re.match(r"[ \t]*", line)
        indent = (leading.group(0) if leading else "") + "  "
    return inner_start, inner_end, indent


def _matching_bracket(content: str, open_at: int) -> int:
    depth = 0
    i = open_at
    while i < len(content):
        ch = content[i]
        if ch in "'\"":
            i += 1
            while i < len(content) and content[i] != ch:
                if content[i] == "\\":
                    i += 1
                i += 1
        elif ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return -1


def _group_overview_dir(path: str) -> str | None:
    """``…/feedback-commands/overview`` -> ``feedback-commands``; else ``None``."""
    match = _GROUP_OVERVIEW_PATH_RE.fullmatch(path)
    return None if match is None else match.group(1)


def _nav_insert_offset(inner: str, new_path: str) -> int:
    """Offset in ``inner`` to insert a node among like siblings.

    A new ``*-commands/overview`` under command-reference is placed among
    existing group-overview siblings by group folder name. ``snow`` and other
    non-overview leaves are skipped so they stay where they are.
    Other inserts still sort by full ``path``.
    """
    new_group = _group_overview_dir(new_path)
    offset = 0
    depth = 0
    i = 0
    child_start = None
    while i < len(inner):
        ch = inner[i]
        if ch == "{":
            if depth == 0:
                child_start = i
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0 and child_start is not None:
                child = inner[child_start : i + 1]
                match = _PATH_FIELD_RE.search(child)
                sibling = match.group(1) if match else None
                if sibling is not None and _should_insert_before(
                    new_path, new_group, sibling
                ):
                    return _line_start(inner, child_start)
                offset = i + 1
                if offset < len(inner) and inner[offset] == ",":
                    offset += 1
                child_start = None
        i += 1
    return offset if offset > 0 else len(inner.rstrip(" \t"))


def _should_insert_before(new_path: str, new_group: str | None, sibling: str) -> bool:
    if new_group is not None:
        sibling_group = _group_overview_dir(sibling)
        if sibling_group is None:
            return False
        return sibling_group > new_group
    return sibling > new_path


def _line_start(text: str, pos: int) -> int:
    newline = text.rfind("\n", 0, pos)
    return 0 if newline < 0 else newline + 1


def _nav_line_insert_at(inner: str, insert_at: int) -> int:
    """Insert at the start of a sibling line, skipping a leading newline in the array."""
    if insert_at <= 0:
        return 1 if inner.startswith("\n") else 0
    if insert_at < len(inner) and inner[insert_at] == "{":
        return _line_start(inner, insert_at)
    if insert_at < len(inner) and inner[insert_at] == "\n":
        return insert_at + 1
    return insert_at


def _format_nav_node(
    *,
    path: str,
    label: str,
    translated: bool,
    indent: str,
    children: str | None,
) -> str:
    label_src = f"t('{label}')" if translated else f"'{label}'"
    inner = indent + "  "
    lines = [
        f"{indent}{{",
        f"{inner}label: {label_src},",
        f"{inner}type: TreeNodeType.PAGE,",
        f"{inner}path: '{path}',",
    ]
    if children is not None:
        if children.strip():
            lines.append(f"{inner}children: [")
            lines.append(children.rstrip("\n"))
            lines.append(f"{inner}],")
        else:
            lines.append(f"{inner}children: [],")
    lines.append(f"{indent}}},")
    return "\n".join(lines) + "\n"
