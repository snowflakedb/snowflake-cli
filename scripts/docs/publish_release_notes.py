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

"""Publish CLI release notes into snowflake-prod-docs.

Reads ``RELEASE-NOTES.md`` from this repo and updates the year release-notes
page, the ``%snowflake-cli-version%`` variable, and the monthly summary table.

Writer-only: does not run git or open a PR. Use ``publish_prod_docs.py`` for
that.

Example::

    hatch run python scripts/docs/publish_release_notes.py \\
        --docs-repo /path/to/snowflake-prod-docs \\
        --release-date 2026-09-28
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path

from prod_docs_common import (
    _ENGLISH_MONTH_NAMES,
    die,
    format_monthly_table_date,
    format_version_heading_date,
    parse_release_date,
    render_template,
    resolve_release_version,
)
from publish_command_docs import CLI_ROOT, DOCS_REPO_ENV, resolve_docs_repo

_SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))
from release_notes_extract import (  # noqa: E402
    ParsedReleaseNotes,
    load_parsed_release_notes,
)

YEAR_NOTES_REL = Path(
    "content/en/release-notes/clients-drivers/snowflake-cli-{year}.mdx"
)
VERSION_VAR_REL = Path(
    "content/en/INCLUDE/text/client-version-vars/snowflake-cli-versions.mdx"
)
MONTHLY_REL = Path(
    "content/en/INCLUDE/release-notes/clients-drivers/monthly-details/{year}-{month:02d}.mdx"
)
MONTHLY_INDEX_REL = Path(
    "content/en/release-notes/clients-drivers/monthly-releases.mdx"
)
MONTHLY_TEMPLATE_PATH = Path(__file__).resolve().parent / "monthly_details_template.mdx"
VERSION_VAR_RE = re.compile(r"(\{/\* %snowflake-cli-version% replace:: )[\d.]+( \*/\})")
SNOWFLAKE_CLI_RN = "%snowflake-cli-rn%"
_CLI_CLIENT_CELL_RE = re.compile(
    rf"<td(?:\s+rowSpan=\{{(\d+)\}})?>\s*{re.escape(SNOWFLAKE_CLI_RN)}\s*</td>"
)
_MONTHLY_ROW_RE = re.compile(
    r"^(?P<indent>[ \t]*)<tr>\n(?P<body>(?:.*\n)*?)(?P=indent)</tr>\n?",
    re.MULTILINE,
)
MONTHLY_CONTINUATION_ROW_TEMPLATE = """{indent}<tr>
{indent}  <td>{version}</td>
{indent}  <td>{table_date}</td>
{indent}  <td></td>
{indent}</tr>
"""


def parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish CLI release notes into snowflake-prod-docs."
    )
    parser.add_argument(
        "--docs-repo",
        type=Path,
        help=f"Path to a snowflake-prod-docs git checkout. Overrides {DOCS_REPO_ENV}.",
    )
    parser.add_argument(
        "--release-date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Ship date for the release (heading, <Release>, and monthly table).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned updates without writing the docs repo.",
    )
    return parser.parse_args(argv)


def run(
    argv: Sequence[str] | None = None,
    *,
    env: Mapping[str, str] | None = None,
    cli_root: Path = CLI_ROOT,
) -> list[Path]:
    """Update prod-docs release-note files. Returns changed docs-repo-relative paths."""
    args = parse_args(argv)
    environ = env if env is not None else os.environ
    docs_repo = resolve_docs_repo(args.docs_repo, environ)
    version = resolve_release_version()
    release_date = parse_release_date(args.release_date)
    release_notes_path = cli_root / "RELEASE-NOTES.md"

    parsed = load_parsed_release_notes(release_notes_path, version)
    section = format_prod_docs_section(parsed, release_date)

    changed: list[Path] = []
    year_path = year_notes_rel(release_date.year)
    if insert_year_section(
        docs_repo / year_path, version, section, dry_run=args.dry_run
    ):
        changed.append(year_path)

    version_var_path = VERSION_VAR_REL
    if bump_version_variable(
        docs_repo / version_var_path, version, dry_run=args.dry_run
    ):
        changed.append(version_var_path)

    created = ensure_monthly_file(docs_repo, release_date, dry_run=args.dry_run)
    changed.extend(created)
    monthly_path = monthly_rel(release_date)
    if args.dry_run and monthly_path in created:
        print(
            f"monthly table -> {monthly_path.name} ({version}) (dry run: not written)"
        )
    elif (
        insert_monthly_row(
            docs_repo / monthly_path,
            version,
            release_date,
            dry_run=args.dry_run,
        )
        and monthly_path not in changed
    ):
        changed.append(monthly_path)

    if not changed:
        print("No release-note files changed.")
        return []
    if args.dry_run:
        print(f"Dry run: {len(changed)} file(s) would be updated.")
    else:
        print(f"Updated {len(changed)} release-note file(s).")
    return changed


def main() -> None:
    run()


def year_notes_rel(year: int) -> Path:
    return Path(str(YEAR_NOTES_REL).format(year=year))


def monthly_rel(release_date: date) -> Path:
    return Path(
        str(MONTHLY_REL).format(year=release_date.year, month=release_date.month)
    )


def format_prod_docs_section(parsed: ParsedReleaseNotes, release_date: date) -> str:
    version = parsed.version
    heading_date = format_version_heading_date(release_date)
    lines = [
        f"## Version {version} ({heading_date})",
        "",
        (
            f'<Release version="{version}" date="{release_date.isoformat()}" '
            'changeType="Client/Driver/Library Release" '
            'keywords="Snowflake CLI, Client Driver" />'
        ),
        "",
    ]
    for section in parsed.sections:
        if not section.bullets:
            continue
        lines.append(f"### {section.title}")
        lines.append("")
        for bullet in section.bullets:
            lines.append(f"- {bullet}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def insert_year_section(
    path: Path,
    version: str,
    section: str,
    *,
    dry_run: bool,
) -> bool:
    if not path.is_file():
        die(f"Year release-notes file does not exist: {path}")
    content = path.read_text(encoding="utf-8")
    if f"## Version {version}" in content:
        return False
    marker = "\n## Version "
    idx = content.find(marker)
    if idx < 0:
        die(f"Could not find a version section to insert before in {path}")
    updated = content[:idx] + "\n" + section + content[idx:]
    if not dry_run:
        path.write_text(updated, encoding="utf-8")
    print(f"release notes -> {path.name} (version {version})")
    return True


def bump_version_variable(path: Path, version: str, *, dry_run: bool) -> bool:
    if not path.is_file():
        die(f"Version variable file does not exist: {path}")
    content = path.read_text(encoding="utf-8")
    updated, count = VERSION_VAR_RE.subn(
        rf"\g<1>{version}\g<2>",
        content,
        count=1,
    )
    if count == 0:
        die(f"Could not find %snowflake-cli-version% in {path}")
    if updated == content:
        return False
    if not dry_run:
        path.write_text(updated, encoding="utf-8")
    print(f"version variable -> {path.name} ({version})")
    return True


def ensure_monthly_file(
    docs_repo: Path, release_date: date, *, dry_run: bool
) -> list[Path]:
    """Create the ship month's table from the in-repo template when missing.

    Returns the docs-repo-relative paths this creates (the month file and the
    monthly-releases index). Returns nothing when the month file already exists.
    """
    rel = monthly_rel(release_date)
    path = docs_repo / rel
    if path.is_file():
        return []
    index = docs_repo / MONTHLY_INDEX_REL
    if not index.is_file():
        die(f"Monthly releases index does not exist: {index}")

    new_month = render_monthly_template(release_date)
    new_index = _wire_monthly_index(
        index.read_text(encoding="utf-8"), release_date, rel
    )
    if not dry_run:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(new_month, encoding="utf-8")
        index.write_text(new_index, encoding="utf-8")
    print(f"monthly table created -> {path.name} (from {MONTHLY_TEMPLATE_PATH.name})")
    return [rel, MONTHLY_INDEX_REL]


def render_monthly_template(release_date: date) -> str:
    """Fill the in-repo monthly table template for ``release_date``'s month."""
    return render_template(
        MONTHLY_TEMPLATE_PATH,
        "Monthly details",
        {
            "__MONTH_NAME__": _ENGLISH_MONTH_NAMES[release_date.month - 1],
            "__YEAR__": str(release_date.year),
        },
    )


def _wire_monthly_index(content: str, release_date: date, rel: Path) -> str:
    """Add the month's import and <IncludeYYYYMM /> at the top of its year in the index."""
    ident = f"Include{release_date.year}{release_date.month:02d}"
    if f"import {ident} from" in content:
        return content
    import_path = rel.relative_to("content/en").as_posix()
    import_line = f"import {ident} from '{import_path}'\n"
    first_import = re.search(
        rf"^import Include{release_date.year}\d\d from .*\n", content, re.MULTILINE
    )
    if first_import is None:
        any_include = re.search(
            r"^import Include\d{6} from .*\n", content, re.MULTILINE
        )
        if any_include is None:
            die(f"No monthly Include imports in {MONTHLY_INDEX_REL}.")
        content = (
            content[: any_include.start()]
            + import_line
            + content[any_include.start() :]
        )
        year_banner = f"{{/* {release_date.year} Release Notes */}}\n\n<{ident} />\n\n"
        existing_banner = re.search(
            r"^\{/\*.*Release Notes \*/\}\s*$", content, re.MULTILINE
        )
        if existing_banner is None:
            first_tag = re.search(r"^<Include\d{6} />", content, re.MULTILINE)
            if first_tag is None:
                die(f"No monthly <Include /> tags in {MONTHLY_INDEX_REL}.")
            return (
                content[: first_tag.start()]
                + year_banner
                + content[first_tag.start() :]
            )
        return (
            content[: existing_banner.start()]
            + year_banner
            + content[existing_banner.start() :]
        )
    content = (
        content[: first_import.start()] + import_line + content[first_import.start() :]
    )
    first_tag = re.search(
        rf"^<Include{release_date.year}\d\d />", content, re.MULTILINE
    )
    if first_tag is None:
        die(f"No {release_date.year} <Include /> tags in {MONTHLY_INDEX_REL}.")
    return (
        content[: first_tag.start()] + f"<{ident} />\n\n" + content[first_tag.start() :]
    )


def insert_monthly_row(
    path: Path,
    version: str,
    release_date: date,
    *,
    dry_run: bool,
) -> bool:
    if not path.is_file():
        die(
            f"Monthly details file does not exist: {path}. "
            "Create it in snowflake-prod-docs before publishing."
        )
    content = path.read_text(encoding="utf-8")
    if _monthly_row_exists(content, version):
        return False
    table_date = format_monthly_table_date(release_date)
    updated = _update_cli_monthly_group(content, version, table_date, path)
    if updated == content:
        return False
    if not dry_run:
        path.write_text(updated, encoding="utf-8")
    print(f"monthly table -> {path.name} ({version})")
    return True


def _monthly_row_exists(content: str, version: str) -> bool:
    group = _find_cli_monthly_group(content)
    if group is None:
        return False
    version_cell = f"<td>{version}</td>"
    return any(version_cell in row for row in group)


def _find_cli_monthly_group(content: str) -> list[str] | None:
    rows = list(_MONTHLY_ROW_RE.finditer(content))
    for index, match in enumerate(rows):
        row = match.group(0)
        client_match = _CLI_CLIENT_CELL_RE.search(row)
        if not client_match:
            continue
        rowspan = int(client_match.group(1) or 1)
        group = [row]
        cursor = index + 1
        while len(group) < rowspan and cursor < len(rows):
            next_row = rows[cursor].group(0)
            if _CLI_CLIENT_CELL_RE.search(next_row):
                break
            if _count_row_cells(next_row) != 3:
                break
            group.append(next_row)
            cursor += 1
        if len(group) != rowspan:
            die(
                f"CLI monthly table group for {SNOWFLAKE_CLI_RN} is malformed "
                f"(expected {rowspan} row(s), found {len(group)})."
            )
        return group
    return None


def _count_row_cells(row: str) -> int:
    return len(re.findall(r"<td\b", row))


def _update_cli_monthly_group(
    content: str, version: str, table_date: str, path: Path
) -> str:
    group = _find_cli_monthly_group(content)
    if group is None:
        die(f"Could not find {SNOWFLAKE_CLI_RN} in {path}")
    first_row = group[0]
    if _row_has_tbd(first_row):
        updated_first = _fill_tbd_row(first_row, version, table_date)
        return content.replace(first_row, updated_first, 1)
    new_rowspan = len(group) + 1
    updated_first = _set_client_rowspan(first_row, new_rowspan)
    indent_match = _MONTHLY_ROW_RE.match(group[-1])
    indent = indent_match.group("indent") if indent_match else "    "
    continuation = MONTHLY_CONTINUATION_ROW_TEMPLATE.format(
        indent=indent,
        version=version,
        table_date=table_date,
    )
    updated = content.replace(first_row, updated_first, 1)
    anchor_row = updated_first if len(group) == 1 else group[-1]
    return updated.replace(anchor_row, anchor_row + continuation, 1)


def _row_has_tbd(row: str) -> bool:
    return "<td>TBD</td>" in row


def _fill_tbd_row(row: str, version: str, table_date: str) -> str:
    marker = "<td>TBD</td>"
    if row.count(marker) < 2:
        die(
            f"Expected TBD version/date cells in CLI monthly row for {SNOWFLAKE_CLI_RN}."
        )
    replaced = row.replace(marker, f"<td>{version}</td>", 1)
    return replaced.replace(marker, f"<td>{table_date}</td>", 1)


def _set_client_rowspan(row: str, rowspan: int) -> str:
    def _replace_client_cell(match: re.Match[str]) -> str:
        return f"<td rowSpan={{{rowspan}}}>{SNOWFLAKE_CLI_RN}</td>"

    updated, count = _CLI_CLIENT_CELL_RE.subn(_replace_client_cell, row, count=1)
    if count != 1:
        die(f"Could not update rowSpan for {SNOWFLAKE_CLI_RN} in monthly table row.")
    return updated


if __name__ == "__main__":
    main()
