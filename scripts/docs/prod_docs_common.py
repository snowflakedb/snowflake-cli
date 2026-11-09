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
