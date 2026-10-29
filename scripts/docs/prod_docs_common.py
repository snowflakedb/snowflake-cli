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


def die(message: str, code: int = 1) -> NoReturn:
    print(message, file=sys.stderr)
    raise SystemExit(code)


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
