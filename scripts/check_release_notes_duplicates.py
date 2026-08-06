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

"""Fail if RELEASE-NOTES.md has a bullet duplicated across sections.

Companion to the `RELEASE-NOTES.md merge=union` git attribute. Union merge
auto-resolves the "two PRs each append a bullet" conflict but silently keeps
both copies of a bullet in scenarios that produce an invalid file:

* The release cut moved a bullet from "Unreleased version" into a `# vX.Y.Z`
  section, but the author's branch still has the bullet under Unreleased.
* A rebase keyed on adjacent bullets as patch context dropped the author's
  new bullet under the most-recent released section instead of Unreleased,
  carrying along context bullets that are also present in an older released
  section.

The script flags both cases so the author can remove the misplaced bullet.
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
RELEASE_NOTES = REPO_ROOT / "RELEASE-NOTES.md"

_TOP_HEADER_RE = re.compile(r"^#\s+(.+?)\s*$")
_BULLET_RE = re.compile(r"^\s*[*-]\s+(.*?)\s*$")
_UNRELEASED_SECTION = "Unreleased version"
_RELEASED_SECTION_RE = re.compile(r"^v\d+\.\d+\.\d+")


def _normalize(text: str) -> str:
    """Collapse whitespace so formatting tweaks don't split identical bullets."""
    return re.sub(r"\s+", " ", text).strip().lower()


def _iter_bullets(lines: Iterable[str]) -> Iterable[tuple[str, str, int]]:
    """Yield (section_title, normalized_bullet_text, line_number) tuples.

    Only lines under a `# ...` header are assigned to a section. Lines inside
    the license comment at the top of the file land under section "" and are
    skipped by the caller.
    """
    current_section = ""
    for lineno, raw in enumerate(lines, start=1):
        header = _TOP_HEADER_RE.match(raw)
        if header:
            current_section = header.group(1).strip()
            continue
        bullet = _BULLET_RE.match(raw)
        if bullet and current_section:
            yield current_section, _normalize(bullet.group(1)), lineno


def _first_released_section(lines: Iterable[str]) -> str | None:
    """Return the first `# vX.Y.Z` header in document order, or None."""
    for raw in lines:
        header = _TOP_HEADER_RE.match(raw)
        if header and _RELEASED_SECTION_RE.match(header.group(1).strip()):
            return header.group(1).strip()
    return None


def find_duplicates(path: Path) -> list[str]:
    """Return human-readable error messages, or an empty list if clean."""
    text = path.read_text(encoding="utf-8").splitlines()
    # bullet_text -> list of (section, lineno)
    occurrences: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for section, bullet, lineno in _iter_bullets(text):
        occurrences[bullet].append((section, lineno))

    most_recent_released = _first_released_section(text)

    errors: list[str] = []
    for bullet, places in occurrences.items():
        in_unreleased = [p for p in places if p[0] == _UNRELEASED_SECTION]
        in_released = [p for p in places if _RELEASED_SECTION_RE.match(p[0])]

        if len(in_unreleased) > 1:
            lines = ", ".join(f"line {ln}" for _, ln in in_unreleased)
            errors.append(
                f"Bullet appears multiple times in '{_UNRELEASED_SECTION}' "
                f"({lines}): {bullet!r}"
            )
        if in_unreleased and in_released:
            released_names = ", ".join(sorted({s for s, _ in in_released}))
            unreleased_lines = ", ".join(f"line {ln}" for _, ln in in_unreleased)
            errors.append(
                f"Bullet appears in both '{_UNRELEASED_SECTION}' "
                f"({unreleased_lines}) and released section(s) "
                f"[{released_names}]: {bullet!r}"
            )
        # A bullet under the most-recent released section that also appears
        # in an older released section is the rebase-context-drift case:
        # the author's patch dragged context bullets that already shipped
        # in a prior release into the current release-staging section.
        # Skip when the bullet is also in Unreleased — the check above
        # already flagged it, and firing here would emit a contradictory
        # second message ("move to Unreleased" vs. "remove from Unreleased").
        if most_recent_released and not in_unreleased:
            in_recent = [p for p in places if p[0] == most_recent_released]
            in_older_released = [
                p
                for p in places
                if _RELEASED_SECTION_RE.match(p[0]) and p[0] != most_recent_released
            ]
            if in_recent and in_older_released:
                older_names = ", ".join(sorted({s for s, _ in in_older_released}))
                recent_lines = ", ".join(f"line {ln}" for _, ln in in_recent)
                errors.append(
                    f"Bullet appears in both '{most_recent_released}' "
                    f"({recent_lines}) and older released section(s) "
                    f"[{older_names}]: {bullet!r}"
                )
    return errors


def main(argv: list[str] | None = None) -> int:
    path = Path(argv[0]) if argv else RELEASE_NOTES
    if not path.exists():
        print(f"RELEASE-NOTES.md not found at {path}", file=sys.stderr)
        return 1
    errors = find_duplicates(path)
    if errors:
        print(
            "Duplicate bullets found in RELEASE-NOTES.md.\n"
            "Common causes:\n"
            "  * The `merge=union` driver kept a bullet in both 'Unreleased "
            "version' and a released section after a release cut. Remove "
            "the bullet from 'Unreleased version'.\n"
            "  * A rebase landed a new bullet in the most-recent released "
            "section instead of 'Unreleased version', dragging along "
            "context bullets that already shipped in older releases. "
            "Move the new bullet to 'Unreleased version' and remove the "
            "stale duplicates.\n",
            file=sys.stderr,
        )
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
