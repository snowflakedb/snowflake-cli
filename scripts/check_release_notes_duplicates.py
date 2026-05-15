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
both copies of a bullet that the release cut has already moved from
"Unreleased version" into a `# vX.Y.Z` section. This script flags that case
so the author removes the already-released bullet from Unreleased.
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


def find_duplicates(path: Path) -> list[str]:
    """Return human-readable error messages, or an empty list if clean."""
    text = path.read_text(encoding="utf-8").splitlines()
    # bullet_text -> list of (section, lineno)
    occurrences: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for section, bullet, lineno in _iter_bullets(text):
        occurrences[bullet].append((section, lineno))

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
            "After a release cut, the `merge=union` driver can leave bullets "
            "in both 'Unreleased version' and the new released section.\n"
            "Remove the bullet from 'Unreleased version' — it's already "
            "documented under the released version.\n",
            file=sys.stderr,
        )
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
