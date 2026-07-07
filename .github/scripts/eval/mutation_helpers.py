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

"""Helpers for expressing eval-case mutations as idempotent, anchored edits.

Each PR-review eval case applies a small source mutation to a fresh checkout of
`main` (see ``cases/<id>/mutate.py``). These helpers make those edits:

- **anchored** — they raise :class:`AnchorNotFoundError` if the target text is gone,
  so a case can't silently degrade into a no-op when the code drifts;
- **unique** — they raise :class:`AmbiguousAnchorError` when an anchor matches more
  than once, keeping the mutation deterministic;
- **idempotent** — applying twice equals applying once, which the runner and
  the tests rely on.
"""

from __future__ import annotations

import re
from pathlib import Path


class AnchorNotFoundError(Exception):
    """Neither the anchor nor the already-applied result was found."""


class AmbiguousAnchorError(Exception):
    """The anchor matched more than once, so the edit is not deterministic."""


def replace_once(repo_root: Path, rel_path: str, old: str, new: str) -> None:
    """Replace the single occurrence of *old* with *new* in *rel_path*.

    Idempotent: once *old* is gone and *new* is present, a second call is a
    no-op. Raises on a missing or ambiguous anchor.
    """
    path = Path(repo_root) / rel_path
    text = path.read_text(encoding="utf-8")
    count = text.count(old)
    if count == 1:
        path.write_text(text.replace(old, new, 1), encoding="utf-8")
    elif count == 0:
        if new in text:
            return  # already applied
        raise AnchorNotFoundError(f"{rel_path}: anchor not found: {old!r}")
    else:
        raise AmbiguousAnchorError(f"{rel_path}: anchor matched {count}x: {old!r}")


def insert_line_before(
    repo_root: Path, rel_path: str, anchor: str, new_line: str
) -> None:
    """Insert *new_line* on its own line just before the line containing *anchor*.

    The inserted line matches the anchor line's indentation. Idempotent: a no-op
    only when *new_line* is already the line **immediately preceding** the anchor
    (an identical string elsewhere in the file does not suppress the edit).
    Raises on a missing or ambiguous anchor.
    """
    path = Path(repo_root) / rel_path
    text = path.read_text(encoding="utf-8")
    count = text.count(anchor)
    if count == 0:
        raise AnchorNotFoundError(f"{rel_path}: anchor not found: {anchor!r}")
    if count > 1:
        raise AmbiguousAnchorError(f"{rel_path}: anchor matched {count}x: {anchor!r}")
    idx = text.index(anchor)
    line_start = text.rfind("\n", 0, idx) + 1
    indent = text[line_start:idx]
    # Already applied only if new_line is the immediately preceding line.
    if line_start > 0:
        prev_end = line_start - 1  # the newline terminating the previous line
        prev_start = text.rfind("\n", 0, prev_end) + 1
        if text[prev_start:prev_end] == indent + new_line:
            return
    path.write_text(
        text[:line_start] + indent + new_line + "\n" + text[line_start:],
        encoding="utf-8",
    )


def rename_symbol(repo_root: Path, rel_path: str, old: str, new: str) -> None:
    """Rename every whole-word occurrence of identifier *old* to *new* in *rel_path*.

    Used for multi-site renames (definition + call sites). Idempotent: a no-op
    once *old* is gone and *new* is present. Raises if neither is found.
    """
    path = Path(repo_root) / rel_path
    text = path.read_text(encoding="utf-8")
    old_pat = r"\b" + re.escape(old) + r"\b"
    if re.search(old_pat, text):
        path.write_text(re.sub(old_pat, new, text), encoding="utf-8")
    elif re.search(r"\b" + re.escape(new) + r"\b", text):
        return  # already applied
    else:
        raise AnchorNotFoundError(f"{rel_path}: symbol not found: {old!r}")
