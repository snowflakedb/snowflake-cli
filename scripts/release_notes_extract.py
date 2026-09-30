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

"""Parse version blocks from the CLI ``RELEASE-NOTES.md`` file."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ReleaseNotesSection:
    title: str
    bullets: tuple[str, ...]


@dataclass(frozen=True)
class ParsedReleaseNotes:
    version: str
    sections: tuple[ReleaseNotesSection, ...]


def extract_release_notes_from_file(file: Path, version: str) -> str:
    """Return the raw markdown block for ``# v{version}`` through the next version."""
    result: list[str] = []
    version_heading = f"# v{version}"

    with file.open(encoding="utf-8") as contents:
        extract_lines = False
        for line in contents:
            stripped = line.rstrip()
            if (
                extract_lines
                and stripped.startswith("# v")
                and stripped != version_heading
            ):
                break
            if stripped == version_heading:
                extract_lines = True
            if extract_lines:
                result.append(stripped)

    return "\n".join(result)


_NESTED_BULLET_RE = re.compile(r"^(\s+)[*-] ")


def parse_release_notes_sections(raw: str, *, version: str) -> ParsedReleaseNotes:
    """Parse a version block into structured sections and bullets."""
    sections: list[ReleaseNotesSection] = []
    current_title: str | None = None
    current_bullets: list[str] = []
    current_bullet: str | None = None

    def _flush_bullet() -> None:
        nonlocal current_bullet
        if current_bullet is not None:
            current_bullets.append(current_bullet)
            current_bullet = None

    def _flush_section() -> None:
        nonlocal current_title, current_bullets
        _flush_bullet()
        if current_title is None:
            return
        sections.append(
            ReleaseNotesSection(title=current_title, bullets=tuple(current_bullets))
        )
        current_title = None
        current_bullets = []

    for line in raw.splitlines():
        if line == f"# v{version}":
            continue
        if line.startswith("## "):
            _flush_section()
            current_title = line[3:].strip()
            continue
        if current_title is None or not line.strip():
            continue
        if line.startswith("* "):
            _flush_bullet()
            current_bullet = line[2:]
            continue
        nested_match = _NESTED_BULLET_RE.match(line)
        if nested_match:
            indent = nested_match.group(1)
            sub = line.strip()[2:].strip()
            if current_bullet is None:
                current_bullet = sub
            else:
                current_bullet = f"{current_bullet}\n{indent}- {sub}"
            continue
        if line.startswith("  "):
            if current_bullet is None:
                continue
            current_bullet = f"{current_bullet} {line.strip()}"
            continue
        if current_bullet is not None:
            current_bullet = f"{current_bullet} {line.strip()}"

    _flush_section()
    return ParsedReleaseNotes(version=version, sections=tuple(sections))


def load_parsed_release_notes(path: Path, version: str) -> ParsedReleaseNotes:
    raw = extract_release_notes_from_file(path, version)
    if not raw.strip():
        raise ValueError(f"No release notes found for version {version} in {path}")
    return parse_release_notes_sections(raw, version=version)
