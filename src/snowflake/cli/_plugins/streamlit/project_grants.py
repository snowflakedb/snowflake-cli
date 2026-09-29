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

"""Recording a share back into the ``grants:`` list of ``snowflake.yml``.

A share issued from the CLI has to survive the next deploy, which means it has
to land in the project file. The file belongs to the user, so the edit is made
by inserting lines rather than by re-serializing the document: a round-trip
through PyYAML would drop every comment, quote style and blank line in it.

The insertion is therefore textual, and then checked: the rewritten file is
re-parsed and compared against the original parse, and anything other than the
expected `grants:` addition puts the original text back. On any doubt at all
this reports failure and leaves the file untouched, so the caller can print the
entry for the user to paste.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

import yaml
from snowflake.cli.api.project.schemas.entities.common import Grant
from snowflake.cli.api.secure_path import SecurePath

_MAX_PROJECT_FILE_SIZE_MB = 5
# A `key:` line of a block mapping, whatever follows the colon.
_KEY_LINE = re.compile(r"([^\s#:]+):(?:\s|$)")


def add_grants(
    project_file: SecurePath, entity_id: str, grants: List[Grant]
) -> Optional[str]:
    """Append `grants` to `entity_id`'s ``grants:`` list, skipping duplicates.

    Returns None when the file now records every grant, or a reason when it was
    left alone.
    """
    if not project_file.exists():
        return f"{project_file.path} does not exist"

    original = project_file.read_text(file_size_limit_mb=_MAX_PROJECT_FILE_SIZE_MB)
    try:
        before = yaml.safe_load(original)
    except yaml.YAMLError as error:
        return f"{project_file.path} is not readable as YAML: {error}"

    entity = _entity_of(before, entity_id)
    if entity is None:
        return f"{project_file.path} has no '{entity_id}' entity under 'entities'"

    missing = [grant for grant in grants if not _already_recorded(entity, grant)]
    if not missing:
        return None

    edited = _insert_grants(original, entity_id, missing)
    if edited is None:
        return (
            f"the '{entity_id}' entity's 'grants' could not be edited"
            f" in {project_file.path}"
        )

    if not _adds_only_the_grants(before, edited, entity_id, missing):
        # Belt and braces: the line insertion changed something else, or landed
        # under the wrong key. Nothing is written in that case.
        return f"editing {project_file.path} would have changed more than 'grants'"

    project_file.write_text(edited)
    return None


def _entity_of(document: Any, entity_id: str) -> Optional[Dict[str, Any]]:
    """The entity's mapping, or None if this is not a v2 project file."""
    if not isinstance(document, dict):
        return None
    entity = (document.get("entities") or {}).get(entity_id)
    return entity if isinstance(entity, dict) else None


def _already_recorded(entity: Dict[str, Any], grant: Grant) -> bool:
    """Whether an equivalent entry is already in the entity's `grants:`.

    Identifiers are compared case-insensitively, since an unquoted name in the
    file and the same name typed in another case are one grant to Snowflake.

    An entry only covers this grant if it is at least as strong: a plain entry
    does not stand in for a share issued WITH GRANT OPTION, since the next
    deploy would re-apply the weaker grant the file records.
    """
    recorded = entity.get("grants") or []
    if not isinstance(recorded, list):
        return False
    wanted = _grant_key(grant.privilege, grant.role, grant.user)
    for entry in recorded:
        if not isinstance(entry, dict):
            continue
        if (
            _grant_key(entry.get("privilege"), entry.get("role"), entry.get("user"))
            != wanted
        ):
            continue
        if grant.with_grant_option and not entry.get("with_grant_option"):
            continue
        return True
    return False


def _grant_key(
    privilege: Any, role: Any, user: Any
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    def fold(value: Any) -> Optional[str]:
        return value.upper() if isinstance(value, str) else None

    return fold(privilege), fold(role), fold(user)


def _insert_grants(text: str, entity_id: str, grants: List[Grant]) -> Optional[str]:
    """Return `text` with `grants` appended to the entity's `grants:` list.

    None when the entity's block cannot be identified, which is the signal to
    leave the file alone.
    """
    lines = text.splitlines(keepends=True)
    entity_start = _entity_key_line(lines, entity_id)
    if entity_start is None or not _opens_block(lines[entity_start]):
        return None

    key_indent = _indent_of(lines[entity_start])
    body = range(entity_start + 1, _block_end(lines, entity_start, key_indent))
    field_indent = next(
        (_indent_of(lines[i]) for i in body if lines[i].strip()), key_indent + 2
    )
    if field_indent <= key_indent:
        # An entity with no fields at all; there is no indent to match.
        return None

    # Matched on the key alone: a `grants:` line carrying a trailing comment or
    # an anchor is still the entity's `grants:`, and inserting a second one would
    # parse as last-key-wins and drop every grant already recorded.
    grants_key = next(
        (
            i
            for i in body
            if _key_name(lines[i]) == "grants" and _indent_of(lines[i]) == field_indent
        ),
        None,
    )
    if grants_key is None:
        insert_at = _last_content_line(lines, body) + 1
        rendered = [f"{' ' * field_indent}grants:\n"]
        item_indent = field_indent + 2
    else:
        if not _opens_block(lines[grants_key]):
            # `grants: []` and the like: an inline value has no block to append
            # to, and a second key is not an option either.
            return None
        items = range(grants_key + 1, _block_end(lines, grants_key, field_indent))
        item_indent = next(
            (_indent_of(lines[i]) for i in items if lines[i].strip()), field_indent + 2
        )
        insert_at = _last_content_line(lines, items) + 1
        rendered = []

    for grant in grants:
        rendered.extend(_render(grant, item_indent))
    return "".join(lines[:insert_at] + rendered + lines[insert_at:])


def _render(grant: Grant, indent: int) -> List[str]:
    pad = " " * indent
    grantee = f"role: {grant.role}" if grant.role else f"user: {grant.user}"
    rendered = [f"{pad}- privilege: {grant.privilege}\n", f"{pad}  {grantee}\n"]
    if grant.with_grant_option:
        rendered.append(f"{pad}  with_grant_option: true\n")
    return rendered


def _entity_key_line(lines: List[str], entity_id: str) -> Optional[int]:
    """Index of the `<entity_id>:` line, as a direct child of `entities:`.

    Direct children only, matched on the indent the first of them sets: a key
    nested deeper that happens to share the entity's name is a field of some
    other entity, and inserting grants there would put them on that entity.
    """
    entities = next(
        (
            i
            for i, line in enumerate(lines)
            if _indent_of(line) == 0
            and _key_name(line) == "entities"
            and _opens_block(line)
        ),
        None,
    )
    if entities is None:
        return None
    body = range(entities + 1, _block_end(lines, entities, 0))
    child_indent = next(
        (_indent_of(lines[i]) for i in body if _is_content(lines[i])), None
    )
    if child_indent is None:
        return None
    return next(
        (
            i
            for i in body
            if _indent_of(lines[i]) == child_indent and _key_name(lines[i]) == entity_id
        ),
        None,
    )


def _key_name(line: str) -> Optional[str]:
    """The mapping key this line sets, whatever follows the colon."""
    match = _KEY_LINE.match(line.strip())
    return match.group(1) if match else None


def _opens_block(line: str) -> bool:
    """Whether a nested block follows this `key:` line, so items can go under it.

    A trailing comment, anchor or tag still opens a block; an inline value such
    as `grants: []` does not.
    """
    _, _, rest = line.strip().partition(":")
    for token in rest.split():
        if token.startswith("#"):
            break
        if not token.startswith(("&", "!")):
            return False
    return True


def _is_content(line: str) -> bool:
    """Whether a line carries YAML rather than blank space or a comment."""
    stripped = line.strip()
    return bool(stripped) and not stripped.startswith("#")


def _indent_of(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _block_end(lines: List[str], key_line: int, key_indent: int) -> int:
    """Index one past the last line belonging to the block opened at `key_line`."""
    for i in range(key_line + 1, len(lines)):
        if not _is_content(lines[i]):
            continue
        if _indent_of(lines[i]) <= key_indent:
            return i
    return len(lines)


def _last_content_line(lines: List[str], span: range) -> int:
    """The last non-blank, non-comment line of `span`, so trailing blanks and a
    comment that introduces whatever follows stay below the insertion."""
    last = span.start - 1
    for i in span:
        if _is_content(lines[i]):
            last = i
    return last


def _adds_only_the_grants(
    before: Any, edited: str, entity_id: str, grants: List[Grant]
) -> bool:
    """Whether re-parsing `edited` yields `before` plus exactly these grants."""
    try:
        after = yaml.safe_load(edited)
    except yaml.YAMLError:
        return False

    entity = _entity_of(after, entity_id)
    if entity is None:
        return False
    # The whole list, not just its tail: a `grants:` key that ended up duplicated
    # parses as last-key-wins, which leaves the tail looking exactly as asked for
    # while every entry the file already recorded is gone.
    expected = _recorded_grants(before, entity_id) + [
        _as_mapping(grant) for grant in grants
    ]
    if [_normalized(entry) for entry in (entity.get("grants") or [])] != [
        _normalized(entry) for entry in expected
    ]:
        return False

    # Everything else, `grants` of this one entity aside, must be untouched.
    return _without_grants(before, entity_id) == _without_grants(after, entity_id)


def _recorded_grants(document: Any, entity_id: str) -> List[Any]:
    """The entity's `grants:` entries as they parse today, or an empty list."""
    recorded = (_entity_of(document, entity_id) or {}).get("grants")
    return list(recorded) if isinstance(recorded, list) else []


def _as_mapping(grant: Grant) -> Dict[str, Any]:
    mapping: Dict[str, Any] = {"privilege": grant.privilege}
    mapping.update({"role": grant.role} if grant.role else {"user": grant.user})
    if grant.with_grant_option:
        mapping["with_grant_option"] = True
    return mapping


def _normalized(entry: Any) -> Any:
    if not isinstance(entry, dict):
        return entry
    return {key: entry[key] for key in sorted(entry)}


def _without_grants(document: Any, entity_id: str) -> Any:
    entity = _entity_of(document, entity_id)
    if entity is None:
        return document
    trimmed = {key: value for key, value in entity.items() if key != "grants"}
    entities = {**document["entities"], entity_id: trimmed}
    return {**document, "entities": entities}
