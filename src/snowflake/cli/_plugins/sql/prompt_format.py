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

"""REPL prompt formatting from a template with session placeholders.

Interface (GA, opt-in):
- Default prompt remains `` > `` when no format is configured.
- Users set a format via ``[cli] prompt_format`` or
  ``snow sql --prompt-format``.
- Placeholders (case-insensitive): ``[user]``, ``[host]``, ``[account]``,
  ``[role]``, ``[warehouse]``, ``[database]``, ``[schema]``, ``[connection]``.
- Missing values render as ``(no user)``, ``(no database)``, and so on.
- ``\\n`` becomes a newline. ``\\[``, ``\\]``, and ``\\\\`` are literal
  ``[``, ``]``, and ``\\``. Every other backslash sequence, ``\\N``
  included, stays literal text; only ``[...]`` is reserved.
- Unrecognised ``[...]`` tokens are dropped from the rendered prompt and
  warned about once at REPL start. Dropping rather than passing them through
  as literal text reserves the whole ``[...]`` namespace for later extensions
  (for example colour directives ``[#rrggbb]`` and ``[bg:#rrggbb]``):
  ``[#ff00ff]`` renders as nothing today and can render as a colour tomorrow,
  and neither changes the visible text of a format that already works.

This module only expands a template. Wiring into the REPL and CLI flag lives
in ``repl.py`` / ``commands.py`` so the expander stays unit-testable without
opening a Snowflake connection.
"""

from __future__ import annotations

import re
from typing import Mapping

from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.sanitizers import sanitize_for_terminal

DEFAULT_REPL_PROMPT = " > "

PROMPT_PLACEHOLDERS = (
    "user",
    "host",
    "account",
    "role",
    "warehouse",
    "database",
    "schema",
    "connection",
)

_PLACEHOLDER_SET = frozenset(PROMPT_PLACEHOLDERS)
_MISSING_VALUE = {name: f"(no {name})" for name in PROMPT_PLACEHOLDERS}
_SUPPORTED_PLACEHOLDERS = ", ".join(f"[{name}]" for name in PROMPT_PLACEHOLDERS)

# Keep TAB and LF so a multi-line prompt still works; strip other C0 controls
# (including CR/NUL) so a session value cannot overwrite the prompt line.
_UNSAFE_PROMPT_CHARS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f]")

# ``\\\\`` is first so a doubled backslash is one literal ``\\``, not the
# start of ``\\[`` / ``\\]`` / ``\\n``.
_ESCAPE_REPLACEMENTS = {
    "\\\\": "\\",
    "\\[": "[",
    "\\]": "]",
    "\\n": "\n",
}
# Bracketed tokens match by shape and are resolved by lowercased name, which
# is what makes ``[USER]`` work. Matching is case-sensitive on purpose: a
# case-insensitive pattern would also make ``\N`` a newline escape, so
# ``foo\New>`` would lose the ``\N``.
_TOKEN_RE = re.compile(r"\\\\|\\\[|\\\]|\\n|\[[^\[\]]*\]")


def require_string_prompt_format(value: object) -> str | None:
    """Accept ``None`` or a string; reject other config types with a CliError."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise CliError(
            "Expected a string for cli.prompt_format, "
            f"got {type(value).__name__}. Use a quoted value, for example "
            'prompt_format = "[user]> ".'
        )
    return value


def unknown_tokens_in_prompt_format(template: str) -> tuple[str, ...]:
    """Return the unrecognised ``[...]`` tokens in ``template``, in order.

    Colour directives (``[#rrggbb]``, ``[bg:#rrggbb]``) are unrecognised in
    this version, so they come back here too and are reported the same way
    as a misspelled placeholder name.
    """
    seen: dict[str, None] = {}
    for match in _TOKEN_RE.finditer(template):
        token = match.group(0)
        if token in _ESCAPE_REPLACEMENTS:
            continue
        if token[1:-1].lower() not in _PLACEHOLDER_SET:
            # Same scrubbing as a rendered value: the token is echoed back in
            # a warning, so it must not carry escapes or CR into the terminal.
            seen.setdefault(_sanitize_prompt_text(token), None)
    return tuple(seen)


def unknown_token_warning(tokens: tuple[str, ...]) -> str:
    """Message shown once at REPL start when a format has unknown tokens."""
    listed = ", ".join(f"'{token}'" for token in tokens)
    return (
        f"Ignoring unknown prompt placeholder(s) {listed}. "
        f"Supported placeholders are {_SUPPORTED_PLACEHOLDERS}. "
        "Colour directives such as [#rrggbb] and [bg:#rrggbb] "
        "are not supported in this version. "
        r"Use \[ \] \\ for literal brackets and backslash."
    )


def format_repl_prompt(
    template: str | None,
    values: Mapping[str, object | None] | None = None,
) -> str:
    """Expand ``template`` with session ``values``.

    ``None`` or an empty template keeps the historical default so enabling
    this feature is not a breaking change. Placeholders match ignoring case
    (``[USER]`` is the same as ``[user]``). ``\\[``, ``\\]``, ``\\\\``, and
    ``\\n`` are expanded in the same pass and are case-sensitive. Unrecognised ``[token]`` values
    render as nothing, which keeps them available to mean something later;
    the caller warns about them once. Values are substituted in a single
    pass so a session name that happens to look like another placeholder is
    not re-expanded.
    """
    if not template:
        return DEFAULT_REPL_PROMPT

    values = values or {}

    def _expand(match: re.Match[str]) -> str:
        return _expand_token(match.group(0), values)

    result = _TOKEN_RE.sub(_expand, template)
    sanitized = _sanitize_prompt_text(result)
    return sanitized if sanitized else DEFAULT_REPL_PROMPT


def session_prompt_values(
    connection: object | None = None,
    connection_name: str | None = None,
) -> dict[str, str | None]:
    """Collect placeholder values from a live connection or CLI context.

    Passing ``connection`` is the REPL path: the connector mutates
    database/schema/role/warehouse in place, so ``USE`` shows on the next
    prompt without looking up the connection cache (that lookup can redial).

    When ``connection`` is omitted, only ``ConnectionContext`` is read.
    That is degraded output, not a live session snapshot: it carries CLI/env
    overrides and cannot reflect a ``USE`` issued in-session. It never
    calls ``get_cli_context().connection``.
    """
    if connection is not None:
        return _values_from_session_object(connection, connection_name)

    ctx = get_cli_context()
    resolved_name = connection_name
    if resolved_name is None:
        resolved_name = ctx.connection_context.connection_name
    return _values_from_session_object(ctx.connection_context, resolved_name)


def _expand_token(token: str, values: Mapping[str, object | None]) -> str:
    if token in _ESCAPE_REPLACEMENTS:
        return _ESCAPE_REPLACEMENTS[token]
    name = token[1:-1].lower()
    if name not in _PLACEHOLDER_SET:
        return ""
    raw = values.get(name)
    if raw is None:
        return _MISSING_VALUE[name]
    sanitized = _sanitize_prompt_text(raw)
    return sanitized if sanitized else _MISSING_VALUE[name]


def _sanitize_prompt_text(raw: object) -> str:
    text = sanitize_for_terminal(str(raw)) or ""
    return _UNSAFE_PROMPT_CHARS.sub("", text)


def _values_from_session_object(
    source: object, connection_name: str | None
) -> dict[str, str | None]:
    return {
        "user": getattr(source, "user", None),
        "host": getattr(source, "host", None),
        "account": getattr(source, "account", None),
        "role": getattr(source, "role", None),
        "warehouse": getattr(source, "warehouse", None),
        "database": getattr(source, "database", None),
        "schema": getattr(source, "schema", None),
        "connection": connection_name,
    }
