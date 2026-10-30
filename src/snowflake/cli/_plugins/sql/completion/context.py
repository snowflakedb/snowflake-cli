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

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

_UNQUOTED_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*")
_TEMPLATE_STARTS = ("<%", "{{", "{%", "&{")
_TEMPLATE_ENDS = ("%>", "}}", "%}")

_CLAUSE_KEYWORDS = frozenset(
    {
        "SELECT",
        "FROM",
        "WHERE",
        "JOIN",
        "UPDATE",
        "INSERT",
        "MERGE",
        "DELETE",
        "CREATE",
        "DROP",
        "USE",
        "DESC",
        "DESCRIBE",
        "WITH",
        "GROUP",
        "ORDER",
        "HAVING",
        "LIMIT",
        "QUALIFY",
        "SET",
        "VALUES",
        "LEFT",
        "RIGHT",
        "INNER",
        "FULL",
        "CROSS",
        "NATURAL",
        "ON",
        "USING",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "MINUS",
        "AS",
        "CAST",
        "TABLE",
        "VIEW",
        "DATABASE",
        "SCHEMA",
        "INTO",
    }
)

_AFTER_RELATION = frozenset(
    {
        "WHERE",
        "GROUP",
        "ORDER",
        "HAVING",
        "LIMIT",
        "QUALIFY",
        "SET",
        "VALUES",
        "ON",
        "USING",
        "JOIN",
        "LEFT",
        "RIGHT",
        "INNER",
        "FULL",
        "CROSS",
        "NATURAL",
        "SAMPLE",
        "PIVOT",
        "UNPIVOT",
        "START",
        "CONNECT",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "MINUS",
        "SELECT",
    }
)


class CompletionSlot(str, Enum):
    NONE = "none"
    KEYWORD = "keyword"
    OBJECT = "object"
    COLUMN = "column"
    TYPE = "type"


class CompletionKind(str, Enum):
    DATABASE = "database"
    SCHEMA = "schema"
    TABLE = "table"
    VIEW = "view"
    COLUMN = "column"
    KEYWORD = "keyword"
    FUNCTION = "function"
    TYPE = "type"
    NONE = "none"


_REL_FIRST = (
    CompletionKind.DATABASE,
    CompletionKind.SCHEMA,
    CompletionKind.TABLE,
    CompletionKind.VIEW,
)
_REL_SECOND = (
    CompletionKind.SCHEMA,
    CompletionKind.TABLE,
    CompletionKind.VIEW,
)
_REL_THIRD = (CompletionKind.TABLE, CompletionKind.VIEW)


@dataclass(frozen=True)
class SuggestContext:
    """`path` and `simple_relation` hold resolved names: unquoted segments
    uppercased, quoted segments verbatim."""

    slot: CompletionSlot
    kinds: tuple[CompletionKind, ...]
    path: tuple[str, ...]
    prefix: str
    quoted: bool
    statement_start: int
    statement_end: int
    simple_relation: tuple[str, ...] | None = None


@dataclass(frozen=True)
class _Scan:
    statement_start: int
    state: str
    saw_template: bool


@dataclass(frozen=True)
class _Ident:
    path: tuple[str, ...]
    prefix: str
    quoted: bool
    start: int
    ended_with_dot: bool


def classify(buffer: str, cursor: int | None = None) -> SuggestContext:
    if cursor is None:
        cursor = len(buffer)
    cursor = max(0, min(cursor, len(buffer)))

    scan = _scan(buffer, cursor)
    if scan.state in {"line_comment", "block_comment", "string", "dollar", "template"}:
        return _none(scan.statement_start, cursor)
    if scan.saw_template:
        return _none(scan.statement_start, cursor)

    stmt_through_cursor = buffer[scan.statement_start : cursor]
    if _is_bang_command(stmt_through_cursor):
        return _none(scan.statement_start, cursor)

    ident = _ident_at_cursor(
        buffer, scan.statement_start, cursor, scan.state == "dquote"
    )
    tokens = _tokenize(buffer[scan.statement_start : ident.start])

    if _is_column_path(ident, tokens):
        return _ctx(
            CompletionSlot.COLUMN,
            (CompletionKind.COLUMN,),
            ident,
            scan.statement_start,
            cursor,
        )

    if _is_type_position(tokens):
        return _ctx(
            CompletionSlot.TYPE,
            (CompletionKind.TYPE,),
            ident,
            scan.statement_start,
            cursor,
            path=(),
        )

    if _endswith_words(tokens, "USE", "DATABASE"):
        return _ctx(
            CompletionSlot.OBJECT,
            (CompletionKind.DATABASE,),
            ident,
            scan.statement_start,
            cursor,
        )
    if _endswith_words(tokens, "USE", "SCHEMA"):
        return _ctx(
            CompletionSlot.OBJECT,
            (CompletionKind.SCHEMA,),
            ident,
            scan.statement_start,
            cursor,
        )
    if _endswith_words(tokens, "USE"):
        return _ctx(
            CompletionSlot.KEYWORD,
            (CompletionKind.KEYWORD,),
            ident,
            scan.statement_start,
            cursor,
            path=(),
        )

    if _is_relation_position(tokens):
        return _ctx(
            CompletionSlot.OBJECT,
            _relation_kinds(ident.path),
            ident,
            scan.statement_start,
            cursor,
        )

    if ident.path and not _is_relation_position(tokens):
        return _ctx(
            CompletionSlot.COLUMN,
            (CompletionKind.COLUMN,),
            ident,
            scan.statement_start,
            cursor,
        )

    if _last_clause(tokens) in {"SELECT", "WHERE"}:
        full_stmt = buffer[
            scan.statement_start : _statement_limit(buffer, cursor, scan.state)
        ]
        simple = _simple_relation(full_stmt)
        if simple:
            return _ctx(
                CompletionSlot.COLUMN,
                (CompletionKind.COLUMN,),
                ident,
                scan.statement_start,
                cursor,
                path=(),
                simple_relation=simple,
            )

    return _ctx(
        CompletionSlot.KEYWORD,
        (CompletionKind.KEYWORD,),
        ident,
        scan.statement_start,
        cursor,
        path=(),
    )


def _none(statement_start: int, statement_end: int) -> SuggestContext:
    return SuggestContext(
        slot=CompletionSlot.NONE,
        kinds=(CompletionKind.NONE,),
        path=(),
        prefix="",
        quoted=False,
        statement_start=statement_start,
        statement_end=statement_end,
    )


def _ctx(
    slot: CompletionSlot,
    kinds: tuple[CompletionKind, ...],
    ident: _Ident,
    statement_start: int,
    statement_end: int,
    *,
    path: tuple[str, ...] | None = None,
    simple_relation: tuple[str, ...] | None = None,
) -> SuggestContext:
    return SuggestContext(
        slot=slot,
        kinds=kinds,
        path=ident.path if path is None else path,
        prefix=ident.prefix,
        quoted=ident.quoted,
        statement_start=statement_start,
        statement_end=statement_end,
        simple_relation=simple_relation,
    )


def _scan(buffer: str, cursor: int) -> _Scan:
    state = "normal"
    statement_start = 0
    saw_template = False
    i = 0
    while i < cursor:
        i, state, event = _step(buffer, i, state)
        if event == "semicolon":
            statement_start = i
            saw_template = False
        elif event == "template":
            saw_template = True
    return _Scan(statement_start, state, saw_template)


def _statement_limit(buffer: str, cursor: int, state: str) -> int:
    """End of the current statement, past the cursor, at the next unquoted `;`."""
    if state not in {"normal", "dquote"}:
        return cursor
    i = cursor
    while i < len(buffer):
        i, state, event = _step(buffer, i, state)
        if event == "semicolon":
            return i - 1
    return len(buffer)


def _step_normal(buffer: str, i: int) -> tuple[int, str, str | None]:
    if buffer.startswith("--", i):
        return i + 2, "line_comment", None
    if buffer.startswith("/*", i):
        return i + 2, "block_comment", None
    if buffer.startswith("$$", i):
        return i + 2, "dollar", None
    if any(buffer.startswith(tok, i) for tok in _TEMPLATE_STARTS):
        return i + 2, "template", "template"
    char = buffer[i]
    if char == "'":
        return i + 1, "string", None
    if char == '"':
        return i + 1, "dquote", None
    if char == ";":
        return i + 1, "normal", "semicolon"
    return i + 1, "normal", None


def _step(buffer: str, i: int, state: str) -> tuple[int, str, str | None]:
    if state == "normal":
        return _step_normal(buffer, i)
    if state == "line_comment":
        return i + 1, "normal" if buffer[i] == "\n" else "line_comment", None
    if state == "block_comment":
        if buffer.startswith("*/", i):
            return i + 2, "normal", None
        return i + 1, "block_comment", None
    if state == "dollar":
        if buffer.startswith("$$", i):
            return i + 2, "normal", None
        return i + 1, "dollar", None
    if state == "string":
        if buffer[i] == "'":
            if i + 1 < len(buffer) and buffer[i + 1] == "'":
                return i + 2, "string", None
            return i + 1, "normal", None
        return i + 1, "string", None
    if state == "dquote":
        if buffer[i] == '"':
            if i + 1 < len(buffer) and buffer[i + 1] == '"':
                return i + 2, "dquote", None
            return i + 1, "normal", None
        return i + 1, "dquote", None
    if state == "template":
        if any(buffer.startswith(tok, i) for tok in _TEMPLATE_ENDS):
            return i + 2, "normal", None
        return i + 1, "template", None
    return i + 1, state, None


def _is_bang_command(stmt: str) -> bool:
    return stmt.lstrip().startswith("!")


def _ident_at_cursor(
    buffer: str, stmt_start: int, cursor: int, in_dquote: bool
) -> _Ident:
    path: list[str] = []
    prefix = ""
    quoted = False
    i = cursor

    if in_dquote:
        open_q = _last_opening_dquote(buffer, stmt_start, cursor)
        prefix = buffer[open_q + 1 : cursor]
        quoted = True
        i = open_q
    else:
        j = cursor
        while j > stmt_start and _is_ident_char(buffer[j - 1]):
            j -= 1
        prefix = buffer[j:cursor]
        i = j

    ended_with_dot = prefix == "" and i > stmt_start and buffer[i - 1] == "."

    while i > stmt_start and buffer[i - 1] == ".":
        i -= 1
        segment, i = _read_segment_backward(buffer, stmt_start, i)
        if segment is None:
            break
        path.append(segment)

    return _Ident(
        path=tuple(reversed(path)),
        prefix=prefix,
        quoted=quoted,
        start=i,
        ended_with_dot=ended_with_dot,
    )


def _last_opening_dquote(buffer: str, start: int, cursor: int) -> int:
    i = start
    last_open = start
    in_quote = False
    while i < cursor:
        if buffer[i] == '"':
            if in_quote and i + 1 < cursor and buffer[i + 1] == '"':
                i += 2
                continue
            if in_quote:
                in_quote = False
            else:
                last_open = i
                in_quote = True
        i += 1
    return last_open


def _read_segment_backward(buffer: str, start: int, end: int) -> tuple[str | None, int]:
    if end <= start:
        return None, end
    if buffer[end - 1] == '"':
        close = end - 1
        i = close - 1
        chars: list[str] = []
        while i >= start:
            if buffer[i] == '"':
                if i > start and buffer[i - 1] == '"':
                    chars.append('"')
                    i -= 2
                    continue
                return "".join(reversed(chars)), i
            chars.append(buffer[i])
            i -= 1
        return "".join(reversed(chars)), start
    i = end
    while i > start and _is_ident_char(buffer[i - 1]):
        i -= 1
    if i == end:
        return None, end
    return buffer[i:end].upper(), i


def _is_ident_char(char: str) -> bool:
    return char.isalnum() or char in "_$"


def _is_column_path(ident: _Ident, tokens: list[tuple[str, str]]) -> bool:
    if len(ident.path) >= 3:
        return True
    if ident.ended_with_dot and len(ident.path) == 1:
        return not _is_relation_position(tokens)
    return False


def _relation_kinds(path: tuple[str, ...]) -> tuple[CompletionKind, ...]:
    if len(path) == 0:
        return _REL_FIRST
    if len(path) == 1:
        return _REL_SECOND
    return _REL_THIRD


def _is_type_position(tokens: list[tuple[str, str]]) -> bool:
    if not tokens:
        return False
    if tokens[-1] == ("PUNCT", "::"):
        return True
    if _word(tokens[-1]) == "AS" and any(_word(tok) == "CAST" for tok in tokens):
        return True
    return _is_create_table_type(tokens)


def _is_create_table_type(tokens: list[tuple[str, str]]) -> bool:
    """Cursor follows a column name at the top level of CREATE TABLE's column list."""
    open_paren = _column_list_start(tokens)
    if open_paren is None or len(tokens) - open_paren < 2:
        return False
    if tokens[-1][0] not in {"WORD", "IDENT"}:
        return False
    if tokens[-2] not in {("PUNCT", "("), ("PUNCT", ",")}:
        return False
    return _paren_depth(tokens[open_paren:-1]) == 1


_CREATE_TABLE_FILLERS = frozenset(
    {
        "OR",
        "REPLACE",
        "ALTER",
        "TRANSIENT",
        "TEMP",
        "TEMPORARY",
        "LOCAL",
        "GLOBAL",
        "VOLATILE",
        "ICEBERG",
        "HYBRID",
        "EXTERNAL",
        "DYNAMIC",
    }
)


def _column_list_start(tokens: list[tuple[str, str]]) -> int | None:
    """Index of the `(` opening the column list in `CREATE … TABLE [IF NOT EXISTS] name (`."""
    if not tokens or _word(tokens[0]) != "CREATE":
        return None
    index = 1
    while index < len(tokens) and _word(tokens[index]) in _CREATE_TABLE_FILLERS:
        index += 1
    if index >= len(tokens) or _word(tokens[index]) != "TABLE":
        return None
    index += 1
    if [_word(tok) for tok in tokens[index : index + 3]] == ["IF", "NOT", "EXISTS"]:
        index += 3
    name, index = _read_dotted_name(tokens, index)
    if name is None or index >= len(tokens) or tokens[index] != ("PUNCT", "("):
        return None
    return index


def _paren_depth(tokens: list[tuple[str, str]]) -> int:
    """Nesting depth at the end of `tokens`; -1 once a paren closes below the start."""
    depth = 0
    for tok in tokens:
        if tok == ("PUNCT", "("):
            depth += 1
        elif tok == ("PUNCT", ")"):
            depth -= 1
            if depth <= 0:
                return -1
    return depth


def _is_relation_position(tokens: list[tuple[str, str]]) -> bool:
    if _endswith_words(tokens, "IF", "EXISTS"):
        return _endswith_words(tokens[:-2], "DROP", "TABLE") or _endswith_words(
            tokens[:-2], "DROP", "VIEW"
        )
    if not tokens:
        return False
    if tokens[-1] == ("PUNCT", ","):
        return _in_from_list(tokens[:-1])
    last = _word(tokens[-1])
    if last in {"FROM", "JOIN", "UPDATE", "DESC", "DESCRIBE"}:
        return True
    if last in {"TABLE", "VIEW"} and len(tokens) >= 2:
        prev = _word(tokens[-2])
        return prev in {"DROP", "DESC", "DESCRIBE"}
    if last == "INTO" and len(tokens) >= 2:
        return _word(tokens[-2]) in {"INSERT", "MERGE"}
    return False


_ENDS_FROM_LIST = frozenset(
    {
        "SELECT",
        "WHERE",
        "GROUP",
        "ORDER",
        "HAVING",
        "LIMIT",
        "QUALIFY",
        "SET",
        "VALUES",
        "ON",
        "USING",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "MINUS",
        "WITH",
        "INTO",
        "UPDATE",
        "INSERT",
        "MERGE",
        "DELETE",
        "CREATE",
        "DROP",
        "USE",
        "DESC",
        "DESCRIBE",
    }
)


def _in_from_list(tokens: list[tuple[str, str]]) -> bool:
    """A comma here separates relations of a FROM clause at the same paren level."""
    depth = 0
    for tok in reversed(tokens):
        if tok == ("PUNCT", ")"):
            depth += 1
        elif tok == ("PUNCT", "("):
            if depth == 0:
                return False
            depth -= 1
        elif depth == 0:
            word = _word(tok)
            if word in {"FROM", "JOIN"}:
                return True
            if word in _ENDS_FROM_LIST:
                return False
    return False


def _endswith_words(tokens: list[tuple[str, str]], *words: str) -> bool:
    if len(tokens) < len(words):
        return False
    tail = tokens[-len(words) :]
    return all(_word(tok) == expected for tok, expected in zip(tail, words))


def _word(tok: tuple[str, str]) -> str | None:
    if tok[0] != "WORD":
        return None
    return tok[1].upper()


def _last_clause(tokens: list[tuple[str, str]]) -> str | None:
    for tok in reversed(tokens):
        word = _word(tok)
        if word in _CLAUSE_KEYWORDS:
            return word
    return None


def _simple_relation(stmt: str) -> tuple[str, ...] | None:
    tokens = _tokenize(stmt)
    if tokens and _word(tokens[0]) == "WITH":
        return None

    relations: list[tuple[str, ...]] = []
    index = 0
    while index < len(tokens):
        word = _word(tokens[index])
        takes_relation = word in {"FROM", "JOIN", "UPDATE"}
        if (
            word == "INTO"
            and index > 0
            and _word(tokens[index - 1]) in {"INSERT", "MERGE"}
        ):
            takes_relation = True
        if not takes_relation:
            index += 1
            continue
        index += 1
        name, index = _read_dotted_name(tokens, index)
        if name is None:
            continue
        if _relation_is_aliased(tokens, index):
            return None
        relations.append(name)
    if len(relations) != 1:
        return None
    return relations[0]


def _relation_is_aliased(tokens: list[tuple[str, str]], index: int) -> bool:
    if index >= len(tokens):
        return False
    tok = tokens[index]
    if tok == ("PUNCT", ","):
        return True
    if tok[0] == "IDENT":
        return True
    word = _word(tok)
    if word == "AS":
        return True
    if word is None:
        return False
    return word not in _AFTER_RELATION


def _read_dotted_name(
    tokens: list[tuple[str, str]], index: int
) -> tuple[tuple[str, ...] | None, int]:
    parts: list[str] = []
    while index < len(tokens):
        tok = tokens[index]
        if tok[0] in {"WORD", "IDENT"}:
            parts.append(tok[1].upper() if tok[0] == "WORD" else tok[1])
            index += 1
            if index < len(tokens) and tokens[index] == ("PUNCT", "."):
                index += 1
                continue
            break
        break
    if not parts:
        return None, index
    return tuple(parts), index


def _tokenize(text: str) -> list[tuple[str, str]]:
    tokens: list[tuple[str, str]] = []
    index = 0
    length = len(text)
    while index < length:
        char = text[index]
        if char.isspace():
            index += 1
            continue
        if text.startswith("--", index):
            newline = text.find("\n", index)
            index = length if newline < 0 else newline
            continue
        if text.startswith("/*", index):
            end = text.find("*/", index + 2)
            index = length if end < 0 else end + 2
            continue
        if text.startswith("::", index):
            tokens.append(("PUNCT", "::"))
            index += 2
            continue
        if char == "'":
            index = _skip_quoted(text, index, "'")
            tokens.append(("STRING", ""))
            continue
        if char == '"':
            value, index = _read_quoted(text, index)
            tokens.append(("IDENT", value))
            continue
        if char in ".,()":
            tokens.append(("PUNCT", char))
            index += 1
            continue
        match = _UNQUOTED_IDENT.match(text, index)
        if match:
            tokens.append(("WORD", match.group()))
            index = match.end()
            continue
        index += 1
    return tokens


def _skip_quoted(text: str, start: int, quote: str) -> int:
    index = start + 1
    while index < len(text):
        if text[index] == quote:
            if index + 1 < len(text) and text[index + 1] == quote:
                index += 2
                continue
            return index + 1
        index += 1
    return len(text)


def _read_quoted(text: str, start: int) -> tuple[str, int]:
    chars: list[str] = []
    index = start + 1
    while index < len(text):
        if text[index] == '"':
            if index + 1 < len(text) and text[index + 1] == '"':
                chars.append('"')
                index += 2
                continue
            return "".join(chars), index + 1
        chars.append(text[index])
        index += 1
    return "".join(chars), len(text)
