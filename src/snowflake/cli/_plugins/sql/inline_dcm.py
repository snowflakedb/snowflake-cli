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

"""Keep an inline DCM script as one statement.

``EXECUTE DCM PLAN|DEPLOY FROM BEGIN ... END`` (and the ``AS`` form selected
when the account flag is off) is one Snowflake statement. The body uses ``;``
as real terminators. ``split_statements`` does not know that block, and other
callers still need ordinary semicolon splitting, so this is a pre-pass in the
SQL plugin only.
"""

from __future__ import annotations

import io
import re
from collections.abc import Iterator

from snowflake.connector.util_text import split_statements

# Longest first so IGNORE_ASSIGNMENTS wins over IGNORE.
_KEYWORDS: tuple[str, ...] = (
    "IGNORE_ASSIGNMENTS",
    "EXECUTE",
    "DEPLOY",
    "REPEAT",
    "BEGIN",
    "WHILE",
    "LOOP",
    "PLAN",
    "FROM",
    "CASE",
    "DCM",
    "END",
    "FOR",
    "IF",
    "AS",
    "IGNORE",
)
_END_CLAUSE = frozenset({"IF", "LOOP", "WHILE", "FOR", "REPEAT"})
# others3.g beginStatement, and psm.g matches BEGIN TRANSACTION before a block.
_BEGIN_STATEMENT = frozenset({"TRANSACTION", "WORK", "NAME"})
_TRAILING_COMMENT = re.compile(r"\s*--")
_ONLY_SPACE = re.compile(r"\s*\Z")


def _is_ident(ch: str) -> bool:
    return ch.isascii() and (ch.isalnum() or ch in "_$")


class _Scanner:
    """Top-level scan: quotes, comments, dollar bodies, and parentheses."""

    def __init__(self, sql: str) -> None:
        self.sql = sql
        self.n = len(sql)
        self.i = 0
        self.paren = 0

    def at_execute(self) -> bool:
        return self.paren == 0 and self._matches("EXECUTE")

    def try_open(self, remove_comments: bool) -> int | None:
        """If ``self.i`` starts a DCM Lite statement, return the end index."""
        mark_i = self.i
        mark_paren = self.paren
        if not self._match_opening():
            self.i = mark_i
            self.paren = mark_paren
            return None
        if not self._consume_body(remove_comments):
            # Unclosed block: keep the rest so inner ';' stay in this statement.
            self.i = self.n
        self.paren = mark_paren
        return self.i

    def _match_opening(self) -> bool:
        if not self._consume_kw("EXECUTE") or not self._consume_kw("DCM"):
            return False
        if not (self._consume_kw("PLAN") or self._consume_kw("DEPLOY")):
            return False
        mark = self.i
        if not self._consume_kw("IGNORE_ASSIGNMENTS"):
            self.i = mark
        if not (self._consume_kw("FROM") or self._consume_kw("AS")):
            return False
        return self._consume_kw("BEGIN")

    def _consume_body(self, remove_comments: bool) -> bool:
        """Consume through the ``END`` that closes the opening ``BEGIN``.

        ``BEGIN`` / ``CASE`` nest. ``END IF`` and the other clause closers do
        not close the wrapper. Keywords inside quotes, comments, dollar
        bodies, or parentheses are not closers.
        """
        stack = ["BEGIN"]
        while True:
            keyword = self._next_top_keyword()
            if keyword is None:
                return False
            if keyword == "BEGIN":
                # BEGIN TRANSACTION / WORK / NAME / BEGIN; is a transaction
                # statement, not a block, so it has no matching END.
                if self._begin_opens_block():
                    stack.append("BEGIN")
            elif keyword == "CASE":
                stack.append("CASE")
            elif keyword == "END":
                follower = self._peek_kw()
                if follower in _END_CLAUSE:
                    self._consume_kw(follower)
                    continue
                kind = stack.pop()
                if not stack:
                    self._finish_terminator(remove_comments)
                    return True
                if kind == "CASE" and self._peek_kw() == "CASE":
                    self._consume_kw("CASE")

    def _finish_terminator(self, remove_comments: bool) -> None:
        mark = self.i
        if self._peek_kw() == "IGNORE":
            self._consume_kw("IGNORE")
            self._skip_noise()
            if self.i >= self.n or self.sql[self.i] == ";":
                if self.i < self.n:
                    self._consume_semicolon(remove_comments)
                return
            self.i = mark
        self._skip_noise()
        if self.i < self.n and self.sql[self.i] == ";":
            self._consume_semicolon(remove_comments)

    def _consume_semicolon(self, remove_comments: bool) -> None:
        self.i += 1
        if self.i < self.n and self.sql[self.i] == ">":
            self.i += 1
        if remove_comments:
            return
        line_end = self.sql.find("\n", self.i)
        if line_end < 0:
            line_end = self.n
        rest = self.sql[self.i : line_end]
        if _ONLY_SPACE.fullmatch(rest) or _TRAILING_COMMENT.match(rest):
            self.i = line_end
            if self.i < self.n and self.sql[self.i] == "\n":
                self.i += 1

    def _next_top_keyword(self) -> str | None:
        while self.i < self.n:
            self._skip_noise()
            if self.i >= self.n:
                return None
            if self._try_quote() or self._try_backtick() or self._try_dollar():
                continue
            ch = self.sql[self.i]
            if ch == "(":
                self.paren += 1
                self.i += 1
                continue
            if ch == ")":
                if self.paren > 0:
                    self.paren -= 1
                self.i += 1
                continue
            keyword = self._keyword_at_point()
            if keyword is not None and self.paren == 0:
                self.i += len(keyword)
                return keyword
            if _is_ident(ch):
                self._skip_ident()
            else:
                self.i += 1
        return None

    def advance(self) -> None:
        if self.i >= self.n:
            return
        if self.sql[self.i].isspace():
            self.i += 1
            return
        if (
            self._try_line_comment()
            or self._try_block_comment()
            or self._try_quote()
            or self._try_backtick()
            or self._try_dollar()
        ):
            return
        ch = self.sql[self.i]
        if ch == "(":
            self.paren += 1
            self.i += 1
            return
        if ch == ")":
            if self.paren > 0:
                self.paren -= 1
            self.i += 1
            return
        if _is_ident(ch):
            self._skip_ident()
            return
        self.i += 1

    def _consume_kw(self, word: str) -> bool:
        mark = self.i
        self._skip_noise()
        if self._keyword_at_point() == word:
            self.i += len(word)
            return True
        self.i = mark
        return False

    def _peek_kw(self) -> str | None:
        mark = self.i
        self._skip_noise()
        keyword = self._keyword_at_point()
        self.i = mark
        return keyword

    def _keyword_at_point(self) -> str | None:
        for word in _KEYWORDS:
            if self._matches(word):
                return word
        return None

    def _matches(self, word: str) -> bool:
        end = self.i + len(word)
        if end > self.n or self.sql[self.i : end].upper() != word:
            return False
        if self.i > 0 and _is_ident(self.sql[self.i - 1]):
            return False
        if end < self.n and (
            _is_ident(self.sql[end]) or self.sql.startswith("\\ ", end)
        ):
            return False
        return True

    def _skip_ident(self) -> None:
        # SFSqlLexer Identifier continues through "\ " (an escaped space).
        while self.i < self.n:
            if _is_ident(self.sql[self.i]):
                self.i += 1
                continue
            if self.sql.startswith("\\ ", self.i):
                self.i += 2
                continue
            return

    def _skip_noise(self) -> None:
        while self.i < self.n:
            if self.sql[self.i].isspace():
                self.i += 1
                continue
            if self._try_line_comment() or self._try_block_comment():
                continue
            return

    def _try_line_comment(self) -> bool:
        if self.sql.startswith("--", self.i) or (
            self.sql.startswith("//", self.i)
            and not (self.i > 0 and self.sql[self.i - 1] == ":")
        ):
            newline = self.sql.find("\n", self.i)
            self.i = self.n if newline < 0 else newline + 1
            return True
        return False

    def _try_block_comment(self) -> bool:
        # SFSqlLexer Comment and split_statements both stop at the first */.
        if not self.sql.startswith("/*", self.i):
            return False
        close = self.sql.find("*/", self.i + 2)
        self.i = self.n if close < 0 else close + 2
        return True

    def _begin_opens_block(self) -> bool:
        follower = self._peek_word()
        if follower in _BEGIN_STATEMENT:
            return False
        return not self._next_is_semicolon()

    def _peek_word(self) -> str | None:
        mark = self.i
        self._skip_noise()
        if self.i >= self.n or not _is_ident(self.sql[self.i]):
            self.i = mark
            return None
        start = self.i
        self._skip_ident()
        word = self.sql[start : self.i].upper()
        self.i = mark
        return word

    def _next_is_semicolon(self) -> bool:
        mark = self.i
        self._skip_noise()
        found = self.i < self.n and self.sql[self.i] == ";"
        self.i = mark
        return found

    def _try_quote(self) -> bool:
        if self.i >= self.n or self.sql[self.i] not in "'\"":
            return False
        quote = self.sql[self.i]
        self.i += 1
        while self.i < self.n:
            ch = self.sql[self.i]
            # StringLiteralSimple and split_statements treat \ as an escape.
            # QuotedString (a double-quoted name) does not: only "" continues it.
            if (
                quote == "'"
                and ch == "\\"
                and self.i + 1 < self.n
                and self.sql[self.i + 1] in (quote, "\\")
            ):
                self.i += 2
                continue
            if ch == quote:
                if self.i + 1 < self.n and self.sql[self.i + 1] == quote:
                    self.i += 2
                    continue
                self.i += 1
                return True
            self.i += 1
        return True

    def _try_backtick(self) -> bool:
        # SFSqlLexer Identifier: `RegexComponent+`
        if self.i >= self.n or self.sql[self.i] != "`":
            return False
        close = self.sql.find("`", self.i + 1)
        self.i = self.n if close < 0 else close + 1
        return True

    def _try_dollar(self) -> bool:
        # StringLiteralHere is only $$ ... $$. $body$ and $1$ are not strings.
        if not self.sql.startswith("$$", self.i):
            return False
        close = self.sql.find("$$", self.i + 2)
        if close < 0:
            return False
        self.i = close + 2
        return True


def _line_start(sql: str, floor: int, index: int) -> int:
    prev = max(sql.rfind("\n", floor, index), sql.rfind("\r", floor, index))
    if prev < floor:
        return floor
    return prev + 1


def _rewind_block_comment(sql: str, end: int) -> int | None:
    """``end`` is the index just after ``*/``. Return the matching ``/*``."""
    if end < 2 or sql[end - 2 : end] != "*/":
        return None
    depth = 1
    j = end - 2
    while j > 0 and depth:
        j -= 1
        if sql.startswith("*/", j):
            depth += 1
        elif sql.startswith("/*", j):
            depth -= 1
            if depth == 0:
                return j
    return None


def _rewind_trivia(sql: str, index: int, floor: int) -> int:
    """Include comments and whitespace that belong to the statement at ``index``."""
    i = index
    while i > floor:
        j = i
        while j > floor and sql[j - 1].isspace():
            j -= 1
        if j >= floor + 2 and sql[j - 2 : j] == "*/":
            start = _rewind_block_comment(sql, j)
            if start is not None and floor <= start < i:
                i = start
                continue
        if j < i and ("\n" in sql[j:i] or "\r" in sql[j:i]):
            line_start = _line_start(sql, floor, j)
            stripped = sql[line_start:j].lstrip()
            if line_start < i and (
                stripped.startswith("--")
                or (stripped.startswith("//") and not stripped.startswith("://"))
            ):
                i = line_start
                continue
        return max(j, floor)
    return i


def _inline_dcm_spans(sql: str, remove_comments: bool) -> list[tuple[int, int]]:
    scanner = _Scanner(sql)
    spans: list[tuple[int, int]] = []
    floor = 0
    while scanner.i < scanner.n:
        if scanner.at_execute():
            exec_at = scanner.i
            end = scanner.try_open(remove_comments)
            if end is not None:
                start = _rewind_trivia(sql, exec_at, floor)
                if start < end:
                    spans.append((start, end))
                    floor = end
                continue
        scanner.advance()
    return spans


def split_sql_statements(
    content: str, remove_comments: bool = False
) -> Iterator[tuple[str, bool | None]]:
    """Split ``content`` like ``split_statements``, keeping each DCM Lite block whole."""
    spans = _inline_dcm_spans(content, remove_comments)
    if not spans:
        yield from split_statements(
            io.StringIO(content), remove_comments=remove_comments
        )
        return

    position = 0
    for start, end in spans:
        if start > position:
            yield from split_statements(
                io.StringIO(content[position:start]),
                remove_comments=remove_comments,
            )
        block = content[start:end].strip()
        if block:
            yield block, False
        position = end
    if position < len(content):
        yield from split_statements(
            io.StringIO(content[position:]), remove_comments=remove_comments
        )


def is_inline_dcm_statement(sql: str) -> bool:
    """True when ``sql`` is one inline DCM script and nothing else."""
    text = sql.strip()
    if not text:
        return False
    spans = _inline_dcm_spans(text, remove_comments=False)
    if len(spans) != 1:
        return False
    start, end = spans[0]
    return not text[:start].strip() and not text[end:].strip()
