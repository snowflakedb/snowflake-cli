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
from unittest.mock import patch

import pytest
from snowflake.cli._plugins.dcm.glyphs import (
    _RUNNING_FRAME,
    arrow,
    ascii_only,
    bullet,
    ellipsis,
    fail_mark,
    pass_mark,
    pending_mark,
    running_mark,
    step_prefix,
    tree_leaf,
    warning_mark,
)

from tests.dcm.encoding_console import encoded_console

_MARKS = (
    (warning_mark, "⚠️", "!"),
    (pass_mark, "✓", "v"),
    (fail_mark, "✗", "x"),
    (tree_leaf, "└─", "`-"),
    (bullet, "• ", "* "),
    (ellipsis, "…", "..."),
    (arrow, "→", "->"),
    (pending_mark, "·", "."),
    (running_mark, _RUNNING_FRAME, "*"),
    (step_prefix, "❯", ">"),
)


class TestGlyphs:
    @pytest.mark.parametrize("encoding", ["utf-8", "utf-16"])
    def test_utf_encodings_keep_unicode(self, encoding: str) -> None:
        # given
        console = encoded_console(encoding)

        # when / then
        for mark, unicode_form, _ascii_form in _MARKS:
            assert mark(console) == unicode_form
        assert not ascii_only(console)

    def test_cp1252_uses_ascii(self) -> None:
        # given
        console = encoded_console("cp1252")

        # when / then
        for mark, _unicode_form, ascii_form in _MARKS:
            assert mark(console) == ascii_form
            ascii_form.encode("ascii")
        assert ascii_only(console)

    def test_none_console_reads_get_console_at_call_time(self) -> None:
        # given
        utf = encoded_console("utf-8")
        cp1252 = encoded_console("cp1252")

        # when / then
        with patch("snowflake.cli._plugins.dcm.glyphs.get_console", return_value=utf):
            assert step_prefix() == "❯"
        with patch(
            "snowflake.cli._plugins.dcm.glyphs.get_console", return_value=cp1252
        ):
            assert step_prefix() == ">"
