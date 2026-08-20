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

from io import StringIO
from typing import List

import pytest
from rich.console import Console
from rich.style import Style
from rich.text import Text
from rich.tree import Tree
from snowflake.cli._plugins.dcm.tree import CompactTree


def _rendered_lines(guide_style: Style) -> List[str]:
    tree = CompactTree(Text("root"), guide_style=guide_style)
    tree.add(Text("branch")).add(Text("leaf"))
    console = Console(file=StringIO(), width=40, no_color=True, legacy_windows=False)
    console.print(tree, soft_wrap=False)
    return [
        line.rstrip() for line in console.file.getvalue().split("\n") if line.strip()
    ]


class TestCompactTreeGuides:
    @pytest.mark.parametrize(
        "guide_style, expected",
        [
            (Style(dim=True), ["root", "└─ branch", "   └─ leaf"]),
            (Style(bold=True), ["root", "┗━ branch", "   ┗━ leaf"]),
            (Style(underline2=True), ["root", "╚═ branch", "   ╚═ leaf"]),
        ],
        ids=["light", "heavy", "double"],
    )
    def test_guide_style_selects_the_same_variant_rich_would(
        self, guide_style: Style, expected: List[str]
    ) -> None:
        """Rich reads ``bold``/``underline2`` off the guide style to pick a glyph
        set, so every variant needs its own narrowed tuple."""
        assert _rendered_lines(guide_style) == expected

    def test_every_variant_is_rich_s_own_glyphs_one_column_narrower(self) -> None:
        assert len(CompactTree.TREE_GUIDES) == len(Tree.TREE_GUIDES)
        for ours, theirs in zip(CompactTree.TREE_GUIDES, Tree.TREE_GUIDES):
            assert len(ours) == len(theirs)
            for narrow, wide in zip(ours, theirs):
                assert len(narrow) == 3
                assert len(wide) == 4
                assert set(narrow) <= set(wide)

    def test_ascii_fallback_is_also_three_columns(self) -> None:
        assert all(len(guide) == 3 for guide in CompactTree.ASCII_GUIDES)
        assert set("".join(CompactTree.ASCII_GUIDES)) <= set("".join(Tree.ASCII_GUIDES))
