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

from rich.tree import Tree


class CompactTree(Tree):
    """A tree whose guides are three columns wide instead of Rich's four.

    ``Tree.__rich_measure__`` indents four columns per level whatever the guides
    are, so a compact tree reports one column per level more than it draws. Left
    that way knowingly: the overestimate only ever pads - a caller sizing a
    container from it gets trailing whitespace - and never clips the tree.
    """

    ASCII_GUIDES = ("   ", "|  ", "+- ", "`- ")
    TREE_GUIDES = [
        ("   ", "│  ", "├─ ", "└─ "),
        ("   ", "┃  ", "┣━ ", "┗━ "),
        ("   ", "║  ", "╠═ ", "╚═ "),
    ]
