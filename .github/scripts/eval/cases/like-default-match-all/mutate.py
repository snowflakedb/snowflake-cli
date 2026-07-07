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

"""False-positive trap: change the shared `--like` default "%%" -> "%".

Looks like a breaking default-filter change across many list commands, but both
patterns match everything in SQL LIKE, so behavior is unchanged (expect PASS).
"""

from __future__ import annotations

from pathlib import Path

from mutation_helpers import replace_once

TARGETS = ["src/snowflake/cli/api/commands/flags.py"]


def mutate(repo_root: Path) -> None:
    replace_once(
        repo_root,
        TARGETS[0],
        old='    return typer.Option(\n        "%%",\n        "--like",',
        new='    return typer.Option(\n        "%",\n        "--like",',
    )
