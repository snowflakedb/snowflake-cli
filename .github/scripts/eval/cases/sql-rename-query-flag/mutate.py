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

"""Breaking change: rename `snow sql --query/-q` to `--sql` (old flag removed)."""

from __future__ import annotations

from pathlib import Path

from mutation_helpers import replace_once

TARGETS = ["src/snowflake/cli/_plugins/sql/commands.py"]


def mutate(repo_root: Path) -> None:
    replace_once(
        repo_root,
        TARGETS[0],
        old='param_decls=["--query", "-q"],',
        new='param_decls=["--sql"],',
    )
