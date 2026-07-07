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

"""No-op: add a comment above `snow object list` — no CLI behavior change (expect SKIP)."""

from __future__ import annotations

from pathlib import Path

from mutation_helpers import insert_line_before

TARGETS = ["src/snowflake/cli/_plugins/object/commands.py"]


def mutate(repo_root: Path) -> None:
    insert_line_before(
        repo_root,
        TARGETS[0],
        anchor="def list_(",
        new_line="# Lists objects of the requested type (see help for supported types).",
    )
