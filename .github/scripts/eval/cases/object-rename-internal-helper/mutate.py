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

"""False-positive trap: rename the private `_scope_validate` helper.

A pure internal rename (definition + call site); no behavior change (expect PASS).
"""

from __future__ import annotations

from pathlib import Path

from mutation_helpers import rename_symbol

TARGETS = ["src/snowflake/cli/_plugins/object/commands.py"]


def mutate(repo_root: Path) -> None:
    rename_symbol(repo_root, TARGETS[0], old="_scope_validate", new="_validate_scope")
