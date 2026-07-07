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

"""False-positive trap: refactor a git path-splitting helper by extracting a local.

`_split_path_without_empty_parts` is rewritten to pull `PurePosixPath(path).parts`
into a local variable — a behavior-identical refactor that touches path-splitting
logic, so it can look risky in the diff (expect PASS). Uses a single-line anchor to
stay robust against unrelated churn in the surrounding function.
"""

from __future__ import annotations

from pathlib import Path

from mutation_helpers import replace_once

TARGETS = ["src/snowflake/cli/_plugins/git/manager.py"]


def mutate(repo_root: Path) -> None:
    replace_once(
        repo_root,
        TARGETS[0],
        old='        return [e for e in PurePosixPath(path).parts if e != "/"]',
        new=(
            "        parts = PurePosixPath(path).parts\n"
            '        return [e for e in parts if e != "/"]'
        ),
    )
