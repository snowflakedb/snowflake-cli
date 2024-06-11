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

from pathlib import Path
from typing import List


def assert_that_current_working_directory_contains_only_following_files(
    *filenames: str | Path, excluded_paths: List[str] | None
) -> None:
    if excluded_paths is None:
        excluded_paths = []

    assert set(
        f
        for f in Path(".").glob("**/*")
        if not any(part in f.parts for part in excluded_paths)
    ) == set(filenames)
