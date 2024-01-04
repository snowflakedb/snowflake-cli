from __future__ import annotations

import os
from pathlib import Path
from typing import List

from syrupy import SnapshotAssertion


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
