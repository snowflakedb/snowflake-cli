from __future__ import annotations

import os
from pathlib import Path

from syrupy import SnapshotAssertion


def assert_that_current_working_directory_contains_only_following_files(
    *filenames: str | Path,
) -> None:
    assert set(f for f in Path(".").glob("**/*")) == set(filenames)
