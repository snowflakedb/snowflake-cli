import os

from syrupy import SnapshotAssertion


def assert_that_current_working_directory_contains_only_following_files(
    *filenames: str,
) -> None:
    assert set(os.listdir()) == set(filenames)


def assert_that_file_content_is_equal_to_snapshot(
    actual_file_path: str,
    snapshot: SnapshotAssertion,
) -> None:
    with open(actual_file_path, "r") as actual_file:
        actual_content = actual_file.read()
    snapshot.assert_match(actual_content)
