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
import os
import re
import shutil
import stat
from functools import reduce
from pathlib import Path

import pytest
import tomlkit
from snowflake.cli.api import secure_path
from snowflake.cli.api.config import config_init
from snowflake.cli.api.exceptions import DirectoryIsNotEmptyError, FileTooLargeError
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.app import loggers

from tests.conftest import clean_logging_handlers
from tests.testing_utils.files_and_dirs import assert_file_permissions_are_strict
from tests_common import IS_WINDOWS

if IS_WINDOWS:
    pytest.skip("Does not work on Windows", allow_module_level=True)


@pytest.fixture()
def save_logs(snowflake_home):
    config = snowflake_home / "config.toml"
    logs_path = snowflake_home / "logs"
    config_data = dict(
        cli=dict(
            logs=dict(
                save_logs=True,
                path=str(logs_path),
            )
        )
    )
    tomlkit.dump(config_data, config.open("w"))
    config_init(config)
    loggers.create_loggers(False, False)

    yield logs_path

    clean_logging_handlers()
    shutil.rmtree(logs_path)


@pytest.fixture()
def _widen_umask_for_testing():
    original_mask = os.umask(0o000)
    yield
    os.umask(original_mask)


def _read_logs(logs_path: Path) -> str:
    return next(logs_path.iterdir()).read_text()


def _assert_count_matching_logs(
    save_logs, expected_count, log_prefix, filename, log_suffix=""
):
    regex = rf"INFO \[snowflake\.cli\.api\.secure_path\] {log_prefix} \S+{filename}{log_suffix}"
    logs = _read_logs(save_logs).splitlines()
    count = sum(1 for line in logs if re.search(regex, line) is not None)
    assert count == expected_count


def test_read_text(temp_dir, save_logs):
    path = Path(temp_dir) / "file.txt"
    expected_result = "Noble Knight\n" * 1024
    path.write_text(expected_result)
    spath = SecurePath(path)
    assert spath.read_text(file_size_limit_mb=secure_path.UNLIMITED) == expected_result
    assert spath.read_text(file_size_limit_mb=100) == expected_result

    _assert_count_matching_logs(save_logs, 2, "Reading file", "file.txt")

    # too large file causes an error
    with pytest.raises(FileTooLargeError):
        spath.read_text(file_size_limit_mb=0)

    # not existing file causes an error
    with pytest.raises(FileNotFoundError):
        (SecurePath(temp_dir) / "not_a_file.txt").read_text(file_size_limit_mb=100)

    # "opening" directory causes an error
    with pytest.raises(IsADirectoryError):
        SecurePath(save_logs).read_text(file_size_limit_mb=100)


def test_write_text(temp_dir, save_logs, _widen_umask_for_testing):
    # not existing file
    path = Path(temp_dir) / "file.txt"
    text = "What say you, noble knight?"
    SecurePath(path).write_text(text)
    assert path.read_text() == text
    assert_file_permissions_are_strict(path)
    _assert_count_matching_logs(save_logs, 1, "Creating file", path.name)
    _assert_count_matching_logs(save_logs, 1, "Writing to file", path.name)

    # existing file permissions are not overwritten
    path.chmod(0o777)
    SecurePath(path).write_text(text * 2)
    assert path.read_text() == text * 2
    _assert_count_matching_logs(save_logs, 1, "Creating file", path.name)
    _assert_count_matching_logs(save_logs, 2, "Writing to file", path.name)
    writable_and_readable_by_group = stat.S_IRGRP | stat.S_IWGRP
    assert (
        path.stat().st_mode & writable_and_readable_by_group
        == writable_and_readable_by_group
    )


def test_open_write(temp_dir, save_logs, _widen_umask_for_testing):
    path = SecurePath(temp_dir) / "file.txt"
    with path.open("w") as fd:
        # permissions are limited on freshly-created file
        assert_file_permissions_are_strict(path.path)
        fd.write("Merlin")
    assert_file_permissions_are_strict(path.path)
    _assert_count_matching_logs(
        save_logs, 1, "Opening file", "file.txt", " in mode 'w'"
    )
    _assert_count_matching_logs(save_logs, 1, "Closing file", "file.txt")


def test_open_read(temp_dir, save_logs):
    path = Path(temp_dir) / "file.txt"
    path.write_text("You play dirty noble knight.")

    with SecurePath(path).open("r", read_file_limit_mb=10) as fd:
        assert fd.read() == "You play dirty noble knight."
    with SecurePath(path).open("r", read_file_limit_mb=secure_path.UNLIMITED) as fd:
        assert fd.read() == "You play dirty noble knight."

    _assert_count_matching_logs(
        save_logs, 2, "Opening file", "file.txt", " in mode 'r'"
    )
    _assert_count_matching_logs(save_logs, 2, "Closing file", "file.txt")

    # too large file causes an error
    with pytest.raises(FileTooLargeError):
        with SecurePath(path).open("r", read_file_limit_mb=0):
            pass

    # not existing file causes an error
    with pytest.raises(FileNotFoundError):
        not_existing_path = SecurePath(temp_dir) / "not_a_file.txt"
        with not_existing_path.open("r", read_file_limit_mb=100):
            pass

    # "opening" directory causes an error
    with pytest.raises(IsADirectoryError):
        with SecurePath(save_logs).open("r", read_file_limit_mb=100):
            pass


def test_navigation():
    p = SecurePath("a/b/c")
    pathstring = reduce(os.path.join, ["a", "b", "c", "b", "c", "d", "e"])
    assert str(p / "b" / "c" / "d" / "e") == f'SecurePath("{pathstring}")'
    assert str(p.parent.parent) == 'SecurePath("a")'
    assert type(p.absolute()) is SecurePath


def test_iterdir(temp_dir):
    for d in "abcde":
        (Path(temp_dir) / "dir" / d).mkdir(parents=True)
    counter = 0
    for file in (SecurePath(temp_dir) / "dir").iterdir():
        assert type(file) is SecurePath
        counter += 1
    assert counter == 5


def test_default_permissions(temp_dir, _widen_umask_for_testing):
    s_temp_dir = SecurePath(temp_dir)
    # test default permissions
    file1 = s_temp_dir / "file1.txt"
    file1.touch()
    assert_file_permissions_are_strict(file1.path)


def test_permissions(temp_dir, save_logs):
    s_temp_dir = SecurePath(temp_dir)
    # test default permissions
    file1 = s_temp_dir / "file1.txt"
    file1.touch()
    assert_file_permissions_are_strict(file1.path)

    # permissions cannot be widened by touch() due to os.umask
    file2 = s_temp_dir / "file2.txt"
    file2.touch(permissions_mask=0o660)
    assert_file_permissions_are_strict(file2.path)
    # but can be widened using chmod
    file2.chmod(permissions_mask=0o660)
    writable_and_readable_by_group = stat.S_IRGRP | stat.S_IWGRP
    assert (
        file2.path.stat().st_mode & writable_and_readable_by_group
        == writable_and_readable_by_group
    )

    with pytest.raises(FileExistsError):
        file1.touch(exist_ok=False)

    _assert_count_matching_logs(save_logs, 1, "Creating file", "file1.txt")
    _assert_count_matching_logs(save_logs, 1, "Creating file", "file2.txt")
    _assert_count_matching_logs(
        save_logs, 1, "Update permissions of file", "file2.txt", " to 0o660"
    )


def test_mkdir(temp_dir, save_logs, _widen_umask_for_testing):
    dir1 = SecurePath(temp_dir) / "dir1"
    dir2 = SecurePath(temp_dir) / "dir2" / "a" / "b" / "c" / "d"
    dir2_regex = r"[\/]".join(["dir2", "a", "b", "c", "d"])

    dir1.mkdir()
    _assert_count_matching_logs(save_logs, 1, "Creating directory", "dir1")
    assert_file_permissions_are_strict(dir1.path)

    with pytest.raises(FileExistsError):
        dir1.mkdir()

    dir1.mkdir(exist_ok=True)
    _assert_count_matching_logs(save_logs, 1, "Creating directory", "dir1")

    with pytest.raises(FileNotFoundError):
        dir2.mkdir()
    _assert_count_matching_logs(save_logs, 1, "Creating directory", dir2_regex)

    dir2.mkdir(parents=True)
    _assert_count_matching_logs(save_logs, 2, "Creating directory", dir2_regex)
    while dir2.path != Path(temp_dir):
        assert_file_permissions_are_strict(dir2.path)
        dir2 = dir2.parent


def test_move(temp_dir, save_logs):
    def _get_new_file():
        file = Path(temp_dir) / "file.txt"
        file.touch()
        return file

    def _get_new_dir():
        dir_ = Path(temp_dir) / "dir"
        (dir_ / "subdir").mkdir(parents=True)
        (dir_ / "empty").mkdir()
        (dir_ / "file1.txt").touch()
        (dir_ / "subdir" / "file2.txt").touch()
        return dir_

    def _check_dir_moved(dst):
        for filename in ["file1.txt", "empty", "subdir/file2.txt"]:
            path = Path(dst) / filename
            assert path.exists()
            if filename.endswith(".txt"):
                assert path.is_file()
            else:
                assert path.is_dir()

    # move file
    file = SecurePath(_get_new_file())
    moved_file = file.move("moved_file.txt")
    assert moved_file.path.exists() and not file.exists()
    assert moved_file.path == Path("moved_file.txt")
    _assert_count_matching_logs(
        save_logs, 1, "Moving", "file.txt", r" to \S+moved_file.txt"
    )

    file = SecurePath(_get_new_file())
    with pytest.raises(FileExistsError):
        file.move("moved_file.txt")
    assert file.exists()

    dir_ = _get_new_dir()
    moved_file = file.move(dir_)
    assert moved_file.exists() and not file.exists()
    assert moved_file.path.resolve() == (Path("dir") / "file.txt").resolve()
    _assert_count_matching_logs(
        save_logs, 1, "Moving", "file.txt", r" to \S+dir[\/]file.txt"
    )

    file = SecurePath(_get_new_file())
    with pytest.raises(FileExistsError):
        file.move(dir_)
    assert file.exists()

    # moving directory
    dir_ = SecurePath(dir_)
    moved_dir = dir_.move("moved_dir")
    assert moved_dir.exists() and not dir_.exists()
    assert moved_dir.path == Path("moved_dir")
    _check_dir_moved("moved_dir")
    _assert_count_matching_logs(save_logs, 1, "Moving", "dir", r" to \S+moved_dir")

    dir_ = SecurePath(_get_new_dir())
    moved_dir = dir_.move("moved_dir")
    assert moved_dir.exists() and not dir_.exists()
    assert moved_dir.path == Path("moved_dir") / "dir"
    _check_dir_moved(Path("moved_dir") / "dir")
    _assert_count_matching_logs(
        save_logs, 1, "Moving", "dir", r" to \S+moved_dir[\/]dir"
    )

    dir_ = SecurePath(_get_new_dir())
    with pytest.raises(FileExistsError):
        dir_.move("moved_dir")


def test_copy_file(temp_dir, save_logs, _widen_umask_for_testing):
    file = SecurePath(temp_dir) / "file.txt"
    file.touch()
    file.chmod(permissions_mask=0o660)

    # copy into file
    dest = Path(temp_dir) / "file.copy.txt"
    file.copy(dest)
    assert dest.exists()
    # copying should restrict permissions
    assert_file_permissions_are_strict(dest)

    # copy into directory
    dest = Path(temp_dir) / "a_directory"
    dest.mkdir()
    dest.chmod(0o771)
    copied_file = file.copy(dest)
    assert copied_file.path.exists()
    assert_file_permissions_are_strict(copied_file.path)

    _assert_count_matching_logs(
        save_logs, 1, "Copying file", "file.txt", r" into \S+file.copy.txt"
    )
    _assert_count_matching_logs(
        save_logs, 1, "Copying file", "file.txt", r" into \S+a_directory[\/]file.txt"
    )


def test_copy_directory(temp_dir, save_logs, _widen_umask_for_testing):
    files = [
        "dir/",
        "dir/file1.txt",
        "dir/file2.txt",
        "dir/subdir/",
        "dir/subdir/subfile1.txt",
        "dir/subdir/subfile2.txt",
        "dir/subdir/subsubdir/",
        "dir/subdir/subsubdir/deep.file.txt",
        "dir/emptydir/",
    ]

    def _is_dummy_file(filename):
        return filename.endswith(".txt")

    for file in files:
        path = Path(temp_dir) / file
        if _is_dummy_file(file):
            path.write_text("Quite a content")
            path.chmod(0o666)
        else:
            path.mkdir()
            path.chmod(0o771)

    src = SecurePath(temp_dir) / "dir"

    # argument is non-existing directory
    dest = Path(temp_dir) / "copydir"
    src.copy(dest)
    for newfile in ["copy" + f for f in files]:
        path = Path(temp_dir) / newfile
        assert path.exists()
        # restricted permissions of files and directory structure
        assert_file_permissions_are_strict(path)

    _assert_count_matching_logs(save_logs, 5, "Copying file", ".txt")
    _assert_count_matching_logs(save_logs, 4, "Creating directory", "")

    # argument is existing directory
    dest.chmod(0o771)
    src.copy(dest)
    for file in files:
        path = dest / file
        assert path.exists()
        # restricted permissions of files and directory structure
        assert_file_permissions_are_strict(path)

    _assert_count_matching_logs(save_logs, 10, "Copying file", ".txt")
    _assert_count_matching_logs(save_logs, 8, "Creating directory", "")


def test_copy_dir_onto_existing_dir(temp_dir, save_logs):
    dir_, file = "DIR", "FILE"
    original = [
        (dir_, "dir"),
        (file, "dir/subfile"),
        (dir_, "dir/subdir"),
        (file, "dir/subdir/subfile"),
        (dir_, "dir/newdir/"),
    ]
    file_instead_of_dir = [
        (dir_, "dir"),
        (file, "dir/subfile"),
        (file, "dir/subdir"),
    ]
    dir_instead_of_file = [
        (dir_, "dir"),
        (dir_, "dir/subfile"),
        (dir_, "dir/subdir"),
        (file, "dir/subdir/subfile"),
    ]
    proper_destination = [
        (dir_, "dir"),
        (file, "dir/subfile"),
        (dir_, "dir/subdir"),
        (file, "dir/subdir/subfile2"),
    ]
    expected_result = [
        (dir_, "dir"),
        (file, "dir/subfile"),
        (dir_, "dir/subdir"),
        (file, "dir/subdir/subfile"),
        (file, "dir/subdir/subfile2"),
        (dir_, "dir/newdir/"),
    ]
    should_be_overridden = [("dir/subfile"), ("dir/subdir/subfile"), ("dir/newdir/")]

    def _check_result_tree(root):
        readable_by_group = stat.S_IRGRP

        for filetype, filename in expected_result:
            file = root / filename
            assert file.exists()
            if filename in should_be_overridden:
                assert_file_permissions_are_strict(root / filename)
                if filetype == file:
                    assert file.read_text() == "new"
            else:
                assert file.stat().st_mode & readable_by_group == readable_by_group
                if filetype == file:
                    assert file.read_text() == "old"

    def _create_tree(root, tree, file_content):
        for filetype, filename in tree:
            if filetype == dir_:
                (root / filename).mkdir(parents=True)
                (root / filename).chmod(0o750)
            else:
                (root / filename).write_text(file_content)
                (root / filename).chmod(0o660)

    tmpdir = Path(temp_dir)
    _create_tree(tmpdir / "original", original, file_content="new")
    src = SecurePath(tmpdir / "original" / "dir")

    _create_tree(
        tmpdir / "file_instead_of_dir", file_instead_of_dir, file_content="old"
    )
    _create_tree(
        tmpdir / "dir_instead_of_file", dir_instead_of_file, file_content="old"
    )
    _create_tree(tmpdir / "good1", proper_destination, file_content="old")
    _create_tree(tmpdir / "good2", proper_destination, file_content="old")

    with pytest.raises(NotADirectoryError):
        src.copy(tmpdir / "file_instead_of_dir", dirs_exist_ok=True)
    with pytest.raises(IsADirectoryError):
        src.copy(tmpdir / "dir_instead_of_file", dirs_exist_ok=True)
    with pytest.raises(FileExistsError):
        src.copy(tmpdir / "good1")

    src.copy(tmpdir / "good1", dirs_exist_ok=True)
    src.copy(tmpdir / "good2" / "dir", dirs_exist_ok=True)

    _check_result_tree(tmpdir / "good1")
    _check_result_tree(tmpdir / "good2")


def test_rm(temp_dir, save_logs):
    temp_dir = Path(temp_dir)

    # removing file
    file = Path(temp_dir) / "file.txt"
    file.touch()
    with pytest.raises(NotADirectoryError):
        SecurePath(file).rmdir()

    SecurePath(file).unlink()
    _assert_count_matching_logs(save_logs, 1, "Removing file", "file.txt")

    with pytest.raises(FileNotFoundError):
        SecurePath(file).unlink()

    SecurePath(file).unlink(missing_ok=True)
    _assert_count_matching_logs(save_logs, 1, "Removing file", "file.txt")

    # removing a directory
    base_dir = Path(temp_dir) / "base"
    full_dir = base_dir / "full"
    empty_dir = full_dir / "empty"
    empty_dir.mkdir(parents=True)

    with pytest.raises(DirectoryIsNotEmptyError):
        SecurePath(full_dir).rmdir()
    SecurePath(full_dir).rmdir(recursive=True)
    _assert_count_matching_logs(save_logs, 1, "Removing directory", r"base[\/]full")

    with pytest.raises(FileNotFoundError):
        SecurePath(full_dir).rmdir(recursive=True)

    with pytest.raises(IsADirectoryError):
        SecurePath(base_dir).unlink()

    SecurePath(base_dir).rmdir()
    _assert_count_matching_logs(save_logs, 2, "Removing directory", "base")

    SecurePath(base_dir).rmdir(missing_ok=True)
    _assert_count_matching_logs(save_logs, 2, "Removing directory", "base")


def test_temporary_directory(save_logs, _widen_umask_for_testing):
    with SecurePath.temporary_directory() as sdir:
        _assert_count_matching_logs(save_logs, 1, "Created temporary directory", "")

        assert type(sdir) is SecurePath
        assert_file_permissions_are_strict(sdir.path)

        file = sdir / "file.txt"
        file.touch()
        assert_file_permissions_are_strict(file.path)

        directory = sdir / "directory"
        directory.mkdir()
        assert_file_permissions_are_strict(directory.path)

        temp_path = sdir
    assert not temp_path.exists()
    _assert_count_matching_logs(save_logs, 1, "Removing temporary directory", "")


def test_file_size_limit_calculation(temp_dir):
    a_file = Path(temp_dir) / "a_file.txt"

    # should work
    a_file.write_bytes(b"x" * 1024 * 900)  # ~900 KB
    SecurePath(a_file).read_text(file_size_limit_mb=1)
    # should rise
    a_file.write_bytes(b"x" * 1024 * 1200)  # ~1.2 MB
    with pytest.raises(FileTooLargeError):
        SecurePath(a_file).read_text(file_size_limit_mb=1)
