import stat

import pytest

from snowflake.cli.api import secure_path
from snowflake.cli.api.exceptions import FileTooLargeError
from snowflake.cli.api.secure_path import SecurePath
from pathlib import Path
from snowflake.cli.api.config import config_init
from snowflake.cli.app import loggers

from tests.testing_utils.files_and_dirs import assert_file_permissions_are_strict

import shutil


@pytest.fixture()
def save_logs(snowflake_home):
    config = snowflake_home / "config.toml"
    logs_path = snowflake_home / "logs"
    logs_path.mkdir()
    config.write_text(
        "\n".join(["[cli.logs]", "save_logs = true", f'path = "{logs_path}"'])
    )
    config_init(config)
    loggers.create_loggers(False, False)

    yield logs_path

    shutil.rmtree(logs_path)


def _read_logs(logs_path: Path) -> str:
    return next(logs_path.iterdir()).read_text()


def test_read_text(temp_dir, save_logs):
    path = Path(temp_dir) / "file.txt"
    expected_result = "Noble Knight\n" * 1024
    path.write_text(expected_result)
    spath = SecurePath(path)
    assert spath.read_text(file_size_limit_kb=secure_path.UNLIMITED) == expected_result
    assert spath.read_text(file_size_limit_kb=100) == expected_result

    logs = _read_logs(save_logs)
    assert logs.count("INFO [snowflake.cli.api.secure_path] Reading file") == 2

    # too large file causes an error
    with pytest.raises(FileTooLargeError):
        spath.read_text(file_size_limit_kb=10)

    # not existing file causes an error
    with pytest.raises(FileNotFoundError):
        (SecurePath(temp_dir) / "not_a_file.txt").read_text(file_size_limit_kb=100)

    # "opening" directory causes an error
    with pytest.raises(IsADirectoryError):
        SecurePath(save_logs).read_text(file_size_limit_kb=100)


def test_open_write(temp_dir, save_logs):
    path = SecurePath(temp_dir) / "file.txt"
    with path.open("w") as fd:
        # permissions are limited on freshly-created file
        assert_file_permissions_are_strict(path.path)
        fd.write("Merlin")
    assert_file_permissions_are_strict(path.path)
    logs = _read_logs(save_logs)
    assert "INFO [snowflake.cli.api.secure_path] Opening file" in logs
    assert "INFO [snowflake.cli.api.secure_path] Closing file" in logs


def test_open_read(temp_dir, save_logs):
    path = Path(temp_dir) / "file.txt"
    path.write_text("You play dirty noble knight.")

    with SecurePath(path).open("r", read_file_limit_kb=100) as fd:
        assert fd.read() == "You play dirty noble knight."
    with SecurePath(path).open("r", read_file_limit_kb=secure_path.UNLIMITED) as fd:
        assert fd.read() == "You play dirty noble knight."

    logs = _read_logs(save_logs)
    assert logs.count("INFO [snowflake.cli.api.secure_path] Opening file") == 2
    assert logs.count("INFO [snowflake.cli.api.secure_path] Closing file") == 2

    # too large file causes an error
    with pytest.raises(FileTooLargeError):
        with SecurePath(path).open("r", read_file_limit_kb=0):
            pass

    # not existing file causes an error
    with pytest.raises(FileNotFoundError):
        not_existing_path = SecurePath(temp_dir) / "not_a_file.txt"
        with not_existing_path.open("r", read_file_limit_kb=100):
            pass

    # "opening" directory causes an error
    with pytest.raises(IsADirectoryError):
        with SecurePath(save_logs).open("r", read_file_limit_kb=100):
            pass


def test_navigation():
    p = SecurePath("a/b/c")
    assert str(p / "b" / "c" / "d" / "e") == 'SecurePath("a/b/c/b/c/d/e")'
    assert str(p.parent.parent) == 'SecurePath("a")'


def test_permissions(temp_dir, save_logs):
    s_temp_dir = SecurePath(temp_dir)
    # test default permissions
    file1 = s_temp_dir / "file1.txt"
    file1.touch()
    assert_file_permissions_are_strict(file1.path)

    # permissions cannot be widened by touch() due to os.umask
    file2 = s_temp_dir / "file2.txt"
    file2.touch(permissions_mask=0o600)
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

    logs = _read_logs(save_logs)
    assert logs.count("INFO [snowflake.cli.api.secure_path] Creating file") == 2
    assert (
        logs.count("INFO [snowflake.cli.api.secure_path] Update permissions of file")
        == 1
    )
    assert "file2.txt to 0o660" in logs
    assert logs.count("file1.txt") == 1 and logs.count("file2.txt") == 2


def test_copy_file(temp_dir, save_logs):
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

    logs = _read_logs(save_logs)
    assert logs.count("INFO [snowflake.cli.api.secure_path] Copying ") == 2


def test_copy_directory(temp_dir, save_logs):
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

    # argument is existing directory
    dest.chmod(0o771)
    src.copy(dest)
    for file in files:
        path = dest / file
        assert path.exists()
        # restricted permissions of files and directory structure
        assert_file_permissions_are_strict(path)

    logs = _read_logs(save_logs)
    assert logs.count("INFO [snowflake.cli.api.secure_path] Copying ") == 2
