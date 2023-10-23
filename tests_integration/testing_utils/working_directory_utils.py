import contextlib
import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest


class WorkingDirectoryChanger:
    def __init__(self):
        self._initial_working_directory = os.getcwd()

    @staticmethod
    def change_working_directory_to(directory: str):
        os.chdir(directory)

    def restore_initial_working_directory(self):
        self.change_working_directory_to(self._initial_working_directory)


@pytest.fixture
def temporary_working_directory():
    working_directory_changer = WorkingDirectoryChanger()
    with TemporaryDirectory() as tmp_dir:
        working_directory_changer.change_working_directory_to(tmp_dir)
        yield Path(tmp_dir)
        working_directory_changer.restore_initial_working_directory()


@pytest.fixture
def temporary_working_directory_ctx():
    @contextlib.contextmanager
    def _ctx_manager():
        working_directory_changer = WorkingDirectoryChanger()
        with TemporaryDirectory() as tmp_dir:
            working_directory_changer.change_working_directory_to(tmp_dir)
            yield Path(tmp_dir)
            working_directory_changer.restore_initial_working_directory()

    return _ctx_manager
