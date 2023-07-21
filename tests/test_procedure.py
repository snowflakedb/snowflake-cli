import os
from pathlib import Path
from shutil import rmtree

import pytest


class TestProcedure:
    TEMP_DIRECTORY_NAME = "test_procedure_tmp"

    def test_procedure_init(self):
        pass

    @pytest.fixture()
    def temp_directory(self):
        current_path = Path(os.getcwd())
        path = os.path.join(current_path, self.TEMP_DIRECTORY_NAME)
        os.mkdir(path)
        yield path
        rmtree(path)