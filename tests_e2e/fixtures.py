import shutil
from contextlib import contextmanager
from pathlib import Path

import pytest


@pytest.fixture
def project_directory(temp_dir, test_root_path):
    @contextmanager
    def _temporary_project_directory(project_name):
        test_data_file = test_root_path / "test_data" / project_name
        shutil.copytree(test_data_file, temp_dir, dirs_exist_ok=True)
        yield Path(temp_dir)

    return _temporary_project_directory
