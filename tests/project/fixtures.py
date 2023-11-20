import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List

import pytest

PROJECT_DIR = Path(__file__).parent.parent / "test_data" / "projects"


@contextmanager
def temp_cloned_dir(path: Path) -> Generator[Path, None, None]:
    """
    Returns a temporary copy of the directory structure at the given path.
    This file will be deleted when this context manager goes out-of-scope.
    """
    if not path.is_dir():
        raise ValueError("temp_cloned_dir requires a directory")

    with tempfile.TemporaryDirectory(suffix=f"_{path.name}") as tmpdir:
        shutil.copytree(path, tmpdir, symlinks=True, dirs_exist_ok=True)
        yield Path(tmpdir)


@contextmanager
def snowflake_ymls(dir_name: str) -> Generator[List[Path], None, None]:
    """
    Returns paths to [snowflake_yml, (snowflake_local_yml)].
    These files are temporary copies of the project definition found in
    dir_name and will be deleted when this context manager goes out-of-scope.
    These files reside alongside any other files in the project directory.
    If there is no local overrides file, returns a list of length 1.
    """
    with temp_cloned_dir(PROJECT_DIR / dir_name) as dir:
        project_yml = dir / "snowflake.yml"
        local_yml = dir / "snowflake.local.yml"
        if local_yml.exists():
            yield [project_yml, local_yml]
        else:
            yield [project_yml]


@pytest.fixture
def project_definition_files(request) -> Generator[List[Path], None, None]:
    """
    Expects indirect parameterization, e.g.
    @pytest.mark.parametrize("project_definition_files", ["project_1"], indirect=True)
    def test_my_project(project_definition_files):
        project = load_project_definition(project_definition_files)
    """
    dir_name = request.param
    with snowflake_ymls(dir_name) as ymls:
        yield ymls
