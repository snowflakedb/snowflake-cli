import os
import pytest
import tempfile

from typing import Generator
from pathlib import Path

from contextlib import contextmanager

REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"
PROJECT_DIR = Path(__file__).parent


@contextmanager
def temporary_of(path: Path):
    """
    Returns a temporary copy of file at the given path.
    This file will be deleted when this context manager goes out-of-scope.
    """
    with tempfile.NamedTemporaryFile(suffix=path.name, mode="w+") as fh:
        fh.write(open(path, "r").read())
        fh.flush()
        yield Path(fh.name)


@contextmanager
def with_project_context(dir_name: str):
    """
    Returns paths to [project_yml, local_yml].
    These files are temporary copies of the project config found in dir_name
    and will be deleted when this context manager goes out-of-scope.
    """
    with temporary_of(PROJECT_DIR / dir_name / "project.yml") as project_yml:
        with temporary_of(PROJECT_DIR / dir_name / "local.yml") as local_yml:
            yield [project_yml, local_yml]


@pytest.fixture
def project_context(request):
    """
    Expects indirect parameterization, e.g.
    @pytest.mark.parametrize("project_context", ["project_1"], indirect=True)
    def test_my_project(project_context):
        [project_yml, local_yml] = project_context
    """
    print(request.param)
    project_dir = request.param
    with with_project_context(project_dir) as ctx:
        yield ctx
