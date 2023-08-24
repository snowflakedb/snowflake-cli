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
    with tempfile.NamedTemporaryFile(suffix=path.name, mode="w+") as fh:
        fh.write(open(path, 'r').read())
        fh.flush()
        yield Path(fh.name)

@contextmanager
def with_project_context(dir_name: str):
    """
    Returns paths to [project_yml, local_yml].
    """
    with temporary_of(PROJECT_DIR / dir_name / "project.yml") as project_yml:
        with temporary_of(PROJECT_DIR / dir_name / "local.yml") as local_yml:
            yield [project_yml, local_yml]

@pytest.fixture
def with_project_1():
    with with_project_context("project_1") as abc:
        yield abc
