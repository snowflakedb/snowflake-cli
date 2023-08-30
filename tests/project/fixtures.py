import pytest
import tempfile
from pathlib import Path
from unittest import mock
from contextlib import contextmanager

from snowcli.cli.common.snow_cli_global_context import snow_cli_global_context_manager

REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"
PROJECT_DIR = Path(__file__).parent


@contextmanager
def temporary_of(path: Path):
    """
    Returns a temporary copy of file at the given path.
    This file will be deleted when this context manager goes out-of-scope.
    """
    with tempfile.NamedTemporaryFile(suffix=path.name, mode="w+") as fh:
        with open(path, "r") as rh:
            fh.write(rh.read())
        fh.flush()
        yield Path(fh.name)


@contextmanager
def snowflake_ymls(dir_name: str):
    """
    Returns paths to [snowflake_yml, (snowflake_local_yml)].
    These files are temporary copies of the project config found in dir_name
    and will be deleted when this context manager goes out-of-scope.
    If there is no local overrides file, returns a list of length 1.
    """
    with temporary_of(PROJECT_DIR / dir_name / "snowflake.yml") as project_yml:
        local_path = PROJECT_DIR / dir_name / "snowflake.local.yml"
        if local_path.exists():
            with temporary_of(local_path) as local_yml:
                yield [project_yml, local_yml]
        else:
            yield [project_yml]


@pytest.fixture
def project_config_files(request):
    """
    Expects indirect parameterization, e.g.
    @pytest.mark.parametrize("project_config_files", ["project_1"], indirect=True)
    def test_my_project(project_config_files):
        config = load_project_config(project_config_files)
    """
    project_dir = request.param
    with snowflake_ymls(project_dir) as ymls:
        yield ymls
