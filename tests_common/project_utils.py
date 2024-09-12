from __future__ import annotations

import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Dict

import pytest
import yaml


@pytest.fixture
def project_directory(temporary_working_directory, test_root_path):
    @contextmanager
    def _temporary_project_directory(
        project_name,
        merge_project_definition: Optional[dict] = None,
        subpath: Optional[Path] = None,
    ):
        test_data_file = test_root_path / "test_data" / "projects" / project_name
        project_dir = temporary_working_directory
        if subpath:
            project_dir = temporary_working_directory / subpath
            project_dir.mkdir(parents=True)
        shutil.copytree(test_data_file, project_dir, dirs_exist_ok=True)
        if merge_project_definition:
            with Path("snowflake.yml").open("r") as fh:
                project_definition = yaml.safe_load(fh)
            _merge_left(project_definition, merge_project_definition)
            with open(Path(project_dir) / "snowflake.yml", "w") as file:
                yaml.dump(project_definition, file)

        yield project_dir

    return _temporary_project_directory


def _merge_left(target: Dict, source: Dict) -> None:
    """
    Recursively merges key/value pairs from source into target.
    Modifies the original dict-like "target".
    """
    for k, v in source.items():
        if k in target and isinstance(target[k], dict):
            # assumption: all inputs have been validated.
            _merge_left(target[k], v)
        else:
            target[k] = v
