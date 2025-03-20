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

from __future__ import annotations

import shutil
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List

import pytest
from snowflake.cli.api.project.schemas.v1.snowpark.callable import FunctionSchema

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
    with temp_cloned_dir(PROJECT_DIR / dir_name) as dir_:
        project_yml = dir_ / "snowflake.yml"
        local_yml = dir_ / "snowflake.local.yml"
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
        project = load_project(project_definition_files).project_definition
    """
    dir_name = request.param
    with snowflake_ymls(dir_name) as ymls:
        yield ymls


@pytest.fixture()
def function_instance():
    return FunctionSchema(
        name="func1",
        handler="app.func1_handler",
        signature=[{"name": "a", "type": "string"}, {"name": "b", "type": "variant"}],
        returns="string",
    )
