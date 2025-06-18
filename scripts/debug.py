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

import sys
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from subprocess import run
from tempfile import TemporaryDirectory
from typing import List
from venv import create as create_venv

import tomlkit

PYPROJECT_TOML = Path(__file__).parent.parent / "pyproject.toml"


@contextmanager
def ensure_uv():
    """Yields command to use to call uv"""
    result = run(["uv", "--version"])
    if result.returncode == 0:
        yield ["uv"]
    else:
        with TemporaryDirectory() as tmpdir:
            venv_path = Path(tmpdir) / "venv"
            create_venv(venv_path, with_pip=True)
            python_path = venv_path / "bin" / "python"
            run([str(python_path), "-m", "pip", "install", "uv"], check=True)
            yield [python_path, "-m", "uv"]


def read_base_dependencies() -> List[str]:
    return ["snowflake-cli"]


def write_dependencies(dependencies: List[str]) -> None:
    pass


def recursively_generate_dependencies(
    base_dependencies: List[str], depth: int
) -> List[str]:
    with TemporaryDirectory() as tmp_project_dir:
        # create fake tmp project
        tmp_pyproject_toml = Path(tmp_project_dir) / "pyproject.toml"
        contents = tomlkit.loads(PYPROJECT_TOML.read_text())
        tmp_contents = {"project": deepcopy(contents["project"])}
        tmp_contents["project"]["dependencies"] = base_dependencies
        tmp_pyproject_toml.write_text(tomlkit.dumps(tmp_contents))

        with ensure_uv() as uv:
            dependencies_raw = run(
                [*uv, "tree", "--depth", str(depth), "--project", tmp_project_dir],
                text=True,
                capture_output=True,
                check=True,
            ).stdout
            Path("debug_output").write_text(dependencies_raw)
            return []


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        sys.exit(0)
    base_dependencies = read_base_dependencies()
    dependencies = recursively_generate_dependencies(base_dependencies, depth=2)
    write_dependencies(dependencies)
