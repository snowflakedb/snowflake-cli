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

import re
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
    return [
        "click==8.1.8",
        "GitPython==3.1.44",
        "PyYAML==6.0.2",
        "jinja2==3.1.6",
        "packaging",
        "pip",
        "pluggy==1.6.0",
        "prompt-toolkit==3.0.51",
        "pydantic==2.11.7",
        "requests==2.32.4",
        "requirements-parser==0.13.0",
        "rich==14.0.0",
        "setuptools==80.9.0",
        "snowflake-connector-python[secure-local-storage]==3.15.0",
        'snowflake-snowpark-python>=1.15.0,<1.26.0;python_version < "3.12"',
        "snowflake.core==1.5.1",
        "tomlkit==0.13.3",
        "typer==0.16.0",
        "urllib3>=1.24.3,<2.5",
    ]


def write_dependencies(dependencies: List[str]) -> None:
    pass


def recursively_generate_dependencies(
    base_dependencies: List[str], depth: int
) -> List[str]:
    with TemporaryDirectory() as tmp_project_dir:
        # create pyproject.toml with dependencies = [base-dependencies]
        tmp_pyproject_toml = Path(tmp_project_dir) / "pyproject.toml"
        contents = tomlkit.loads(PYPROJECT_TOML.read_text())
        tmp_contents = {"project": deepcopy(contents["project"])}
        tmp_contents["project"]["dependencies"] = base_dependencies
        tmp_pyproject_toml.write_text(tomlkit.dumps(tmp_contents))

        # run uv to list dependencies
        with ensure_uv() as uv:
            dependencies_raw = run(
                [*uv, "tree", "--depth", str(depth), "--project", tmp_project_dir],
                text=True,
                capture_output=True,
                check=True,
            ).stdout
            Path("debug_output_raw").write_text(dependencies_raw)

        # parse uv output
        dependecy_regex = (
            r".* (?P<name>[a-zA-Z].*) v(?P<version>[\.0-9]+)(?P<uv_comment>.*)"
        )
        ignored_comments = [
            "(*)",  # uv symbol for "repeated row"
            "(extra: development)",  # CLI deployment mode dependency
            "(extra: development) (*)",
        ]
        dependencies = []
        debug_output: List[str] = []
        for line in dependencies_raw.splitlines():
            match = re.match(dependecy_regex, line)
            if not match or match.group("uv_comment").strip() in ignored_comments:
                continue
            # dependencies.append(line)
            dependencies.append(f"{match.group('name')}=={match.group('version')}")

        Path("debug_output").write_text("\n".join(dependencies))

    return dependencies


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        # do not execute script on Windows, but do not block the commit
        sys.exit(0)
    base_dependencies = read_base_dependencies()
    dependencies = recursively_generate_dependencies(base_dependencies, depth=2)
    write_dependencies(dependencies)
