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
from pathlib import Path
from shutil import which
from subprocess import run
from tempfile import TemporaryDirectory
from typing import List, Set
from venv import create as create_venv

import tomlkit


class PyprojectToml:
    """Class responsible for reading and writing pyproject.toml file"""

    PYPROJECT_TOML = Path(__file__).parent.parent / "pyproject.toml"

    def read_base_dependencies(self):
        contents = tomlkit.loads(self.PYPROJECT_TOML.read_text())
        return contents["cli"]["dependencies"]

    def read_project_dependencies(self):
        contents = tomlkit.loads(self.PYPROJECT_TOML.read_text())
        return contents["project"]["dependencies"]

    def create_minimal_project_with_dependencies(
        self, dependencies: List[str], path: Path
    ):
        original_contents = tomlkit.loads(self.PYPROJECT_TOML.read_text())
        new_contents = {"project": original_contents["project"]}
        new_contents["project"]["dependencies"] = dependencies
        path.write_text(tomlkit.dumps(new_contents))

    def write_generated_dependencies(self, dependencies: List[str]):
        contents = tomlkit.loads(self.PYPROJECT_TOML.read_text())
        contents["project"]["dependencies"] = tomlkit.array(
            "[\n  # v-- section generated from cli.dependencies --v\n"
            + "\n".join([f"  '{dep}'," for dep in sorted(dependencies)])
            + "\n  # ^-- section generated from cli.dependencies --^\n]"
        )
        self.PYPROJECT_TOML.write_text(tomlkit.dumps(contents))


@contextmanager
def ensure_uv():
    """Yields command to use to call uv"""
    if which("uv"):
        yield ["uv"]
        return

    # install uv in temporary virtual environment
    with TemporaryDirectory() as tmpdir:
        venv_path = Path(tmpdir) / "venv"
        create_venv(venv_path, with_pip=True)
        python_path = venv_path / "bin" / "python"
        run([str(python_path), "-m", "pip", "install", "uv"], check=True)
        yield [python_path, "-m", "uv"]


def recursively_generate_dependencies(
    base_dependencies: List[str], depth: int
) -> Set[str]:
    with TemporaryDirectory() as tmp_project_dir:
        # create pyproject.toml with dependencies = [cli.dependencies]
        PyprojectToml().create_minimal_project_with_dependencies(
            dependencies=base_dependencies,
            path=Path(tmp_project_dir) / "pyproject.toml",
        )

        # run uv to list dependencies
        with ensure_uv() as uv:
            dependencies_raw = run(
                [*uv, "tree", "--depth", str(depth), "--project", tmp_project_dir],
                text=True,
                capture_output=True,
                check=True,
            ).stdout

        # parse uv output
        dependency_regex = r".* (?P<name>[a-zA-Z].*) v(?P<version>[\.0-9a-zA-Z]+?)(?P<uv_comment>\s.*)?"
        ignored_comments = [
            "(*)",  # uv symbol for "repeated row"
            "(extra: development)",  # CLI deployment mode dependency
            "(extra: development) (*)",
        ]
        dependencies = set()
        for line in dependencies_raw.splitlines():
            match = re.fullmatch(dependency_regex, line)
            if not match or str(match.group("uv_comment")).strip() in ignored_comments:
                continue
            dependencies.add(f"{match.group('name')}=={match.group('version')}")

    return dependencies


def join_dependencies(
    dependencies: List[str], generated_dependencies: Set[str]
) -> None:
    """Add `generated_dependencies` not listed in `dependencies` to `dependencies`"""

    def _get_name(dependency: str) -> str:
        """get package name from dependency"""
        min_found = len(dependency)
        for sep in "=<>":
            idx = dependency.find(sep)
            if idx >= 0:
                min_found = min(idx, min_found)

        if min_found == len(dependency):
            # version not provided
            return dependency.lower()
        return dependency[:min_found].lower()

    existing_names = set(_get_name(dep) for dep in dependencies)
    for dep in generated_dependencies:
        if _get_name(dep) in existing_names:
            continue
        existing_names.add(_get_name(dep))
        dependencies.append(dep)


if __name__ == "__main__":
    if sys.platform.startswith("win"):
        # do not execute script on Windows, but do not block the commit
        # reason: the plugin was only tested on unix-like systems (virtualenv and UV output might vary on Windows OS)
        sys.exit(0)
    pyproject = PyprojectToml()
    dependencies = pyproject.read_base_dependencies()
    original_dependencies = pyproject.read_project_dependencies()
    # Depth limited to 2 (dependencies and their sub-dependencies) to avoid drastic changes. Can be changed later.
    generated_dependencies = recursively_generate_dependencies(dependencies, depth=2)
    join_dependencies(dependencies, generated_dependencies)
    pyproject.write_generated_dependencies(dependencies)
    print("\nNew dependencies added:")
    for dep in sorted(set(dependencies) - set(original_dependencies)):
        print(f"  {dep}")
