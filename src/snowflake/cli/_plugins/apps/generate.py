# Copyright (c) 2026 Snowflake Inc.
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
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, cast

from snowflake.cli.api.exceptions import FileTooLargeError
from snowflake.cli.api.secure_path import SecurePath

_REQUIREMENTS_FILE = "requirements.txt"
_PYPROJECT_FILE = "pyproject.toml"
_PYTHON_DEPENDENCY_FILES = (_PYPROJECT_FILE, _REQUIREMENTS_FILE)

# Dependency files are small; anything larger is not read.
_DEPENDENCY_FILE_SIZE_LIMIT_MB = 1

# The project name that starts a PEP 508 requirement; extras, version
# specifiers, markers and URLs follow it.
_REQUIREMENT_NAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?")
# Runs that PEP 503 folds into one dash.
_NAME_SEPARATORS = re.compile(r"[-_.]+")


def _requirement_name(requirement: str) -> str:
    """The PEP 503-normalized project name of a requirement line, or ``""`` for a
    blank, comment or option line."""
    match = _REQUIREMENT_NAME.match(requirement.strip())
    if match is None:
        return ""
    return _NAME_SEPARATORS.sub("-", match.group().lower())


def _read_dependency_file(path: Path, encoding: str) -> str:
    """The text of a dependency file, or ``""`` when it can't be read.

    Detection only seeds a new ``app.yml``, so an unreadable or oversized file
    counts as declaring nothing.
    """
    try:
        return SecurePath(path).read_text(
            file_size_limit_mb=_DEPENDENCY_FILE_SIZE_LIMIT_MB, encoding=encoding
        )
    except (OSError, UnicodeDecodeError, FileTooLargeError):
        return ""


def _has_streamlit_dependency(project_dir: Path) -> bool:
    """Whether ``requirements.txt`` or the PEP 621 ``[project] dependencies`` of
    ``pyproject.toml`` name ``streamlit`` itself.

    Mirrors the builder's check, so a manifest generated here passes it: names
    match exactly after normalization (``streamlit-aggrid`` and the project's own
    name do not count), and other layouts (``-r`` includes, Poetry, optional
    dependencies, dependency groups) are not read.
    """
    requirements: List[str] = []
    requirements_file = project_dir / _REQUIREMENTS_FILE
    if requirements_file.is_file():
        text = _read_dependency_file(requirements_file, "utf-8-sig")
        requirements += text.splitlines()

    pyproject_file = project_dir / _PYPROJECT_FILE
    if pyproject_file.is_file():
        try:
            data = tomllib.loads(_read_dependency_file(pyproject_file, "utf-8"))
        except tomllib.TOMLDecodeError:
            data = {}
        project = data.get("project")
        dependencies = (
            project.get("dependencies") if isinstance(project, dict) else None
        )
        if isinstance(dependencies, list):
            requirements += [d for d in dependencies if isinstance(d, str)]

    return any(_requirement_name(r) == "streamlit" for r in requirements)


@dataclass(frozen=True)
class PythonProject:
    """Python dependency files found by ``snow app setup``.

    ``streamlit`` is whether they name streamlit. ``node`` is whether the project
    also has a ``package.json``: like the builder, setup then treats it as a Node
    app unless ``app.yml`` declares python, so it writes no ``runtime`` and only
    explains how to declare a hybrid app (see :func:`hybrid_app_hint`).
    """

    streamlit: bool
    node: bool

    @property
    def runtime(self) -> str:
        """The ``runtime`` setup writes, or ``node`` when it writes none."""
        return "node" if self.node else "python"


def detect_python_project(project_dir: Path) -> Optional[PythonProject]:
    """Find Python dependency files (``pyproject.toml`` or ``requirements.txt``)
    in *project_dir*, or ``None`` when there are none.

    The result only seeds a newly generated ``app.yml``, which the user can edit.
    """
    if not any((project_dir / n).is_file() for n in _PYTHON_DEPENDENCY_FILES):
        return None
    return PythonProject(
        streamlit=_has_streamlit_dependency(project_dir),
        node=(project_dir / "package.json").is_file(),
    )


def hybrid_app_hint(streamlit: bool) -> str:
    """How to declare a project with a ``package.json`` and Python dependency
    files as a hybrid app, which builds as Node until ``app.yml`` declares python.
    """
    settings = ["`runtime: [node, python]`", "`compute_resource: SERVERLESS`"]
    if streamlit:
        settings.append("`streamlit: true`")
    return (
        "Found package.json, so the app deploys as Node. If it also runs Python, "
        f"set {', '.join(settings[:-1])} and {settings[-1]}, and declare "
        "`install`, `build` and `run`."
    )


def _yaml_str(v: str) -> str:
    # YAML treats bare double quotes as string delimiters and strips them on
    # round-trip, turning '"lower_db"' into 'lower_db' (then uppercased by
    # Snowflake).  Wrapping in YAML single quotes preserves embedded double
    # quotes as literal data.  Single quotes inside the value are escaped by
    # doubling them, per the YAML 1.1 single-quoted scalar spec.
    if '"' in v:
        return "'" + v.replace("'", "''") + "'"
    return v


def _generate_app_yml(
    app_id: str,
    resolved: Dict[str, Optional[str]],
    *,
    use_workspace: bool,
    python_project: Optional[PythonProject] = None,
) -> str:
    """Generate ``app.yml`` (Snowflake App Runtime v2) content.

    Emits the flat ``app.yml`` v2 manifest that the ``snow app`` commands read
    instead of ``snowflake.yml``. Required keys: ``database``, ``schema``,
    ``warehouse``. Optional key: ``build_eai`` (omitted when ``None``).

    The manifest carries a single top-level *baseline* (no ``targets``), so
    ``snow app deploy`` deploys it directly. ``package_name`` and
    ``artifact_repo`` are omitted; they default to ``name`` and ``<name>_repo``
    at deploy time. Code storage (``code_workspace`` / ``code_stage``) is also
    omitted: ``snow app deploy`` picks the backend at deploy time — a workspace
    when the destination is a personal database or the role can create one,
    otherwise a ``<name>_CODE`` stage — so the manifest stays minimal and the
    decision follows the role's actual privileges. The builder ``install`` /
    ``build`` / ``run`` phases are likewise omitted; they default to the Node
    conventions at deploy time. With a Python *python_project*, ``runtime:
    python``, ``compute_resource: SERVERLESS`` and, for a Streamlit app,
    ``streamlit: true`` are written, and the user declares ``run``. One that
    also has a ``package.json`` stays a Node app; setup prints the
    :func:`hybrid_app_hint` for it instead.

    ``use_workspace`` is accepted but unused — the code-storage backend is
    resolved at deploy time rather than baked into ``app.yml``.
    """
    database = cast(str, resolved["database"])
    schema = cast(str, resolved["schema"])
    warehouse = cast(str, resolved["warehouse"])
    build_eai = resolved.get("build_eai")

    lines = [
        "version: 2",
        "",
        f"name: {app_id.upper()}",
        f"database: {_yaml_str(database)}",
        f"schema: {_yaml_str(schema)}",
        f"query_warehouse: {_yaml_str(warehouse)}",
    ]
    if build_eai:
        lines.append(f"build_eai: {_yaml_str(build_eai)}")
    if python_project is not None and not python_project.node:
        lines += ["", "runtime: python"]
        if python_project.streamlit:
            lines.append("streamlit: true")
        lines.append("compute_resource: SERVERLESS")
    lines += [
        "",
        "ignore:",
        "  - node_modules",
        "  - .env*",
        "  - __pycache__",
        '  - "*.pyc"',
        "  - .next",
        "  - .git",
        "  - snowflake.log",
    ]
    return "\n".join(lines) + "\n"
