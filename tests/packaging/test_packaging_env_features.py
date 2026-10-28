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

"""Packaging hatch env must not install development extras.

Linux daily packaging builds that env inside Ubuntu 18.04
(scripts/packaging/Dockerfile). A native wheel pulled in through the
development extra (pyarrow via snowflake-ml-python) dies there for lack
of rustc. Win/Mac stay green. Asserting packaging.features excludes
development fails that class of change on the PR.
"""

from __future__ import annotations

import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = REPO_ROOT / "pyproject.toml"
COMPOSE = REPO_ROOT / "scripts" / "packaging" / "docker-compose.yaml"


def _pyproject() -> dict:
    with PYPROJECT.open("rb") as fh:
        return tomllib.load(fh)


def test_packaging_env_does_not_install_development_extras():
    packaging = _pyproject()["tool"]["hatch"]["envs"]["packaging"]
    extras = _pyproject()["project"]["optional-dependencies"]
    default = _pyproject()["tool"]["hatch"]["envs"]["default"]
    assert packaging.get("features") == ["packaging"]
    assert "template" not in packaging
    assert extras.get("packaging") == []
    assert "development" in default.get("features", [])
    assert any(
        str(dep).startswith("snowflake-ml-python")
        for dep in extras.get("development", [])
    )


def test_legacy_pypi_build_is_a_packaging_script():
    packaging_scripts = _pyproject()["tool"]["hatch"]["envs"]["packaging"]["scripts"]
    default_scripts = _pyproject()["tool"]["hatch"]["envs"]["default"]["scripts"]
    assert "legacy-pypi-build" in packaging_scripts
    assert "legacy-pypi-build" not in default_scripts


def test_wheel_compose_uses_packaging_env_not_default():
    compose = COMPOSE.read_text()
    assert "hatch run packaging:legacy-pypi-build" in compose
    assert "hatch run legacy-pypi-build" not in compose
    assert "hatch run default:legacy-pypi-build" not in compose
