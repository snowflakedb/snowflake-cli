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

import importlib.util
from pathlib import Path

import pytest
from snowflake.cli import __about__
from snowflake.cli.__about__ import CLIInstallationSource


def _load_packaging_module():
    path = (
        Path(__file__).resolve().parents[1]
        / "scripts"
        / "packaging"
        / "build_isolated_binary_with_hatch.py"
    )
    spec = importlib.util.spec_from_file_location(
        "build_isolated_binary_with_hatch", path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PYPI_ASSIGNMENT = "INSTALLATION_SOURCE = CLIInstallationSource.PYPI"


def test_git_tree_stays_pypi():
    assert __about__.INSTALLATION_SOURCE is CLIInstallationSource.PYPI
    assert CLIInstallationSource.SNOWFLAKE_MANAGED.value == "snowflake-managed"


def test_rewrite_defaults_to_binary():
    packaging = _load_packaging_module()
    rewritten = packaging.rewrite_installation_source_assignment(PYPI_ASSIGNMENT)
    assert rewritten == "INSTALLATION_SOURCE = CLIInstallationSource.BINARY"


def test_rewrite_can_stamp_snowflake_managed():
    packaging = _load_packaging_module()
    rewritten = packaging.rewrite_installation_source_assignment(
        PYPI_ASSIGNMENT, source="SNOWFLAKE_MANAGED"
    )
    assert rewritten == (
        "INSTALLATION_SOURCE = CLIInstallationSource.SNOWFLAKE_MANAGED"
    )


@pytest.mark.parametrize("source", ["PYPI", "NATIVE", "SELF_MANAGED", "native"])
def test_rewrite_rejects_unknown_stamps(source):
    packaging = _load_packaging_module()
    with pytest.raises(ValueError, match="installation source stamp"):
        packaging.rewrite_installation_source_assignment(PYPI_ASSIGNMENT, source=source)


def test_rewrite_requires_assignment():
    packaging = _load_packaging_module()
    with pytest.raises(RuntimeError, match="INSTALLATION_SOURCE"):
        packaging.rewrite_installation_source_assignment("VERSION = '1.0.0'")
