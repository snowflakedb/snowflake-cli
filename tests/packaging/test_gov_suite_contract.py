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

"""Gov RT suite contract: hatch script shape.

Hatch `integration:test_gov` is collection shape (markers + SPCS ignore).
Extra args come from {args}/PYTEST_ARGS. This lock fails if the hatch
script grows -k, --deselect, or a gov-specific marker. SPCS --ignore is
preprod collection parity. Do not list skipped node ids in this file.
"""

from __future__ import annotations

import os
import re
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = REPO_ROOT / "pyproject.toml"
GOV_SCRIPT = REPO_ROOT / "scripts" / "run_tests_gov.sh"
QA_SCRIPT = REPO_ROOT / "scripts" / "run_tests_qa.sh"
SPCS_IGNORE = "tests_integration/tests_using_container_services"
# Pytest -k / --keyword as a flag, not the substring "-k".
PYTEST_K_FLAG = re.compile(r"(?:^|[\s])(?:-k|--keyword)(?:[\s=]|$)")


def _pyproject() -> dict:
    with PYPROJECT.open("rb") as fh:
        return tomllib.load(fh)


def _hatch_script(name: str) -> str:
    val = _pyproject()["tool"]["hatch"]["envs"]["integration"]["scripts"][name]
    if isinstance(val, list):
        return " ".join(val)
    return str(val)


def test_packaging_env_untouched_by_gov_suite():
    packaging = _pyproject()["tool"]["hatch"]["envs"]["packaging"]
    assert packaging.get("features") == ["packaging"]


def test_gov_hatch_script_is_suite_shape_without_k_or_hides():
    gov = _hatch_script("test_gov")
    commercial = _hatch_script("test")
    assert "not no_qa" in gov
    assert "not qa_only" in gov
    assert PYTEST_K_FLAG.search(gov) is None
    assert "--maxfail=0" in gov
    assert "-n6" in gov
    assert "{args}" in gov
    assert SPCS_IGNORE in gov
    assert SPCS_IGNORE in commercial
    assert "no_gov" not in gov
    assert "--deselect" not in gov


def test_no_gov_marker_is_not_registered():
    markers = _pyproject()["tool"]["pytest"]["ini_options"]["markers"]
    assert not any(m.startswith("no_gov:") or m.startswith("no_gov ") for m in markers)


def test_run_tests_gov_sh_matches_qa_shape_on_python_311():
    gov = GOV_SCRIPT.read_text(encoding="utf-8")
    qa = QA_SCRIPT.read_text(encoding="utf-8")
    assert GOV_SCRIPT.is_file()
    assert os.access(GOV_SCRIPT, os.X_OK)
    assert "python3.11 -m venv venv" in gov
    assert "python3.10" not in gov
    assert "integration:test_gov" in gov
    assert "PYTEST_ARGS" in gov
    assert 'echo "PYTEST_ARGS=${PYTEST_ARGS}"' in gov
    assert PYTEST_K_FLAG.search(gov) is None
    assert "--ignore" not in gov
    assert "--deselect" not in gov
    assert "hatch env create integration" in gov
    assert "e2e:cleanup" in gov
    assert "python3.11 -m venv venv" in qa
    assert "no_gov" not in gov
