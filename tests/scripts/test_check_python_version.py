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

"""Tests for check_python_version.py — the daily pinned-Python-version check.
All tests are offline: no real HTTP calls are made.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

import pytest
import requests

_SCRIPTS_DIR = Path(__file__).parents[2] / ".github" / "scripts"
sys.path.insert(0, str(_SCRIPTS_DIR))

import check_python_version as cpv  # noqa: E402


def test_read_pinned_version(tmp_path):
    build_script = tmp_path / "build_python.sh"
    build_script.write_text("PYTHON_VERSION=3.12.5\n")

    assert cpv.read_pinned_version(str(build_script)) == (3, 12, 5)


def test_read_pinned_version_missing_raises(tmp_path, capsys):
    build_script = tmp_path / "build_python.sh"
    build_script.write_text("SOMETHING_ELSE=1\n")

    with pytest.raises(SystemExit) as exc_info:
        cpv.read_pinned_version(str(build_script))

    assert exc_info.value.code == 1
    assert "Could not find PYTHON_VERSION" in capsys.readouterr().out


def _releases_response(names):
    response = mock.Mock(status_code=200)
    response.json.return_value = [{"name": name} for name in names]
    response.raise_for_status.return_value = None
    return response


def test_latest_patch_for_series_returns_highest_patch():
    releases = _releases_response(
        ["Python 3.12.3", "Python 3.12.10", "Python 3.12.5", "Python 3.11.9"]
    )
    with mock.patch.object(cpv.requests, "get", return_value=releases):
        assert cpv.latest_patch_for_series(3, 12) == 10


def test_latest_patch_for_series_no_matching_series_exits(capsys):
    releases = _releases_response(["Python 3.11.9"])
    with mock.patch.object(cpv.requests, "get", return_value=releases):
        with pytest.raises(SystemExit) as exc_info:
            cpv.latest_patch_for_series(3, 12)

    assert exc_info.value.code == 1
    assert "No published releases found" in capsys.readouterr().out


def test_latest_patch_for_series_network_error_exits_cleanly(capsys):
    with mock.patch.object(
        cpv.requests, "get", side_effect=requests.ConnectionError("boom")
    ):
        with pytest.raises(SystemExit) as exc_info:
            cpv.latest_patch_for_series(3, 12)

    assert exc_info.value.code == 1
    assert "Failed to query python.org" in capsys.readouterr().out


def test_latest_patch_for_series_http_error_exits_cleanly(capsys):
    response = mock.Mock(status_code=500)
    response.raise_for_status.side_effect = requests.HTTPError("server error")
    with mock.patch.object(cpv.requests, "get", return_value=response):
        with pytest.raises(SystemExit) as exc_info:
            cpv.latest_patch_for_series(3, 12)

    assert exc_info.value.code == 1
    assert "Failed to query python.org" in capsys.readouterr().out


def test_main_writes_github_output(tmp_path, monkeypatch, capsys):
    build_script = tmp_path / "build_python.sh"
    build_script.write_text("PYTHON_VERSION=3.12.5\n")
    monkeypatch.setattr(cpv, "BUILD_SCRIPT_PATH", str(build_script))

    out_file = tmp_path / "gh_output"
    out_file.write_text("")
    monkeypatch.setenv("GITHUB_OUTPUT", str(out_file))

    releases = _releases_response(["Python 3.12.10"])
    with mock.patch.object(cpv.requests, "get", return_value=releases):
        cpv.main()

    output = out_file.read_text()
    assert "current_version=3.12.5" in output
    assert "latest_version=3.12.10" in output
    assert "update_available=true" in output
    assert "🚨" in capsys.readouterr().out
