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
"""What happens when the bundle directory cannot be deleted."""
from __future__ import annotations

import os
import stat
from functools import partial
from pathlib import Path
from unittest.mock import patch

import pytest
from snowflake.cli._plugins.apps.snowflake_app_project_paths import (
    CLEAN_UP_OUTPUT_SPAN,
    SnowflakeAppProjectPaths,
)
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.metrics import CLIMetrics, CLIMetricsSpan
from snowflake.cli.api.secure_path import SecurePath

from tests_common import IS_WINDOWS

_MODULE = "snowflake.cli._plugins.apps.snowflake_app_project_paths"


@pytest.fixture(autouse=True)
def _no_retry_delay():
    """The retry backoff is real time; no test needs to spend it."""
    with patch(f"{_MODULE}.time.sleep"):
        yield


@pytest.fixture
def project_paths(tmp_path) -> SnowflakeAppProjectPaths:
    paths = SnowflakeAppProjectPaths(project_root=tmp_path)
    paths.bundle_root.mkdir(parents=True)
    (paths.bundle_root / "app.py").write_text("print('hi')")
    return paths


class _DeniedRmdir:
    """A ``SecurePath.rmdir`` that denies the first *times* attempts.

    The failures that motivate the retry (a read-only file, a handle held by an
    editor or a virus scanner) are Windows behaviours a POSIX test host will not
    reproduce, so they are simulated.
    """

    def __init__(self, times: int):
        self.remaining = times
        self.attempts = 0
        self._real = SecurePath.rmdir

    def __get__(self, instance, owner=None):
        # Patched in as a class attribute, so it has to bind like a method.
        return partial(self.__call__, instance)

    def __call__(self, secure_path: SecurePath, *args, **kwargs):
        self.attempts += 1
        if self.remaining > 0:
            self.remaining -= 1
            raise PermissionError(13, "Access is denied")
        return self._real(secure_path, *args, **kwargs)


class TestRemoveUpBundleRoot:
    def test_removes_the_bundle_root(self, project_paths):
        project_paths.remove_up_bundle_root()

        assert not project_paths.bundle_root.exists()

    def test_is_a_no_op_when_there_is_nothing_to_remove(self, tmp_path):
        SnowflakeAppProjectPaths(project_root=tmp_path).remove_up_bundle_root()

    def test_a_delete_that_fails_once_is_retried(self, project_paths):
        rmdir = _DeniedRmdir(times=1)

        with patch.object(SecurePath, "rmdir", rmdir):
            project_paths.remove_up_bundle_root()

        assert rmdir.attempts == 2
        assert not project_paths.bundle_root.exists()

    def test_a_bundle_root_that_cannot_be_removed_is_reported(self, project_paths):
        """Bundling must not continue: files left behind would be uploaded as
        part of the bundle."""
        with patch.object(SecurePath, "rmdir", _DeniedRmdir(times=99)):
            with pytest.raises(CliError) as err:
                project_paths.remove_up_bundle_root()

        message = str(err.value)
        assert str(project_paths.bundle_root) in message
        assert "Access is denied" in message
        assert "run the command again" in message

    @pytest.mark.skipif(
        IS_WINDOWS, reason="Directory permissions do not govern deletion on Windows"
    )
    @pytest.mark.skipif(
        hasattr(os, "geteuid") and os.geteuid() == 0,
        reason="root ignores the permission bits this test relies on",
    )
    def test_a_directory_stripped_of_write_permission_is_still_removed(
        self, project_paths
    ):
        locked = project_paths.bundle_root / "locked"
        locked.mkdir()
        (locked / "data.txt").write_text("x")
        locked.chmod(stat.S_IRUSR | stat.S_IXUSR)

        try:
            project_paths.remove_up_bundle_root()
        finally:
            if locked.exists():
                locked.chmod(stat.S_IRWXU)

        assert not project_paths.bundle_root.exists()


class TestCleanUpOutput:
    @pytest.fixture(autouse=True)
    def _fresh_metrics(self):
        get_cli_context_manager().metrics = CLIMetrics()

    def _span(self, name: str) -> dict | None:
        spans = get_cli_context_manager().metrics.completed_spans
        return next(
            (s for s in spans if s[CLIMetricsSpan.NAME_KEY] == name),
            None,
        )

    def test_removes_the_bundle_directory(self, project_paths):
        project_paths.clean_up_output()

        assert not project_paths.bundle_root.exists()

    def test_removes_the_output_directory_it_created(self, project_paths):
        """``output`` exists only to hold the bundle in most projects, so it
        should not be left behind either."""
        project_paths.clean_up_output()

        assert not (project_paths.project_root / "output").exists()

    def test_keeps_what_the_project_put_in_the_output_directory(self, project_paths):
        """``output`` is a common name for a project's own build artifacts,
        exports and notebook results. Bundling into a subdirectory of it must
        not destroy them."""
        report = project_paths.project_root / "output" / "report.csv"
        report.write_text("id,value")

        project_paths.clean_up_output()

        assert report.exists()
        assert not project_paths.bundle_root.exists()

    def test_is_a_no_op_when_there_is_nothing_to_remove(self, tmp_path):
        SnowflakeAppProjectPaths(project_root=tmp_path).clean_up_output()

        assert self._span(CLEAN_UP_OUTPUT_SPAN) is None

    def test_a_failed_clean_up_does_not_fail_the_command(self, project_paths, capsys):
        """The command's own work is already done by the time this runs."""
        with patch.object(SecurePath, "rmdir", _DeniedRmdir(times=99)):
            project_paths.clean_up_output()

        warning = capsys.readouterr().out
        assert str(project_paths.bundle_root) in warning
        assert "finished successfully" in warning

    def test_a_failed_clean_up_is_attributed_to_a_span(self, project_paths):
        """Nothing else is open at this point, so without a span of its own the
        failure would be reported against no span at all."""
        with patch.object(SecurePath, "rmdir", _DeniedRmdir(times=99)):
            project_paths.clean_up_output()

        span = self._span(CLEAN_UP_OUTPUT_SPAN)
        assert span is not None
        assert span[CLIMetricsSpan.ERROR_KEY] == "PermissionError"

    def test_a_successful_clean_up_records_no_error(self, project_paths):
        project_paths.clean_up_output()

        span = self._span(CLEAN_UP_OUTPUT_SPAN)
        assert span is not None
        assert span[CLIMetricsSpan.ERROR_KEY] is None


def test_the_bundle_root_is_where_the_shared_flow_puts_it(tmp_path: Path):
    """The subclass must not move it: the upload step and every existing
    project's .gitignore expect output/bundle."""
    paths = SnowflakeAppProjectPaths(project_root=tmp_path)

    assert paths.bundle_root == tmp_path / "output" / "bundle"
