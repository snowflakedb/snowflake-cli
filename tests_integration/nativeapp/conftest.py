from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from tests_integration.conftest import SnowCLIRunner


@pytest.fixture
def nativeapp_project_directory(project_directory, nativeapp_teardown):
    """Wrapper around the project_directory fixture specific to Native App testing.

    This fixture provides a context manager that does the following:
    - Automatically calls `snow app teardown` before exiting

    Parameters for the returned context manager:
    :param name: The name of the directory in tests_integration/test_data/projects to use.
    """

    @contextmanager
    def _nativeapp_project_directory(name):
        with project_directory(name) as d:
            with nativeapp_teardown(project_dir=d):
                yield d

    return _nativeapp_project_directory


@pytest.fixture
def nativeapp_teardown(runner: SnowCLIRunner):
    """Runs `snow app teardown` before exiting.

    This fixture provides a context manager that runs
    `snow app teardown --force --cascade` before exiting,
    regardless of any exceptions raised.

    Parameters for the returned context manager:
    :param project_dir: Path to the project directory (optional)
    :param env: Environment variables to replace os.environ (optional)
    """

    @contextmanager
    def _nativeapp_teardown(
        *,
        project_dir: Path | None = None,
        env: dict | None = None,
    ):
        try:
            yield
        finally:
            args = ["--force", "--cascade"]
            if project_dir:
                args += ["--project", str(project_dir)]
            kwargs: dict[str, Any] = {}
            if env:
                kwargs["env"] = env
            result = runner.invoke_with_connection(["app", "teardown", *args], **kwargs)
            assert result.exit_code == 0

    return _nativeapp_teardown
