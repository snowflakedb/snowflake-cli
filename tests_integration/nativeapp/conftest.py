from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from tests_integration.conftest import SnowCLIRunner


@pytest.fixture
def nativeapp_project_directory(project_directory, nativeapp_teardown):
    @contextmanager
    def _nativeapp_project_directory(name):
        with project_directory(name) as d:
            with nativeapp_teardown(d):
                yield d

    return _nativeapp_project_directory


@pytest.fixture
def nativeapp_teardown(runner: SnowCLIRunner):
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
