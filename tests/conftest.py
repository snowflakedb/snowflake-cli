from __future__ import annotations

import functools

import pytest
from typer import Typer
from typer.testing import CliRunner

from snowcli.cli.common import snow_cli_global_context


class SnowCLIRunner(CliRunner):
    def __init__(self, app: Typer, test_snowcli_config: str):
        super().__init__()
        self.app = app
        self.test_snowcli_config = test_snowcli_config

    @functools.wraps(CliRunner.invoke)
    def invoke(self, *a, **kw):
        kw.update(catch_exceptions=False)
        return super().invoke(
            self.app, ["--config-file", self.test_snowcli_config, *a[0]], **kw
        )


@pytest.fixture(autouse=True)
def reset_global_context_after_each_test(request):
    snow_cli_global_context.reset_global_context()
    yield
