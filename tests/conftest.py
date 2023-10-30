from __future__ import annotations

import functools
import logging

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


# This cleanup function is required to avoid random breaking of logging
# in one test caused by presence of capsys in other test.
# See similar issues: https://github.com/pytest-dev/pytest/issues/5502
@pytest.fixture(autouse=True)
def clean_logging_handlers(request):
    for logger in [logging.getLogger()] + list(
        logging.Logger.manager.loggerDict.values()
    ):
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
