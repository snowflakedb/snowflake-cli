from __future__ import annotations

import logging

import pytest

from snowcli.cli.common import snow_cli_global_context
from snowcli.config import config_init
from tests.testing_utils.fixtures import test_snowcli_config


@pytest.fixture(autouse=True)
# Global context reset is required.
# Without it, context state from previous tests is visible in following tests.
def reset_global_context_after_each_test(request):
    snow_cli_global_context.reset_global_context()
    yield


# This automatically used cleanup fixture is required to avoid random breaking of logging
# in one test caused by presence of capsys in other test.
# See similar issues: https://github.com/pytest-dev/pytest/issues/5502
@pytest.fixture(autouse=True)
def clean_logging_handlers(request):
    yield
    for logger in [logging.getLogger()] + list(
        logging.Logger.manager.loggerDict.values()
    ):
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)


# This automatically used setup fixture is required to use test.conf from resources
# in unit tests which are not using "runner" fixture (tests which do not invoke CLI command).
@pytest.fixture(autouse=True)
def set_test_config_in_config_manager(request, test_snowcli_config):
    config_init(test_snowcli_config)
    yield
