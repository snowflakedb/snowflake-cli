from __future__ import annotations

import logging

import pytest
from snowflake.cli.api.cli_global_context import cli_context_manager
from snowflake.cli.api.config import config_init, CONFIG_MANAGER, CONNECTIONS_SECTION
from snowflake.cli.app import loggers

from snowflake.connector.config_manager import ConfigSlice
from pathlib import Path

pytest_plugins = ["tests.testing_utils.fixtures", "tests.project.fixtures"]


@pytest.fixture(autouse=True)
# Global context and logging levels reset is required.
# Without it, state from previous tests is visible in following tests.
#
# This automatically used setup fixture is required to use test.conf from resources
# in unit tests which are not using "runner" fixture (tests which do not invoke CLI command).
def reset_global_context_and_setup_config_and_logging_levels(
    request, test_snowcli_config
):
    cli_context_manager.reset()
    cli_context_manager.set_verbose(False)
    cli_context_manager.set_enable_tracebacks(False)
    config_init(test_snowcli_config)
    loggers.create_loggers(verbose=False, debug=False)
    yield


# This automatically used cleanup fixture is required to avoid random breaking of logging
# in one test caused by presence of capsys in other test.
# See similar issues: https://github.com/pytest-dev/pytest/issues/5502
@pytest.fixture(autouse=True)
def clean_logging_handlers_fixture(request):
    yield
    clean_logging_handlers()


# This automatically used fixture changes location in which
# ConfigManager looks up connections.toml file to non-existing location.
# This causes it to behave as the file was not provided
# Reason: if connections.toml is found, it automatically overrides "connections"
# section in config.toml, which causes tests to fail.
@pytest.fixture(autouse=True)
def remove_connections_toml_from_config():
    # HACK: as ConfigManager does not provide explicit way to
    # modify submanagers, we need to explicitly override variable
    # in which they're stored.
    for i, slice in enumerate(CONFIG_MANAGER._slices):
        if slice.section == CONNECTIONS_SECTION:
            CONFIG_MANAGER._slices[i] = ConfigSlice(
                path=Path("/this/file/does/not/exist"),
                section=slice.section,
                options=slice.options,
            )
    yield


def clean_logging_handlers():
    for logger in [logging.getLogger()] + list(
        logging.Logger.manager.loggerDict.values()
    ):
        handlers = [hdl for hdl in getattr(logger, "handlers", [])]
        for handler in handlers:
            logger.removeHandler(handler)
