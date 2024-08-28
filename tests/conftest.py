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

import logging
from datetime import datetime
from logging import FileHandler
from unittest import mock

import pytest
from rich import box
from snowflake.cli._app import loggers
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.commands.decorators import global_options, with_output
from snowflake.cli.api.config import config_init
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import QueryResult
from syrupy.extensions import AmberSnapshotExtension

from tests_common import IS_WINDOWS

pytest_plugins = [
    "tests_common",
    "tests.testing_utils.fixtures",
    "tests.project.fixtures",
    "tests.nativeapp.fixtures",
]


class CustomSnapshotExtension(AmberSnapshotExtension):
    def matches(
        self,
        *,
        serialized_data,
        snapshot_data,
    ) -> bool:
        if isinstance(serialized_data, str) and IS_WINDOWS:
            # To make Windows path to work with snapshots
            serialized_data = serialized_data.replace("\\", "/")
            # To make POSIX path snapshot work with Windows
            serialized_data = serialized_data.replace("//", "/")
            # To fix side effects of above replace change
            serialized_data = serialized_data.replace("https:/", "https://")
        return super().matches(
            serialized_data=serialized_data, snapshot_data=snapshot_data
        )


@pytest.fixture()
def os_agnostic_snapshot(snapshot):
    return snapshot.use_extension(CustomSnapshotExtension)


@pytest.fixture(autouse=True)
# Global context and logging levels reset is required.
# Without it, state from previous tests is visible in following tests.
#
# This automatically used setup fixture is required to use test.conf from resources
# in unit tests which are not using "runner" fixture (tests which do not invoke CLI command).
def reset_global_context_and_setup_config_and_logging_levels(
    request, test_snowcli_config
):
    cli_context_manager = get_cli_context_manager()
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
def clean_logging_handlers_fixture(request, snowflake_home):
    yield
    clean_logging_handlers()


# This automatically used fixture isolates default location
# of config files from user's system.
@pytest.fixture(autouse=True)
def isolate_snowflake_home(snowflake_home):
    yield snowflake_home


@pytest.fixture(autouse=True, scope="session")
def mocked_rich():
    from rich.panel import Panel

    class CustomPanel(Panel):
        def __init__(self, *arg, **kwargs):
            super().__init__(*arg, box=box.ASCII, **kwargs)

    # The box can be configured for typer but unfortunately it's not passed down the line to `Panel`
    # that's being used for printing help.
    with mock.patch("typer.rich_utils.Panel", CustomPanel):
        yield


def clean_logging_handlers():
    for logger in [logging.getLogger()] + list(
        logging.Logger.manager.loggerDict.values()
    ):
        handlers = [hdl for hdl in getattr(logger, "handlers", [])]
        for handler in handlers:
            logger.removeHandler(handler)
            if isinstance(handler, FileHandler):
                handler.close()


@pytest.fixture(name="_create_mock_cursor")
def make_mock_cursor(mock_cursor):
    return lambda: mock_cursor(
        columns=["string", "number", "array", "object", "date"],
        rows=[
            ("string", 42, ["array"], {"k": "object"}, datetime(2022, 3, 21)),
            ("string", 43, ["array"], {"k": "object"}, datetime(2022, 3, 21)),
        ],
    )


@pytest.fixture(name="faker_app")
def make_faker_app(runner, _create_mock_cursor):
    app = runner.app

    @app.command("Faker")
    @with_output
    @global_options
    def faker_app(**options):
        """Faker app"""
        with cli_console.phase("Enter", "Exit") as step:
            step("Faker. Teeny Tiny step: UNO UNO")

        return QueryResult(_create_mock_cursor())

    yield
