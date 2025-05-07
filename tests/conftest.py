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

import functools
import logging
import os
import shutil
import tempfile
from contextlib import contextmanager
from datetime import datetime
from io import StringIO
from logging import FileHandler
from pathlib import Path
from typing import Generator, List, NamedTuple, Optional, Union
from unittest import mock

import pytest
import yaml
from rich import box
from snowflake.cli._app import loggers
from snowflake.cli._app.cli_app import CliAppFactory
from snowflake.cli.api.cli_global_context import (
    fork_cli_context,
    get_cli_context_manager,
)
from snowflake.cli.api.commands.decorators import global_options, with_output
from snowflake.cli.api.config import config_init
from snowflake.cli.api.connections import OpenConnectionCache
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.output.types import QueryResult
from snowflake.cli.api.project.schemas.project_definition import (
    build_project_definition,
)
from snowflake.cli.api.project.schemas.v1.snowpark.argument import Argument
from snowflake.connector import ProgrammingError
from snowflake.connector.cursor import SnowflakeCursor
from syrupy.extensions import AmberSnapshotExtension
from typer import Typer
from typer.testing import CliRunner

from tests.test_data import test_data
from tests.testing_utils.files_and_dirs import (
    create_named_file,
    create_temp_file,
    merge_left,
)
from tests_common import IS_WINDOWS

pytest_plugins = [
    "tests_common",
    "tests.testing_utils",
    "tests.project.fixtures",
]

REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"
REQUIREMENTS_TXT = "requirements.txt"
TEST_DIR = Path(__file__).parent


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
#
# In addition to its own CliContextManager, each test gets its own OpenConnectionCache
# which is cleared after the test completes.
def reset_global_context_and_setup_config_and_logging_levels(
    request, test_snowcli_config
):
    with fork_cli_context():
        connection_cache = OpenConnectionCache()
        cli_context_manager = get_cli_context_manager()
        cli_context_manager.reset()
        cli_context_manager.verbose = False
        cli_context_manager.enable_tracebacks = False
        cli_context_manager.connection_cache = connection_cache
        config_init(test_snowcli_config)
        loggers.create_loggers(verbose=False, debug=False)
        try:
            yield
        finally:
            connection_cache.clear()


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


@pytest.fixture(name="_patch_app_version_in_tests", autouse=True)
def mock_app_version_in_tests(request):
    """Set predefined Snowflake-CLI for testing.

    Marker `app_version_patch` can be used in tests to skip it.

    @pytest.mark.app_version_patch(False)
    def test_case():
        ...
    """
    marker = request.node.get_closest_marker("app_version_patch")

    if marker and marker.kwargs.get("use") is False:
        yield
    else:
        with mock.patch(
            "snowflake.cli.__about__.VERSION", "0.0.0-test_patched"
        ) as _fixture:
            yield _fixture


class SnowCLIRunner(CliRunner):
    def __init__(self, app: Typer, test_snowcli_config: str):
        super().__init__()
        self.app = app
        self.test_snowcli_config = test_snowcli_config

    @functools.wraps(CliRunner.invoke)
    def invoke(self, *a, **kw):
        return self.invoke_with_config_file(self.test_snowcli_config, *a, **kw)

    def invoke_with_config_file(self, config_file: str, *a, **kw):
        kw.update(catch_exceptions=False)
        return super().invoke(self.app, ["--config-file", config_file, *a[0]], **kw)

    def super_invoke(self, *a, **kw):
        return super().invoke(self.app, [*a[0]], **kw)


@pytest.fixture
def app_zip(temporary_directory) -> Generator:
    yield create_temp_file(".zip", temporary_directory, [])


@pytest.fixture
def correct_requirements_txt(temporary_directory) -> Generator:
    req_txt = create_named_file(
        REQUIREMENTS_TXT, temporary_directory, test_data.requirements
    )
    yield req_txt
    os.remove(req_txt)


@pytest.fixture
def correct_requirements_snowflake_txt(temporary_directory) -> Generator:
    req_txt = create_named_file(
        REQUIREMENTS_SNOWFLAKE, temporary_directory, test_data.requirements
    )
    yield req_txt
    os.remove(req_txt)


@pytest.fixture()
def mock_ctx(mock_cursor):
    def _mock_connection_ctx_factory(cursor=mock_cursor(["row"], []), **kwargs):
        kwargs["cursor"] = cursor
        return MockConnectionCtx(**kwargs)

    yield _mock_connection_ctx_factory


@pytest.fixture()
def mock_streamlit_ctx(mock_cursor, mock_ctx):
    yield mock_ctx(
        mock_cursor(
            rows=[
                {"SYSTEM$GET_SNOWSIGHT_HOST()": "https://snowsight.domain"},
                {"CURRENT_ACCOUNT_NAME()": "https://snowsight.domain"},
            ],
            columns=["SYSTEM$GET_SNOWSIGHT_HOST()"],
        )
    )


class MockConnectionCtx(mock.MagicMock):
    def __init__(
        self,
        cursor=None,
        role: Optional[str] = "MockRole",
        warehouse: Optional[str] = "MockWarehouse",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.queries: List[str] = []
        self.cs = cursor
        self._checkout_count = 0
        self._role = role
        self._warehouse = warehouse

    def get_query(self):
        return "\n".join(self.queries)

    def get_queries(self):
        return self.queries

    @property
    def warehouse(self):
        return self._warehouse

    @property
    def database(self):
        return "MockDatabase"

    @property
    def schema(self):
        return "MockSchema"

    @property
    def role(self):
        return self._role

    @property
    def host(self):
        return "account.test.region.aws.snowflakecomputing.com"

    @property
    def account(self):
        return "account"

    def execute_string(self, query: str, **kwargs):
        if query.lower().startswith("alter streamlit") and query.lower().endswith(
            " checkout"
        ):
            self._checkout_count += 1
            if self._checkout_count > 1:
                raise ProgrammingError("Checkout already exists")
        self.queries.append(query)
        return (self.cs,)

    def execute_stream(self, query: StringIO, **kwargs):
        return self.execute_string(query.read(), **kwargs)


@pytest.fixture
def mock_cursor():
    class MockResultMetadata(NamedTuple):
        name: str

    class _MockCursor(SnowflakeCursor):
        def __init__(self, rows: List[Union[tuple, dict]], columns: List[str]):
            super().__init__(mock.Mock())
            self._rows = rows
            self._columns = [MockResultMetadata(c) for c in columns]
            self.query = "SELECT A MOCK QUERY"

        def fetchone(self):
            if self._rows:
                return self._rows.pop(0)
            return None

        def fetchall(self):
            return self._rows

        @property
        def rowcount(self):
            return len(self._rows)

        @property
        def description(self):
            yield from self._columns

        @classmethod
        def from_input(cls, rows, columns):
            return cls(rows, columns)

    return _MockCursor.from_input


@pytest.fixture
def mock_statement_success(mock_cursor):
    def generate_mock_success(is_dict: bool = False) -> SnowflakeCursor:
        if is_dict:
            return mock_cursor(
                rows=[{"status": "Statement executed successfully."}],
                columns=["status"],
            )
        else:
            return mock_cursor(
                rows=[("Statement executed successfully.",)], columns=["status"]
            )

    return generate_mock_success


@pytest.fixture
def package_file():
    with tempfile.TemporaryDirectory() as tmp:
        yield create_named_file("app.zip", tmp, [])


@pytest.fixture(scope="function")
def app_factory():
    yield CliAppFactory()


@pytest.fixture(scope="function")
def get_click_context(app_factory):
    yield lambda: app_factory.get_click_context()


@pytest.fixture(scope="function")
def runner(build_runner):
    yield build_runner()


@pytest.fixture(scope="function")
def build_runner(app_factory, test_snowcli_config):
    def func():
        app = app_factory.create_or_get_app()
        return SnowCLIRunner(app, test_snowcli_config)

    return func


@contextmanager
def _named_temporary_file(suffix=None, prefix=None):
    with tempfile.TemporaryDirectory() as tmp_dir:
        suffix = suffix or ""
        prefix = prefix or ""
        f = Path(tmp_dir) / f"{prefix}tmp_file{suffix}"
        f.touch()
        yield f


@pytest.fixture()
def named_temporary_file():
    return _named_temporary_file


@pytest.fixture(scope="function")
def test_snowcli_config():
    test_config = TEST_DIR / "test.toml"
    with _named_temporary_file(suffix=".toml") as p:
        p.write_text(test_config.read_text())
        p.chmod(0o600)  # Make config file private
        yield p


@pytest.fixture
def config_file():
    @contextmanager
    def _config_file(content: str = ""):
        with _named_temporary_file(suffix=".toml") as p:
            p.write_text(content)
            p.chmod(0o600)  # Make config file private
            yield p

    return _config_file


@pytest.fixture(scope="session")
def test_root_path():
    return TEST_DIR


@pytest.fixture(scope="session")
def test_projects_path(test_root_path):
    return test_root_path / "test_data" / "projects"


@pytest.fixture
def project_directory(temporary_directory, test_projects_path):
    @contextmanager
    def _temporary_project_directory(
        project_name, merge_project_definition: Optional[dict] = None
    ):
        test_data_file = test_projects_path / project_name
        shutil.copytree(test_data_file, temporary_directory, dirs_exist_ok=True)
        if merge_project_definition:
            project_definition = yaml.load(
                Path("snowflake.yml").read_text(), Loader=yaml.BaseLoader
            )
            merge_left(project_definition, merge_project_definition)
            with open(Path(temporary_directory) / "snowflake.yml", "w") as file:
                file.write(yaml.dump(project_definition))

        yield Path(temporary_directory)

    return _temporary_project_directory


@pytest.fixture(autouse=True)
def global_setup(monkeypatch):
    width = 81 if IS_WINDOWS else 80
    monkeypatch.setenv("COLUMNS", str(width))


@pytest.fixture()
def argument_instance():
    return Argument(name="Foo", type="Bar")


@pytest.fixture()
def native_app_project_instance():
    return build_project_definition(
        **dict(
            definition_version="2",
            entities=dict(
                pkg=dict(
                    type="application package",
                    artifacts=[dict(dest="./", src="app/*")],
                    manifest="app/manifest.yml",
                    meta=dict(role="test_role"),
                )
            ),
        )
    )


@pytest.fixture
def enable_snowpark_glob_support_feature_flag():
    with mock.patch(
        f"snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_SNOWPARK_GLOB_SUPPORT.is_enabled",
        return_value=True,
    ), mock.patch(
        f"snowflake.cli.api.feature_flags.FeatureFlag.ENABLE_SNOWPARK_GLOB_SUPPORT.is_disabled",
        return_value=False,
    ):
        yield


@pytest.fixture
def mock_connect(mock_ctx):
    with mock.patch("snowflake.connector.connect") as _fixture:
        ctx = mock_ctx()
        _fixture.return_value = ctx
        _fixture.mocked_ctx = _fixture.return_value
        yield _fixture
