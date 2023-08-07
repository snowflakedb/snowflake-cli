from __future__ import annotations

import functools
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import List, NamedTuple
from unittest import mock

import pytest

from typer import Typer
from typer.testing import CliRunner

TEST_DIR = Path(__file__).parent


@pytest.fixture(scope="session")
def test_snowcli_config():
    test_config = TEST_DIR / "test.toml"
    with NamedTemporaryFile(suffix=".toml", mode="w+") as fh:
        fh.write(test_config.read_text())
        fh.flush()
        yield Path(fh.name)


class SnowCLIRunner(CliRunner):
    def __init__(self, app: Typer, test_snowcli_config: str):
        super().__init__()
        self.app = app
        self.test_snowcli_config = test_snowcli_config

    @functools.wraps(CliRunner.invoke)
    def invoke(self, *a, **kw):
        kw.update(catch_exceptions=False)
        return super().invoke(self.app, *a, **kw)

    def invoke_with_config(self, *args, **kwargs):
        return self.invoke(
            ["--config-file", self.test_snowcli_config, *args[0]],
            **kwargs,
        )


@pytest.fixture(scope="function")
def runner(test_snowcli_config):
    from snowcli.cli.app import app

    return SnowCLIRunner(app, test_snowcli_config)


@pytest.fixture
def mock_cursor():
    class MockResultMetadata(NamedTuple):
        name: str

    class _MockCursor:
        def __init__(self, rows: List[tuple], columns: List[str]):
            self._rows = rows
            self._columns = [MockResultMetadata(c) for c in columns]

        def fetchone(self):
            return self.fetchall()

        def fetchall(self):
            yield from self._rows

        @property
        def description(self):
            yield from self._columns

        @classmethod
        def from_input(cls, rows, columns):
            return cls(rows, columns)

    return _MockCursor.from_input


@pytest.fixture()
def mock_ctx(mock_cursor):
    class _MockConnectionCtx(mock.MagicMock):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.queries: List[str] = []

        def get_query(self):
            return self.queries[0]

        @property
        def warehouse(self):
            return "MockWarehouse"

        @property
        def database(self):
            return "MockDatabase"

        @property
        def schema(self):
            return "MockSchema"

        @property
        def role(self):
            return "mockRole"

        def execute_string(self, query: str):
            self.queries.append(query)
            return (mock_cursor(["row"], []),)

        def execute_stream(self, query: StringIO):
            return self.execute_string(query.read())

    return _MockConnectionCtx()
