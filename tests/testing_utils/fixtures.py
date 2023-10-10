import os
import shutil
from contextlib import contextmanager

import pytest
import tempfile

from io import StringIO
from pathlib import Path
from typing import Generator, List, NamedTuple
from unittest import mock

from snowflake.connector.cursor import SnowflakeCursor
from tests.conftest import SnowCLIRunner
from tests.test_data import test_data
from tests.testing_utils.files_and_dirs import create_named_file, create_temp_file


REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"
REQUIREMENTS_TXT = "requirements.txt"
TEST_DIR = Path(__file__).parent.parent


@pytest.fixture
def app_zip(temp_dir) -> Generator:
    yield create_temp_file(".zip", temp_dir, [])


@pytest.fixture
def correct_metadata_file(temp_dir) -> Generator:
    yield create_temp_file(".yaml", temp_dir, test_data.correct_package_metadata)


@pytest.fixture
def correct_requirements_txt(temp_dir) -> Generator:
    req_txt = create_named_file(REQUIREMENTS_TXT, temp_dir, test_data.requirements)
    yield req_txt
    os.remove(req_txt)


@pytest.fixture
def correct_requirements_snowflake_txt(temp_dir) -> Generator:
    req_txt = create_named_file(
        REQUIREMENTS_SNOWFLAKE, temp_dir, test_data.requirements
    )
    yield req_txt
    os.remove(req_txt)


@pytest.fixture
def dot_packages_directory(temp_dir):
    dir_path = ".packages/totally-awesome-package"
    os.makedirs(dir_path)
    create_named_file("totally-awesome-module.py", dir_path, [])


@pytest.fixture
def include_paths_env_variable(other_directory: str) -> Generator:
    os.environ["SNOWCLI_INCLUDE_PATHS"] = other_directory
    yield os.environ["SNOWCLI_INCLUDE_PATHS"]
    os.environ.pop("SNOWCLI_INCLUDE_PATHS")


@pytest.fixture()
def mock_ctx(mock_cursor):
    class _MockConnectionCtx(mock.MagicMock):
        def __init__(self, cursor=None, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.queries: List[str] = []
            self.cs = cursor

        def get_query(self):
            return "\n".join(self.queries)

        def get_queries(self):
            return self.queries

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
            return "MockRole"

        @property
        def host(self):
            return "account.test.region.aws.snowflakecomputing.com"

        def execute_string(self, query: str):
            self.queries.append(query)
            if self.cs:
                return (self.cs,)
            else:
                return (mock_cursor(["row"], []),)

        def execute_stream(self, query: StringIO):
            return self.execute_string(query.read())

    return lambda cursor=None: _MockConnectionCtx(cursor)


@pytest.fixture
def mock_cursor():
    class MockResultMetadata(NamedTuple):
        name: str

    class _MockCursor(SnowflakeCursor):
        def __init__(self, rows: List[tuple], columns: List[str]):
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
        def description(self):
            yield from self._columns

        @classmethod
        def from_input(cls, rows, columns):
            return cls(rows, columns)

    return _MockCursor.from_input


@pytest.fixture
def other_directory():
    tmp_dir = tempfile.TemporaryDirectory()
    yield tmp_dir.name
    tmp_dir.cleanup()


@pytest.fixture
def package_file():
    with tempfile.TemporaryDirectory() as tmp:
        yield create_named_file("app.zip", tmp, [])


@pytest.fixture(scope="function")
def runner(test_snowcli_config):
    from snowcli.app.cli_app import app

    return SnowCLIRunner(app, test_snowcli_config)


@pytest.fixture
def temp_dir():
    initial_dir = os.getcwd()
    tmp = tempfile.TemporaryDirectory()
    os.chdir(tmp.name)
    yield tmp.name
    os.chdir(initial_dir)
    tmp.cleanup()


@pytest.fixture
def temp_directory_for_app_zip(temp_dir) -> Generator:
    temp_dir = tempfile.TemporaryDirectory(dir=temp_dir)
    yield temp_dir.name


@pytest.fixture
def temp_file_in_other_directory(other_directory: str) -> Generator:
    yield create_temp_file(".txt", other_directory, [])


@pytest.fixture(scope="session")
def test_snowcli_config():
    test_config = TEST_DIR / "test.toml"
    with tempfile.NamedTemporaryFile(suffix=".toml", mode="w+") as fh:
        fh.write(test_config.read_text())
        fh.flush()
        yield Path(fh.name)


@pytest.fixture(scope="session")
def test_root_path():
    return TEST_DIR


@pytest.fixture
def txt_file_in_a_subdir(temp_dir: str) -> Generator:
    subdir = tempfile.TemporaryDirectory(dir=temp_dir)
    yield create_temp_file(".txt", subdir.name, [])


@pytest.fixture
def project_file(temp_dir, test_root_path):
    @contextmanager
    def _temporary_project_file(project_name):
        test_data_file = test_root_path / "test_data" / "projects" / project_name
        shutil.copytree(test_data_file, temp_dir, dirs_exist_ok=True)
        yield Path(temp_dir)

    return _temporary_project_file
