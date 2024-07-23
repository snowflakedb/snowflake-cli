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
import importlib
import os
import shutil
import sys
import tempfile
from contextlib import contextmanager
from io import StringIO
from pathlib import Path
from typing import Any, Dict, Generator, List, NamedTuple, Optional, Union
from unittest import mock

import pytest
import yaml
from snowflake.cli.api.project.schemas.project_definition import (
    build_project_definition,
)
from snowflake.cli.api.project.schemas.snowpark.argument import Argument
from snowflake.cli.api.project.schemas.snowpark.callable import FunctionSchema
from snowflake.cli.app.cli_app import app_factory
from snowflake.cli.plugins.nativeapp.codegen.snowpark.models import (
    NativeAppExtensionFunction,
)
from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.errors import ProgrammingError
from typer import Typer
from typer.testing import CliRunner

from tests.test_data import test_data
from tests.testing_utils.files_and_dirs import (
    create_named_file,
    create_temp_file,
    merge_left,
)
from tests_common import IS_WINDOWS

REQUIREMENTS_SNOWFLAKE = "requirements.snowflake.txt"
REQUIREMENTS_TXT = "requirements.txt"
TEST_DIR = Path(__file__).parent.parent


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


@pytest.fixture()
def mock_ctx(mock_cursor):
    def _mock_connection_ctx_factory(cursor=mock_cursor(["row"], []), **kwargs):
        kwargs["cursor"] = cursor
        return MockConnectionCtx(**kwargs)

    yield _mock_connection_ctx_factory


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
    app = app_factory()
    yield SnowCLIRunner(app, test_snowcli_config)


@pytest.fixture
def temp_dir():
    initial_dir = os.getcwd()
    tmp = tempfile.TemporaryDirectory()
    os.chdir(tmp.name)
    yield tmp.name
    os.chdir(initial_dir)
    tmp.cleanup()


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


@pytest.fixture
def temp_directory_for_app_zip(temp_dir) -> Generator:
    temp_dir = tempfile.TemporaryDirectory(dir=temp_dir)
    yield temp_dir.name


@pytest.fixture(scope="session")
def test_snowcli_config():
    test_config = TEST_DIR / "test.toml"
    with _named_temporary_file(suffix=".toml") as p:
        p.write_text(test_config.read_text())
        p.chmod(0o777)
        yield p


@pytest.fixture(scope="session")
def test_root_path():
    return TEST_DIR


@pytest.fixture(scope="session")
def test_projects_path(test_root_path):
    return test_root_path / "test_data" / "projects"


@pytest.fixture
def txt_file_in_a_subdir(temp_dir: str) -> Generator:
    subdir = tempfile.TemporaryDirectory(dir=temp_dir)
    yield create_temp_file(".txt", subdir.name, [])


@pytest.fixture
def project_directory(temp_dir, test_projects_path):
    @contextmanager
    def _temporary_project_directory(
        project_name, merge_project_definition: Optional[dict] = None
    ):
        test_data_file = test_projects_path / project_name
        shutil.copytree(test_data_file, temp_dir, dirs_exist_ok=True)
        if merge_project_definition:
            project_definition = yaml.load(
                Path("snowflake.yml").read_text(), Loader=yaml.BaseLoader
            )
            merge_left(project_definition, merge_project_definition)
            with open(Path(temp_dir) / "snowflake.yml", "w") as file:
                file.write(yaml.dump(project_definition))

        yield Path(temp_dir)

    return _temporary_project_directory


@pytest.fixture(autouse=True)
def global_setup(monkeypatch):
    width = 81 if IS_WINDOWS else 80
    monkeypatch.setenv("COLUMNS", str(width))


@pytest.fixture
def snowflake_home(monkeypatch):
    """
    Set up the default location of config files to [temp_dir]/.snowflake
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        snowflake_home = Path(tmp_dir) / ".snowflake"
        snowflake_home.mkdir()
        monkeypatch.setenv("SNOWFLAKE_HOME", str(snowflake_home))
        for module in [
            sys.modules["snowflake.connector.constants"],
            sys.modules["snowflake.connector.config_manager"],
            sys.modules["snowflake.cli.api.config"],
        ]:
            importlib.reload(module)

        yield snowflake_home


@pytest.fixture
def alter_snowflake_yml():
    def _update(snowflake_yml_path: Path, parameter_path: str, value=None):
        import yaml

        with open(snowflake_yml_path) as fh:
            yml = yaml.safe_load(fh)

        parts = parameter_path.split(".")
        current_object = yml
        while parts:
            part = parts.pop(0)
            evaluated_part = int(part) if part.isdigit() else part

            if parts:
                if isinstance(current_object, dict):
                    current_object = current_object.setdefault(evaluated_part, {})
                else:
                    current_object = current_object[evaluated_part]
            else:
                current_object[evaluated_part] = value

        with open(snowflake_yml_path, "w+") as fh:
            yaml.safe_dump(yml, fh)

    return _update


@pytest.fixture()
def argument_instance():
    return Argument(name="Foo", type="Bar")


@pytest.fixture()
def function_instance():
    return FunctionSchema(
        name="func1",
        handler="app.func1_handler",
        signature=[{"name": "a", "type": "string"}, {"name": "b", "type": "variant"}],
        returns="string",
    )


@pytest.fixture()
def native_app_project_instance():
    return build_project_definition(
        **{
            "definition_version": "1",
            "native_app": {
                "artifacts": [{"dest": "./", "src": "app/*"}],
                "name": "napp_test",
                "package": {
                    "scripts": [
                        "package/001.sql",
                    ]
                },
            },
        }
    )


@pytest.fixture
def native_app_extension_function_raw_data() -> Dict[str, Any]:
    return {
        "type": "procedure",
        "lineno": 42,
        "name": "my_function",
        "signature": [{"name": "first", "type": "int", "default": "42"}],
        "returns": "int",
        "runtime": "3.11",
        "handler": "my_function_handler",
        "external_access_integrations": ["integration_one", "integration_two"],
        "secrets": {"key1": "secret_one", "key2": "integration_two"},
        "packages": ["package_one==1.0.2", "package_two"],
        "imports": ["/path/to/import1.py", "/path/to/import2.zip"],
        "execute_as_caller": False,
        "schema": "DATA",
        "application_roles": ["APP_ADMIN", "APP_VIEWER"],
    }


@pytest.fixture
def native_app_extension_function(
    native_app_extension_function_raw_data,
) -> NativeAppExtensionFunction:
    return NativeAppExtensionFunction(**native_app_extension_function_raw_data)


@pytest.fixture
def mock_procedure_description(mock_cursor):
    yield mock_cursor(
        rows=[
            ("signature", "(NAME VARCHAR)"),
            ("returns", "VARCHAR(16777216)"),
            ("language", "PYTHON"),
            ("null handling", "CALLED ON NULL INPUT"),
            ("volatility", "VOLATILE"),
            ("execute as", "CALLER"),
            ("body", None),
            ("imports", "[@FOO.BAR.BAZ/my_snowpark_project/app.zip]"),
            ("handler", "app.hello_procedure"),
            ("runtime_version", "3.8"),
            ("packages", "['snowflake-snowpark-python','pytest<9.0.0,>=7.0.0']"),
            ("installed_packages", "['_libgcc_mutex==0.1']"),
        ],
        columns=[
            "signature",
            "returns",
            "language",
            "null handling",
            "volatility",
            "execute as",
            "body",
            "imports",
            "handler",
            "runtime_version",
            "packages",
            "installed_packages",
        ],
    )
