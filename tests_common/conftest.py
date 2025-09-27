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
import os
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from unittest import mock

import pytest
import yaml

from snowflake.cli._plugins.streamlit.streamlit_entity import StreamlitEntity
from snowflake.cli._plugins.streamlit.streamlit_entity_model import StreamlitEntityModel
from snowflake.cli._plugins.workspace.context import WorkspaceContext, ActionContext
from snowflake.cli.api.console.abc import AbstractConsole
from snowflake.cli.api.constants import PYTHON_3_12

PROJECT_DIR = Path(__file__).parent / "test_data" / "projects"


@pytest.fixture
def temporary_directory():
    initial_dir = os.getcwd()

    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            os.chdir(tmp_dir)
            yield tmp_dir
        finally:
            os.chdir(initial_dir)


# Borrowed from tests_integration/test_utils.py
# TODO: remove from here when testing utils become shared
# TODO: contextlib.chdir isn't available before Python 3.11, so this is an alternative for older versions
@contextmanager
def change_directory(directory: Path):
    cwd = os.getcwd()
    os.chdir(directory)
    try:
        yield directory
    finally:
        os.chdir(cwd)


@pytest.fixture()
def workspace_context():
    return WorkspaceContext(
        console=mock.MagicMock(spec=AbstractConsole),
        project_root=Path().resolve(),
        get_default_role=lambda: "mock_role",
        get_default_warehouse=lambda: "mock_warehouse",
    )


@pytest.fixture()
def action_context():
    return ActionContext(get_entity=lambda *args: None)


@pytest.fixture
def example_entity(project_directory, workspace_context):
    with project_directory("example_streamlit_v2") as pdir:
        with Path(pdir / "snowflake.yml").open() as definition_file:
            definition = yaml.safe_load(definition_file)
            model = StreamlitEntityModel(
                **definition.get("entities", {}).get("test_streamlit")
            )
            model.set_entity_id("test_streamlit")

            yield StreamlitEntity(workspace_ctx=workspace_context, entity_model=model)


@pytest.fixture
def snowflake_home(monkeypatch):
    """
    Provide isolated config environment for testing.

    Uses the hybrid approach with dependency injection and minimal module reloading.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        snowflake_home = Path(tmp_dir) / ".snowflake"
        snowflake_home.mkdir()

        # Set environment variable for any code that reads it directly
        monkeypatch.setenv("SNOWFLAKE_HOME", str(snowflake_home))

        # We still need to reload the constants module to update CONFIG_FILE and CONNECTIONS_FILE
        # This is necessary for permission checking which uses these global constants
        import importlib
        import snowflake.connector.constants  # Import first

        importlib.reload(snowflake.connector.constants)

        # Just provide the isolated snowflake_home directory and updated constants
        # Don't use isolated_config here as it interferes with tests that use specific config files
        # Tests that need isolated CONFIG_MANAGER state should use isolated_config explicitly
        yield snowflake_home


@pytest.fixture
def reset_config_manager():
    """
    Fixture that completely resets CONFIG_MANAGER state before each test.

    This ensures clean config state for tests that need to manipulate connections.toml
    or other config manager state without interference from previous tests.

    Usage:
        def test_something(reset_config_manager):
            # CONFIG_MANAGER is now in a clean state
            config_init(some_config_file)
            # ... test logic
    """
    from snowflake.cli.api.config.legacy import reset_config_manager_completely

    # Reset before the test runs
    reset_config_manager_completely()

    yield

    # Optionally reset after the test as well for extra cleanliness
    reset_config_manager_completely()


@pytest.fixture
def clean_config_manager():
    """
    Fixture that provides a clean CONFIG_MANAGER with force reload capability.

    This is specifically for tests that need to test connections.toml override behavior.
    It returns a tuple of (CONFIG_MANAGER, force_reload_function) for convenience.

    Usage:
        def test_something(clean_config_manager):
            CONFIG_MANAGER, force_reload = clean_config_manager
            # Create connections.toml
            config_init(some_file)
            force_reload()  # Ensure connections.toml is detected
            # ... test logic
    """
    from snowflake.cli.api.config.legacy import (
        reset_config_manager_completely,
        get_config_manager,
    )

    # Reset before the test runs
    reset_config_manager_completely()

    config_manager = get_config_manager()
    force_reload = config_manager.force_reload

    yield config_manager, force_reload

    # Reset after the test as well for extra cleanliness
    reset_config_manager_completely()


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


skip_snowpark_on_newest_python = pytest.mark.skipif(
    sys.version_info >= PYTHON_3_12,
    reason="requires python3.11 or lower",
)
