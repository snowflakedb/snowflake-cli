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

"""
Configuration testing utilities for config_ng tests.

Provides fixtures for setting up temporary configuration environments.
"""

import copy
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from textwrap import dedent
from typing import Dict, Optional

import pytest


@contextmanager
def _temp_environment(env_vars: Dict[str, str]):
    """
    Context manager for temporarily setting environment variables.

    Saves the entire environment, applies new variables, then restores
    the original environment completely on exit.

    Args:
        env_vars: Dictionary of environment variables to set

    Yields:
        None
    """
    original_env = copy.deepcopy(dict(os.environ))
    try:
        os.environ.update(env_vars)
        yield
    finally:
        os.environ.clear()
        os.environ.update(original_env)


@pytest.fixture
def config_ng_setup():
    """
    Fixture that provides a context manager for setting up config_ng test environments.

    Returns a context manager function that:
    1. Creates temp SNOWFLAKE_HOME
    2. Writes config files
    3. Sets env vars
    4. Enables config_ng
    5. Resets provider
    6. Yields (test can now call get_connection_dict())
    7. Cleans up

    Usage:
        def test_something(config_ng_setup):
            with config_ng_setup(
                cli_config="[connections.test]\\naccount = 'test'",
                env_vars={"SNOWFLAKE_USER": "alice"}
            ):
                from snowflake.cli.api.config import get_connection_dict
                conn = get_connection_dict("test")
                assert conn["account"] == "test"

    Args (to returned context manager):
        snowsql_config: SnowSQL INI config content (will be dedented)
        cli_config: CLI TOML config content (will be dedented)
        connections_toml: Connections TOML content (will be dedented)
        env_vars: Environment variables to set
    """

    @contextmanager
    def _setup(
        snowsql_config: Optional[str] = None,
        cli_config: Optional[str] = None,
        connections_toml: Optional[str] = None,
        env_vars: Optional[Dict[str, str]] = None,
    ):
        with tempfile.TemporaryDirectory() as tmpdir:
            snowflake_home = Path(tmpdir) / ".snowflake"
            snowflake_home.mkdir()

            # Write config files if provided
            if snowsql_config:
                (snowflake_home / "config").write_text(dedent(snowsql_config))
            if cli_config:
                (snowflake_home / "config.toml").write_text(dedent(cli_config))
            if connections_toml:
                (snowflake_home / "connections.toml").write_text(
                    dedent(connections_toml)
                )

            # Prepare environment variables
            env_to_set = {
                "SNOWFLAKE_HOME": str(snowflake_home),
                "SNOWFLAKE_CLI_CONFIG_V2_ENABLED": "true",
            }
            if env_vars:
                env_to_set.update(env_vars)

            # Set up environment and run test
            with _temp_environment(env_to_set):
                # Clear config_file_override to use SNOWFLAKE_HOME instead
                from snowflake.cli.api.cli_global_context import (
                    get_cli_context_manager,
                )

                cli_ctx_mgr = get_cli_context_manager()
                original_config_override = cli_ctx_mgr.config_file_override
                cli_ctx_mgr.config_file_override = None

                try:
                    # Reset config provider to use new config
                    from snowflake.cli.api.config_provider import reset_config_provider

                    reset_config_provider()

                    yield

                finally:
                    # Restore config_file_override
                    if original_config_override is not None:
                        cli_ctx_mgr = get_cli_context_manager()
                        cli_ctx_mgr.config_file_override = original_config_override

                    # Reset config provider
                    reset_config_provider()

    return _setup
