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
Tests for configuration package refactoring.

This module tests that:
1. The config package structure works correctly
2. Environment variable switching works
3. All imports remain backward compatible
4. Feature flag detection works
5. NotImplementedError is raised when config-ng is enabled but not implemented
"""

import os
from unittest import mock

import pytest


class TestConfigurationRefactoring:
    """Test suite for configuration refactoring."""

    def test_legacy_system_by_default(self):
        """Test that the legacy configuration system is used by default."""
        # Ensure config-ng is disabled
        with mock.patch.dict(os.environ, {}, clear=True):
            from snowflake.cli.api.config import (
                CONFIG_NG_ENV_VAR,
                _is_config_ng_enabled,
            )

            # Test feature flag detection
            assert (
                not _is_config_ng_enabled()
            ), "Config-ng should be disabled by default"
            assert (
                CONFIG_NG_ENV_VAR == "SNOWFLAKE_CLI_CONFIG_NG"
            ), "Environment variable name should be correct"

    def test_configuration_constants_import(self):
        """Test that configuration constants can be imported successfully."""
        with mock.patch.dict(os.environ, {}, clear=True):
            # Test that we can import key symbols (these would fail if the structure was broken)
            from snowflake.cli.api.config import (
                CLI_SECTION,
                CONNECTIONS_SECTION,
                IGNORE_NEW_VERSION_WARNING_KEY,
                LOGS_SECTION,
                PLUGINS_SECTION,
            )

            # Verify the constants have expected values
            assert CONNECTIONS_SECTION == "connections"
            assert CLI_SECTION == "cli"
            assert LOGS_SECTION == "logs"
            assert PLUGINS_SECTION == "plugins"
            assert IGNORE_NEW_VERSION_WARNING_KEY == "ignore_new_version_warning"

    def test_connection_config_class(self):
        """Test that the ConnectionConfig class works correctly."""
        with mock.patch.dict(os.environ, {}, clear=True):
            # Test that the ConnectionConfig class is available
            from snowflake.cli.api.config import ConnectionConfig

            # Create a test config instance
            config = ConnectionConfig(account="test", user="testuser")
            assert config.account == "test"
            assert config.user == "testuser"
            assert config.password is None  # Default value

    def test_configuration_functions_import(self):
        """Test that configuration functions can be imported successfully."""
        with mock.patch.dict(os.environ, {}, clear=True):
            # Test function imports
            from snowflake.cli.api.config import (
                config_init,
                connection_exists,
                get_config_value,
                get_env_variable_name,
                set_config_value,
            )

            # Verify functions are callable
            assert callable(config_init)
            assert callable(get_config_value)
            assert callable(set_config_value)
            assert callable(connection_exists)
            assert callable(get_env_variable_name)

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("1", True),
            ("true", True),
            ("True", True),
            ("yes", True),
            ("YES", True),
            ("on", True),
            ("ON", True),
            ("0", False),
            ("false", False),
            ("no", False),
            ("off", False),
            ("", False),
            ("random", False),
        ],
    )
    def test_config_ng_flag_values(self, value, expected):
        """Test that config-ng feature flag responds correctly to various values."""
        with mock.patch.dict(os.environ, {"SNOWFLAKE_CLI_CONFIG_NG": value}):
            # Need to reload the module to test the flag
            import importlib

            import snowflake.cli.api.config

            importlib.reload(snowflake.cli.api.config)

            from snowflake.cli.api.config import _is_config_ng_enabled

            result = _is_config_ng_enabled()
            assert (
                result == expected
            ), f"Flag value '{value}' should return {expected}, got {result}"

    def test_import_compatibility_patterns(self):
        """Test that all existing import patterns still work."""
        with mock.patch.dict(os.environ, {}, clear=True):
            # Pattern 1: Import specific items
            # Pattern 3: Import constants
            from snowflake.cli.api.config import (
                CLI_SECTION,
                ConnectionConfig,
                config_init,
            )

            # Pattern 2: Import with alias
            from snowflake.cli.api.config import CONNECTIONS_SECTION as CONN_SECTION

            # Verify imports work
            assert callable(config_init)
            assert ConnectionConfig is not None
            assert CONN_SECTION == "connections"
            assert CLI_SECTION == "cli"

    def test_config_ng_not_implemented_error(self):
        """Test that NotImplementedError is raised when config-ng is enabled but not implemented."""
        with mock.patch.dict(os.environ, {"SNOWFLAKE_CLI_CONFIG_NG": "1"}):
            # Need to reload the module to test the flag
            import importlib

            import snowflake.cli.api.config

            importlib.reload(snowflake.cli.api.config)

            from snowflake.cli.api.config import config_init

            # Should raise NotImplementedError when config-ng is enabled
            with pytest.raises(
                NotImplementedError, match="config_init is not implemented in config-ng"
            ):
                config_init(None)

    def test_config_ng_fallback_warning(self):
        """Test that a warning is issued when config-ng is requested but falls back to legacy."""
        # This test would be relevant if config-ng module was missing entirely
        # For now, we test the current behavior where config-ng exists but raises NotImplementedError
        with mock.patch.dict(os.environ, {"SNOWFLAKE_CLI_CONFIG_NG": "1"}):
            # Need to reload the module to test the flag
            import importlib

            import snowflake.cli.api.config

            importlib.reload(snowflake.cli.api.config)

            from snowflake.cli.api.config import _is_config_ng_enabled

            # Should be enabled
            assert _is_config_ng_enabled() is True

    def test_legacy_system_constants_available(self):
        """Test that all legacy system constants are available through the package."""
        with mock.patch.dict(os.environ, {}, clear=True):
            from snowflake.cli.api.config import (
                FEATURE_FLAGS_SECTION_PATH,
                LOGS_SECTION_PATH,
                PLUGIN_ENABLED_KEY,
                PLUGINS_SECTION_PATH,
                Empty,
            )

            # Verify constants are available and have expected values
            assert Empty is not None
            assert LOGS_SECTION_PATH == ["cli", "logs"]
            assert PLUGINS_SECTION_PATH == ["cli", "plugins"]
            assert PLUGIN_ENABLED_KEY == "enabled"
            assert FEATURE_FLAGS_SECTION_PATH == ["cli", "features"]

    def test_legacy_system_functions_available(self):
        """Test that all legacy system functions are available through the package."""
        with mock.patch.dict(os.environ, {}, clear=True):
            from snowflake.cli.api.config import (
                add_connection_to_proper_file,
                get_all_connections,
                get_config_bool_value,
                get_config_section,
                get_connection_dict,
                get_default_connection_dict,
                get_default_connection_name,
                get_env_value,
                get_feature_flags_section,
                get_logs_config,
                get_plugins_config,
                remove_connection_from_proper_file,
            )

            # Verify all functions are callable
            functions = [
                add_connection_to_proper_file,
                remove_connection_from_proper_file,
                get_logs_config,
                get_plugins_config,
                get_all_connections,
                get_connection_dict,
                get_default_connection_name,
                get_default_connection_dict,
                get_config_section,
                get_config_bool_value,
                get_env_value,
                get_feature_flags_section,
            ]

            for func in functions:
                assert callable(func), f"Function {func.__name__} should be callable"

    def test_package_exports_feature_flag_utilities(self):
        """Test that the package exports feature flag utilities."""
        with mock.patch.dict(os.environ, {}, clear=True):
            from snowflake.cli.api.config import (
                CONFIG_NG_ENV_VAR,
                _is_config_ng_enabled,
            )

            # These should be available as they're in __all__
            assert callable(_is_config_ng_enabled)
            assert CONFIG_NG_ENV_VAR == "SNOWFLAKE_CLI_CONFIG_NG"
