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
Integration tests for environment variable handler migration scenarios.

Tests verify:
- Migration from SnowSQL to SnowCLI environment variables
- Handler ordering (SNOWFLAKE_* overrides SNOWSQL_*)
- Fallback behavior for unmigrated keys
- Complete migration scenarios
"""

import os
from unittest.mock import patch

from snowflake.cli.api.config_ng.env_handlers import (
    SnowCliEnvHandler,
    SnowSqlEnvHandler,
)
from snowflake.cli.api.config_ng.sources import EnvironmentSource


class TestEnvironmentHandlerMigration:
    """Test suite for environment variable migration scenarios."""

    def test_pure_snowsql_environment(self):
        """Scenario: User has only SNOWSQL_* environment variables."""
        env_vars = {
            "SNOWSQL_ACCOUNT": "old_account",
            "SNOWSQL_USER": "old_user",
            "SNOWSQL_PWD": "old_password",
            "SNOWSQL_WAREHOUSE": "old_warehouse",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            # Setup source with both handlers (SnowCLI first, SnowSQL second)
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )

            values = source.discover()

            # All values should come from SnowSQL handler
            assert len(values) == 4
            assert values["account"].value == "old_account"
            assert values["account"].source_name == "snowsql_env"
            assert values["user"].value == "old_user"
            assert values["password"].value == "old_password"  # Mapped from PWD
            assert values["warehouse"].value == "old_warehouse"

    def test_pure_snowflake_cli_environment(self):
        """Scenario: User has migrated to SNOWFLAKE_* environment variables."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "new_account",
            "SNOWFLAKE_USER": "new_user",
            "SNOWFLAKE_PASSWORD": "new_password",
            "SNOWFLAKE_WAREHOUSE": "new_warehouse",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )

            values = source.discover()

            # All values should come from SnowCLI handler
            assert len(values) == 4
            assert values["account"].value == "new_account"
            assert values["account"].source_name == "snowflake_cli_env"
            assert values["user"].value == "new_user"
            assert values["password"].value == "new_password"
            assert values["warehouse"].value == "new_warehouse"

    def test_partial_migration_snowflake_overrides_snowsql(self):
        """
        Scenario: User is migrating - some SNOWFLAKE_* vars override SNOWSQL_*.
        This is the key migration scenario.
        """
        env_vars = {
            # Legacy SnowSQL vars (complete set)
            "SNOWSQL_ACCOUNT": "old_account",
            "SNOWSQL_USER": "old_user",
            "SNOWSQL_PWD": "old_password",
            "SNOWSQL_WAREHOUSE": "old_warehouse",
            "SNOWSQL_DATABASE": "old_database",
            # New SnowCLI vars (partial migration)
            "SNOWFLAKE_ACCOUNT": "new_account",
            "SNOWFLAKE_USER": "new_user",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            # Handler order: SnowCLI first (higher priority), SnowSQL second (fallback)
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )

            values = source.discover()

            # Migrated keys should use SNOWFLAKE_* values
            assert values["account"].value == "new_account"
            assert values["account"].source_name == "snowflake_cli_env"
            assert values["user"].value == "new_user"
            assert values["user"].source_name == "snowflake_cli_env"

            # Unmigrated keys should fallback to SNOWSQL_* values
            assert values["password"].value == "old_password"
            assert values["password"].source_name == "snowsql_env"
            assert values["warehouse"].value == "old_warehouse"
            assert values["warehouse"].source_name == "snowsql_env"
            assert values["database"].value == "old_database"
            assert values["database"].source_name == "snowsql_env"

    def test_migration_with_pwd_to_password_mapping(self):
        """
        Scenario: User migrates from SNOWSQL_PWD to SNOWFLAKE_PASSWORD.
        Tests the key mapping during migration.
        """
        env_vars = {
            "SNOWSQL_PWD": "old_password",
            "SNOWFLAKE_PASSWORD": "new_password",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )

            values = source.discover()

            # SNOWFLAKE_PASSWORD should override SNOWSQL_PWD
            assert len(values) == 1  # Only "password" key
            assert values["password"].value == "new_password"
            assert values["password"].source_name == "snowflake_cli_env"

    def test_migration_only_pwd_remains_in_snowsql(self):
        """
        Scenario: User has migrated everything except password.
        SNOWSQL_PWD should still work as fallback.
        """
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "new_account",
            "SNOWFLAKE_USER": "new_user",
            "SNOWSQL_PWD": "old_password",  # Not yet migrated
        }

        with patch.dict(os.environ, env_vars, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )

            values = source.discover()

            assert values["account"].value == "new_account"
            assert values["account"].source_name == "snowflake_cli_env"
            assert values["user"].value == "new_user"
            assert values["user"].source_name == "snowflake_cli_env"
            # Password from SnowSQL (mapped from PWD)
            assert values["password"].value == "old_password"
            assert values["password"].source_name == "snowsql_env"

    def test_both_handlers_provide_different_keys(self):
        """
        Scenario: Each handler provides unique keys that don't overlap.
        """
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "new_account",
            "SNOWSQL_WAREHOUSE": "old_warehouse",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )

            values = source.discover()

            assert len(values) == 2
            assert values["account"].source_name == "snowflake_cli_env"
            assert values["warehouse"].source_name == "snowsql_env"

    def test_handler_order_matters(self):
        """
        Verify that handler order determines precedence.
        First handler with value wins.
        """
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "snowflake_value",
            "SNOWSQL_ACCOUNT": "snowsql_value",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            # Test SnowCLI first (correct order for migration)
            source_snowcli_first = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )
            values = source_snowcli_first.discover()
            assert values["account"].value == "snowflake_value"

            # Test SnowSQL first (wrong order, but tests the mechanism)
            source_snowsql_first = EnvironmentSource(
                handlers=[SnowSqlEnvHandler(), SnowCliEnvHandler()]
            )
            values = source_snowsql_first.discover()
            assert values["account"].value == "snowsql_value"

    def test_discover_specific_key_with_both_handlers(self):
        """Should discover specific key considering both handlers."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "new_account",
            "SNOWSQL_USER": "old_user",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )

            # Discover account - should get from SnowCLI
            values = source.discover(key="account")
            assert values["account"].value == "new_account"

            # Discover user - should get from SnowSQL
            values = source.discover(key="user")
            assert values["user"].value == "old_user"

    def test_empty_environment_both_handlers(self):
        """With no environment variables, both handlers should return nothing."""
        with patch.dict(os.environ, {}, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )

            values = source.discover()
            assert len(values) == 0

    def test_complete_migration_timeline(self):
        """
        Simulates a complete migration timeline from Step 1 to Step 4.
        """
        # Step 1: Pure SnowSQL user
        env_step1 = {"SNOWSQL_ACCOUNT": "account"}
        with patch.dict(os.environ, env_step1, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )
            values = source.discover()
            assert values["account"].value == "account"
            assert values["account"].source_name == "snowsql_env"

        # Step 2: Start migration - add SNOWFLAKE_ACCOUNT
        env_step2 = {
            "SNOWSQL_ACCOUNT": "old_account",
            "SNOWFLAKE_ACCOUNT": "new_account",
        }
        with patch.dict(os.environ, env_step2, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )
            values = source.discover()
            # SNOWFLAKE_* should win
            assert values["account"].value == "new_account"
            assert values["account"].source_name == "snowflake_cli_env"

        # Step 3: SNOWSQL_ACCOUNT still present but ignored
        env_step3 = {
            "SNOWSQL_ACCOUNT": "old_account",  # Still set but ignored
            "SNOWFLAKE_ACCOUNT": "new_account",
        }
        with patch.dict(os.environ, env_step3, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )
            values = source.discover()
            # Still uses SNOWFLAKE_*
            assert values["account"].value == "new_account"
            assert values["account"].source_name == "snowflake_cli_env"

        # Step 4: Complete migration - remove SNOWSQL_ACCOUNT
        env_step4 = {"SNOWFLAKE_ACCOUNT": "new_account"}
        with patch.dict(os.environ, env_step4, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )
            values = source.discover()
            # Uses SNOWFLAKE_* (no change in behavior from step 3)
            assert values["account"].value == "new_account"
            assert values["account"].source_name == "snowflake_cli_env"

    def test_mixed_types_from_both_handlers(self):
        """Should handle different value types from both handlers."""
        env_vars = {
            "SNOWFLAKE_ACCOUNT": "my_account",  # String
            "SNOWFLAKE_PORT": "443",  # Integer
            "SNOWSQL_ENABLE_DIAG": "true",  # Boolean
            "SNOWSQL_TIMEOUT": "30",  # Integer from SnowSQL
        }

        with patch.dict(os.environ, env_vars, clear=True):
            source = EnvironmentSource(
                handlers=[SnowCliEnvHandler(), SnowSqlEnvHandler()]
            )

            values = source.discover()

            assert isinstance(values["account"].value, str)
            assert isinstance(values["port"].value, int)
            assert isinstance(values["enable_diag"].value, bool)
            assert isinstance(values["timeout"].value, int)
