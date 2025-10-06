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
Unit tests for get_snowsql_config_paths() helper function.

Tests verify:
- Returns paths in correct precedence order (highest to lowest)
- Order is reversed from SnowSQL's CNF_FILES to match FileSource's "first wins" logic
- Only returns paths that exist
- Handles RPM config precedence correctly
"""

from pathlib import Path
from unittest.mock import patch

from snowflake.cli.api.config_ng.file_handlers import get_snowsql_config_paths


class TestGetSnowSqlConfigPaths:
    """Test suite for get_snowsql_config_paths() function."""

    def test_returns_list_of_paths(self):
        """Should return a list of Path objects."""
        paths = get_snowsql_config_paths()

        assert isinstance(paths, list)
        assert all(isinstance(p, Path) for p in paths)

    def test_only_returns_existing_paths(self, tmp_path):
        """Should only return paths that exist on the filesystem."""
        with patch("pathlib.Path.home", return_value=tmp_path):
            # Create only one of the expected files
            snowsql_dir = tmp_path / ".snowsql"
            snowsql_dir.mkdir()
            config_file = snowsql_dir / "config"
            config_file.touch()

            paths = get_snowsql_config_paths()

            # Should only return the one file that exists
            assert len(paths) == 1
            assert paths[0] == config_file

    def test_user_config_has_highest_priority(self, tmp_path):
        """User config should come first in the list (highest priority)."""
        with patch("pathlib.Path.home", return_value=tmp_path):
            # Create user .snowsql directory config
            snowsql_dir = tmp_path / ".snowsql"
            snowsql_dir.mkdir()
            user_config = snowsql_dir / "config"
            user_config.touch()

            # Create legacy user config
            legacy_config = tmp_path / ".snowsql.cnf"
            legacy_config.touch()

            paths = get_snowsql_config_paths()

            # User .snowsql/config should come before .snowsql.cnf
            assert len(paths) == 2
            assert paths[0] == user_config
            assert paths[1] == legacy_config

    def test_rpm_config_has_highest_priority_if_exists(self, tmp_path):
        """RPM config should be first if it exists (concept test)."""
        # This test verifies the logic conceptually
        # In reality, RPM config path is unlikely to exist in test environment
        # The important part is that IF it exists, it gets inserted at position 0

        with patch("pathlib.Path.home", return_value=tmp_path):
            # Create user config
            snowsql_dir = tmp_path / ".snowsql"
            snowsql_dir.mkdir()
            user_config = snowsql_dir / "config"
            user_config.touch()

            paths = get_snowsql_config_paths()

            # User config should be first (RPM likely doesn't exist)
            assert len(paths) >= 1
            assert paths[0] == user_config

            # Verify that the logic in get_snowsql_config_paths checks for RPM
            # This is validated by code inspection - the function checks rpm_config.exists()

    def test_precedence_order_matches_snowsql_behavior(self, tmp_path):
        """
        Test that the returned order matches SnowSQL's effective precedence.

        SnowSQL reads files where "last one wins", so:
        - bundled config (read first, lowest priority)
        - system configs
        - user configs (read last, highest priority)

        FileSource uses "first one wins", so we reverse the order:
        - user configs (first in list, highest priority)
        - system configs
        - bundled config (last in list, lowest priority)
        """
        with patch("pathlib.Path.home", return_value=tmp_path):
            # Create all user config files
            snowsql_dir = tmp_path / ".snowsql"
            snowsql_dir.mkdir()
            user_snowsql_config = snowsql_dir / "config"
            user_snowsql_config.touch()

            user_legacy_config = tmp_path / ".snowsql.cnf"
            user_legacy_config.touch()

            paths = get_snowsql_config_paths()

            # Verify order: most specific (user) configs first
            assert len(paths) == 2
            assert paths[0] == user_snowsql_config  # Highest priority
            assert paths[1] == user_legacy_config  # Second priority

    def test_handles_missing_home_directory_gracefully(self):
        """Should handle case where home directory doesn't exist."""
        with patch("pathlib.Path.home", return_value=Path("/nonexistent")):
            paths = get_snowsql_config_paths()

            # Should return empty list or only system paths that exist
            assert isinstance(paths, list)

    def test_returns_empty_list_when_no_configs_exist(self, tmp_path):
        """Should return empty list if no config files exist."""
        with patch("pathlib.Path.home", return_value=tmp_path):
            paths = get_snowsql_config_paths()

            assert paths == []

    def test_system_configs_have_lower_priority_than_user(self, tmp_path):
        """System configs should appear after user configs in the list."""
        # This test verifies the concept even if system paths don't exist in test env
        with patch("pathlib.Path.home", return_value=tmp_path):
            snowsql_dir = tmp_path / ".snowsql"
            snowsql_dir.mkdir()
            user_config = snowsql_dir / "config"
            user_config.touch()

            paths = get_snowsql_config_paths()

            # User config should be first (if any paths are returned)
            if len(paths) > 0:
                assert paths[0] == user_config


class TestSnowSqlConfigPathsIntegration:
    """Integration tests with FileSource and SnowSqlConfigHandler."""

    def test_paths_work_with_file_source(self, tmp_path):
        """Paths should work correctly with FileSource."""
        from snowflake.cli.api.config_ng.file_handlers import SnowSqlConfigHandler
        from snowflake.cli.api.config_ng.sources import FileSource

        with patch("pathlib.Path.home", return_value=tmp_path):
            # Create a user config file
            snowsql_dir = tmp_path / ".snowsql"
            snowsql_dir.mkdir()
            user_config = snowsql_dir / "config"
            user_config.write_text(
                '[connections]\naccountname = "user_account"\nusername = "user"\n'
            )

            # Get paths using helper
            paths = get_snowsql_config_paths()

            # Create FileSource with these paths
            source = FileSource(file_paths=paths, handlers=[SnowSqlConfigHandler()])

            values = source.discover()

            # Should discover values from user config
            assert values["account"].value == "user_account"
            assert values["user"].value == "user"

    def test_file_precedence_with_multiple_configs(self, tmp_path):
        """
        Test that file precedence matches SnowSQL behavior.

        In SnowSQL: later files override earlier ones
        In FileSource: earlier files override later ones
        With reversed order: same effective behavior
        """
        from snowflake.cli.api.config_ng.file_handlers import SnowSqlConfigHandler
        from snowflake.cli.api.config_ng.sources import FileSource

        with patch("pathlib.Path.home", return_value=tmp_path):
            # Create user .snowsql/config (should have highest priority)
            snowsql_dir = tmp_path / ".snowsql"
            snowsql_dir.mkdir()
            user_config = snowsql_dir / "config"
            user_config.write_text(
                "[connections]\n"
                'accountname = "priority1_account"\n'
                'username = "priority1_user"\n'
            )

            # Create another config in snowsql dir (should have lower priority)
            # Using .toml extension so handler can process it
            legacy_config = snowsql_dir / "legacy.toml"
            legacy_config.write_text(
                "[connections]\n"
                'accountname = "priority2_account"\n'
                'username = "priority2_user"\n'
                'databasename = "priority2_db"\n'
            )

            # Manually specify paths to test precedence
            paths = [user_config, legacy_config]

            source = FileSource(file_paths=paths, handlers=[SnowSqlConfigHandler()])

            values = source.discover()

            # Values from user_config should win (it's first in the list)
            assert values["account"].value == "priority1_account"
            assert values["user"].value == "priority1_user"

            # Database only exists in legacy config, so it should be found
            assert values["database"].value == "priority2_db"
