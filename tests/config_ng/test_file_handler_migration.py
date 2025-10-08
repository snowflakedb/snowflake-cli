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
Integration tests for file handler migration scenarios.

Tests verify:
- File path precedence (first file wins)
- Handler ordering (TOML > SnowSQL)
- Migration from SnowSQL to SnowCLI TOML
- Complete integration with FileSource
"""

from pathlib import Path
from tempfile import NamedTemporaryFile

from snowflake.cli.api.config_ng.file_handlers import (
    IniFileHandler,
    TomlFileHandler,
)
from snowflake.cli.api.config_ng.sources import FileSource


class TestFileHandlerMigration:
    """Test suite for file handler migration scenarios."""

    def test_pure_toml_configuration(self):
        """Scenario: User has only SnowCLI TOML configuration."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('[default]\naccount = "toml_account"\nuser = "toml_user"\n')
            f.flush()
            toml_path = Path(f.name)

        try:
            source = FileSource(
                file_paths=[toml_path],
                handlers=[
                    TomlFileHandler(section_path=["default"]),
                    IniFileHandler(),
                ],
            )

            values = source.discover()

            assert len(values) == 2
            assert values["account"].value == "toml_account"
            assert values["account"].source_name == "toml:default"
            assert values["user"].value == "toml_user"
        finally:
            toml_path.unlink()

    def test_pure_snowsql_configuration(self):
        """Scenario: User has only SnowSQL configuration."""
        with NamedTemporaryFile(mode="w", suffix=".cnf", delete=False) as f:
            f.write(
                "[connections]\n"
                "accountname = snowsql_account\n"
                "username = snowsql_user\n"
            )
            f.flush()
            snowsql_path = Path(f.name)

        try:
            source = FileSource(
                file_paths=[snowsql_path],
                handlers=[
                    TomlFileHandler(section_path=["default"]),
                    IniFileHandler(),
                ],
            )

            values = source.discover()

            # Values should come from SnowSQL with key mapping
            assert len(values) == 2
            assert values["account"].value == "snowsql_account"
            assert values["account"].source_name == "snowsql_config"
            assert values["user"].value == "snowsql_user"
        finally:
            snowsql_path.unlink()

    def test_partial_migration_toml_overrides_snowsql(self):
        """Scenario: User has both configs, TOML should override SnowSQL."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f1:
            f1.write('[default]\naccount = "new_account"\n')
            f1.flush()
            toml_path = Path(f1.name)

        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f2:
            f2.write(
                "[connections]\n"
                "accountname = old_account\n"
                "username = old_user\n"
                "databasename = old_db\n"
            )
            f2.flush()
            snowsql_path = Path(f2.name)

        try:
            # First file path has highest precedence
            source = FileSource(
                file_paths=[toml_path, snowsql_path],
                handlers=[
                    TomlFileHandler(section_path=["default"]),
                    IniFileHandler(),
                ],
            )

            values = source.discover()

            # account from TOML (first file), others from SnowSQL (second file)
            assert values["account"].value == "new_account"
            assert values["account"].source_name == "toml:default"
            assert values["user"].value == "old_user"
            assert values["user"].source_name == "snowsql_config"
            assert values["database"].value == "old_db"
        finally:
            toml_path.unlink()
            snowsql_path.unlink()

    def test_handler_ordering_within_same_file(self):
        """Handler order matters when both can handle same file."""
        # Create a pure TOML file that both handlers could potentially read
        # TomlFileHandler will read [default], IniFileHandler will read [connections]
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            # Pure TOML format file with both sections
            f.write(
                '[default]\naccount = "toml_format"\n'
                '[connections]\naccount = "other_format"\n'
            )
            f.flush()
            temp_path = Path(f.name)

        try:
            # TOML handler first - should find account in [default]
            source = FileSource(
                file_paths=[temp_path],
                handlers=[
                    TomlFileHandler(section_path=["default"]),
                    TomlFileHandler(section_path=["connections"]),
                ],
            )

            values = source.discover()

            # First TOML handler should win (reads [default])
            assert values["account"].value == "toml_format"
            assert values["account"].source_name == "toml:default"
        finally:
            temp_path.unlink()

    def test_file_path_precedence_first_wins(self):
        """First file path should take precedence over later ones."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f1:
            f1.write('[default]\naccount = "file1_account"\n')
            f1.flush()
            file1_path = Path(f1.name)

        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f2:
            f2.write('[default]\naccount = "file2_account"\n')
            f2.flush()
            file2_path = Path(f2.name)

        try:
            source = FileSource(
                file_paths=[file1_path, file2_path],
                handlers=[TomlFileHandler(section_path=["default"])],
            )

            values = source.discover()

            # First file wins
            assert values["account"].value == "file1_account"
        finally:
            file1_path.unlink()
            file2_path.unlink()

    def test_nonexistent_files_skipped(self):
        """Should skip nonexistent files gracefully."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('[default]\naccount = "existing_account"\n')
            f.flush()
            existing_path = Path(f.name)

        nonexistent_path = Path("/nonexistent/file.toml")

        try:
            source = FileSource(
                file_paths=[nonexistent_path, existing_path],
                handlers=[TomlFileHandler(section_path=["default"])],
            )

            values = source.discover()

            # Should still get values from existing file
            assert values["account"].value == "existing_account"
        finally:
            existing_path.unlink()

    def test_complete_migration_timeline(self):
        """Simulates complete migration from SnowSQL to TOML."""
        # Step 1: Pure SnowSQL user
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write("[connections]\naccountname = account\nusername = user\n")
            f.flush()
            snowsql_path = Path(f.name)

        try:
            source = FileSource(
                file_paths=[snowsql_path],
                handlers=[
                    TomlFileHandler(section_path=["default"]),
                    IniFileHandler(),
                ],
            )

            values = source.discover()
            assert values["account"].value == "account"
            assert values["account"].source_name == "snowsql_config"
        finally:
            snowsql_path.unlink()

        # Step 2: Start migration - create TOML file
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f1:
            f1.write('[default]\naccount = "new_account"\n')
            f1.flush()
            toml_path = Path(f1.name)

        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f2:
            f2.write("[connections]\naccountname = old_account\nusername = old_user\n")
            f2.flush()
            snowsql_path = Path(f2.name)

        try:
            source = FileSource(
                file_paths=[toml_path, snowsql_path],
                handlers=[
                    TomlFileHandler(section_path=["default"]),
                    IniFileHandler(),
                ],
            )

            values = source.discover()
            # TOML overrides account, SnowSQL provides user
            assert values["account"].value == "new_account"
            assert values["account"].source_name == "toml:default"
            assert values["user"].value == "old_user"
            assert values["user"].source_name == "snowsql_config"
        finally:
            toml_path.unlink()
            snowsql_path.unlink()

    def test_multiple_toml_handlers_different_sections(self):
        """Should handle multiple TOML handlers for different sections."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                '[default]\naccount = "default_account"\n'
                '[prod]\naccount = "prod_account"\n'
            )
            f.flush()
            temp_path = Path(f.name)

        try:
            # Handler for [default] section
            source_default = FileSource(
                file_paths=[temp_path],
                handlers=[TomlFileHandler(section_path=["default"])],
            )

            # Handler for [prod] section
            source_prod = FileSource(
                file_paths=[temp_path],
                handlers=[TomlFileHandler(section_path=["prod"])],
            )

            values_default = source_default.discover()
            values_prod = source_prod.discover()

            assert values_default["account"].value == "default_account"
            assert values_prod["account"].value == "prod_account"
        finally:
            temp_path.unlink()

    def test_discover_specific_key_with_migration(self):
        """Should handle specific key discovery with migration."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f1:
            f1.write('[default]\naccount = "toml_account"\n')
            f1.flush()
            toml_path = Path(f1.name)

        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f2:
            f2.write("[connections]\naccountname = snowsql_account\nusername = user\n")
            f2.flush()
            snowsql_path = Path(f2.name)

        try:
            source = FileSource(
                file_paths=[toml_path, snowsql_path],
                handlers=[
                    TomlFileHandler(section_path=["default"]),
                    IniFileHandler(),
                ],
            )

            # Discover specific key
            values = source.discover(key="account")

            # Should get from TOML (first file)
            assert len(values) == 1
            assert values["account"].value == "toml_account"

            # Discover different key
            values = source.discover(key="user")

            # Should get from SnowSQL (second file)
            assert len(values) == 1
            assert values["user"].value == "user"
        finally:
            toml_path.unlink()
            snowsql_path.unlink()

    def test_complex_configuration_with_all_features(self):
        """Complex scenario with multiple files, handlers, and sections."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f1:
            f1.write('[connections]\naccount = "connections_account"\n')
            f1.flush()
            connections_toml = Path(f1.name)

        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f2:
            f2.write("[cli]\nverbose = true\n")
            f2.flush()
            config_toml = Path(f2.name)

        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f3:
            f3.write(
                "[connections]\naccountname = legacy_account\nusername = legacy_user\n"
            )
            f3.flush()
            snowsql_config = Path(f3.name)

        try:
            source = FileSource(
                file_paths=[connections_toml, config_toml, snowsql_config],
                handlers=[
                    TomlFileHandler(section_path=["connections"]),
                    TomlFileHandler(section_path=["cli"]),
                    IniFileHandler(),
                ],
            )

            values = source.discover()

            # Should get account from connections.toml (first file, first handler)
            assert values["account"].value == "connections_account"
            assert values["account"].source_name == "toml:connections"

            # Should get verbose from config.toml (second file, second handler)
            assert values["verbose"].value is True
            assert values["verbose"].source_name == "toml:cli"

            # Should get user from snowsql config (third file, third handler)
            assert values["user"].value == "legacy_user"
            assert values["user"].source_name == "snowsql_config"
        finally:
            connections_toml.unlink()
            config_toml.unlink()
            snowsql_config.unlink()
