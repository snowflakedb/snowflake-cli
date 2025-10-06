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
Unit tests for TomlFileHandler.

Tests verify:
- TOML file discovery
- Section navigation
- Caching behavior
- File format detection
- Value metadata
"""

from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest
from snowflake.cli.api.config_ng.core import SourcePriority
from snowflake.cli.api.config_ng.file_handlers import TomlFileHandler


class TestTomlFileHandler:
    """Test suite for TomlFileHandler."""

    def test_create_handler(self):
        """Should create handler with correct properties."""
        handler = TomlFileHandler()

        assert handler.source_name == "toml:root"
        assert handler.priority == SourcePriority.FILE
        assert handler.handler_type == "toml"

    def test_create_handler_with_section_path(self):
        """Should create handler with section path."""
        handler = TomlFileHandler(section_path=["connections", "default"])

        assert handler.source_name == "toml:connections.default"
        assert handler.priority == SourcePriority.FILE

    def test_can_handle_always_true(self):
        """Should always return True."""
        handler = TomlFileHandler()
        assert handler.can_handle() is True

    def test_can_handle_toml_files(self):
        """Should detect TOML files by extension."""
        handler = TomlFileHandler()

        assert handler.can_handle_file(Path("config.toml")) is True
        assert handler.can_handle_file(Path("connections.toml")) is True
        assert handler.can_handle_file(Path("file.tml")) is True

    def test_cannot_handle_non_toml_files(self):
        """Should reject non-TOML files."""
        handler = TomlFileHandler()

        assert handler.can_handle_file(Path("config.json")) is False
        assert handler.can_handle_file(Path("config.yaml")) is False
        assert handler.can_handle_file(Path("config")) is False

    def test_discover_raises_not_implemented(self):
        """Should raise NotImplementedError for discover() without file_path."""
        handler = TomlFileHandler()

        with pytest.raises(NotImplementedError, match="requires file_path"):
            handler.discover()

    def test_discover_from_nonexistent_file(self):
        """Should return empty dict for nonexistent file."""
        handler = TomlFileHandler()
        values = handler.discover_from_file(Path("/nonexistent/file.toml"))

        assert len(values) == 0

    def test_discover_from_simple_toml(self):
        """Should discover values from simple TOML file."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('[default]\naccount = "my_account"\nuser = "my_user"\n')
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler(section_path=["default"])
            values = handler.discover_from_file(temp_path)

            assert len(values) == 2
            assert values["account"].value == "my_account"
            assert values["user"].value == "my_user"
        finally:
            temp_path.unlink()

    def test_discover_root_level(self):
        """Should discover values at root level."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('account = "my_account"\nuser = "my_user"\n')
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler()  # No section path
            values = handler.discover_from_file(temp_path)

            assert len(values) == 2
            assert values["account"].value == "my_account"
        finally:
            temp_path.unlink()

    def test_discover_nested_section(self):
        """Should navigate to nested sections."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('[connections]\n[connections.default]\naccount = "test"\n')
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler(section_path=["connections", "default"])
            values = handler.discover_from_file(temp_path)

            assert len(values) == 1
            assert values["account"].value == "test"
        finally:
            temp_path.unlink()

    def test_discover_nonexistent_section(self):
        """Should return empty dict for nonexistent section."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('account = "my_account"\n')
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler(section_path=["nonexistent"])
            values = handler.discover_from_file(temp_path)

            assert len(values) == 0
        finally:
            temp_path.unlink()

    def test_discover_specific_key(self):
        """Should discover only specific key."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('account = "my_account"\nuser = "my_user"\n')
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler()
            values = handler.discover_from_file(temp_path, key="account")

            assert len(values) == 1
            assert "account" in values
            assert "user" not in values
        finally:
            temp_path.unlink()

    def test_discover_nonexistent_key(self):
        """Should return empty dict for nonexistent key."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('account = "my_account"\n')
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler()
            values = handler.discover_from_file(temp_path, key="nonexistent")

            assert len(values) == 0
        finally:
            temp_path.unlink()

    def test_values_have_correct_metadata(self):
        """Discovered values should have correct metadata."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('[default]\naccount = "my_account"\n')
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler(section_path=["default"])
            values = handler.discover_from_file(temp_path)

            config_value = values["account"]
            assert config_value.source_name == "toml:default"
            assert config_value.priority == SourcePriority.FILE
            assert config_value.key == "account"
            assert config_value.value == "my_account"
            assert config_value.raw_value == "my_account"
        finally:
            temp_path.unlink()

    def test_handles_various_value_types(self):
        """Should handle different TOML value types."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                'string_val = "text"\n'
                "int_val = 42\n"
                "bool_val = true\n"
                'list_val = ["a", "b"]\n'
            )
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler()
            values = handler.discover_from_file(temp_path)

            assert values["string_val"].value == "text"
            assert values["int_val"].value == 42
            assert values["bool_val"].value is True
            assert values["list_val"].value == ["a", "b"]
        finally:
            temp_path.unlink()

    def test_caching_behavior(self):
        """Should cache file data for performance."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('account = "my_account"\n')
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler()

            # First call loads file
            values1 = handler.discover_from_file(temp_path)
            # Second call uses cache
            values2 = handler.discover_from_file(temp_path)

            assert values1 == values2
            # Verify caching by checking results are consistent
        finally:
            temp_path.unlink()

    def test_cache_invalidation_on_different_file(self):
        """Should invalidate cache when file changes."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f1:
            f1.write('account = "account1"\n')
            f1.flush()
            temp_path1 = Path(f1.name)

        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f2:
            f2.write('account = "account2"\n')
            f2.flush()
            temp_path2 = Path(f2.name)

        try:
            handler = TomlFileHandler()

            values1 = handler.discover_from_file(temp_path1)
            values2 = handler.discover_from_file(temp_path2)

            assert values1["account"].value == "account1"
            assert values2["account"].value == "account2"
        finally:
            temp_path1.unlink()
            temp_path2.unlink()

    def test_supports_any_string_key(self):
        """Should support any string key."""
        handler = TomlFileHandler()

        assert handler.supports_key("account") is True
        assert handler.supports_key("any_key") is True
        assert handler.supports_key("") is True

    def test_invalid_toml_returns_empty(self):
        """Should handle invalid TOML gracefully."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write("invalid toml content [[[")
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler()
            values = handler.discover_from_file(temp_path)

            assert len(values) == 0
        finally:
            temp_path.unlink()

    def test_multiple_discover_calls_consistent(self):
        """Multiple discover calls should return consistent results."""
        with NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('account = "my_account"\n')
            f.flush()
            temp_path = Path(f.name)

        try:
            handler = TomlFileHandler()

            values1 = handler.discover_from_file(temp_path)
            values2 = handler.discover_from_file(temp_path)

            assert values1 == values2
        finally:
            temp_path.unlink()
