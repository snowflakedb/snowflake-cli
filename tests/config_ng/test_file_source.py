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
Unit tests for FileSource.

Tests verify:
- Lowest priority source (FILE)
- File-based discovery with handlers
- File path ordering for precedence
- Handler ordering within files
- File existence handling
"""

from pathlib import Path
from typing import Any, Dict, Optional

from snowflake.cli.api.config_ng.core import ConfigValue, SourcePriority
from snowflake.cli.api.config_ng.handlers import SourceHandler
from snowflake.cli.api.config_ng.sources import FileSource


class MockFileHandler(SourceHandler):
    """Mock file handler for testing."""

    def __init__(
        self,
        data: Dict[Path, Dict[str, Any]],
        name: str = "mock_file_handler",
        file_extensions: Optional[list] = None,
    ):
        self._data = data  # Path -> {key: value}
        self._name = name
        self._file_extensions = file_extensions or [".toml", ".conf"]

    @property
    def source_name(self) -> str:
        return self._name

    @property
    def priority(self) -> SourcePriority:
        return SourcePriority.FILE

    @property
    def handler_type(self) -> str:
        return "mock_file"

    def can_handle(self) -> bool:
        return len(self._data) > 0

    def can_handle_file(self, file_path: Path) -> bool:
        return file_path.suffix in self._file_extensions

    def discover_from_file(
        self, file_path: Path, key: Optional[str] = None
    ) -> Dict[str, ConfigValue]:
        if file_path not in self._data:
            return {}

        file_data = self._data[file_path]

        if key is None:
            return {
                k: ConfigValue(
                    key=k,
                    value=v,
                    source_name=self.source_name,
                    priority=self.priority,
                )
                for k, v in file_data.items()
            }
        elif key in file_data:
            return {
                key: ConfigValue(
                    key=key,
                    value=file_data[key],
                    source_name=self.source_name,
                    priority=self.priority,
                )
            }
        return {}

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        # Not used in FileSource - discover_from_file is called instead
        return {}

    def supports_key(self, key: str) -> bool:
        # Check if key exists in any file
        return any(key in file_data for file_data in self._data.values())


class TestFileSource:
    """Test suite for FileSource."""

    def test_create_with_no_paths_or_handlers(self):
        """Should create source with empty file paths and handlers."""
        source = FileSource()

        assert source.source_name == "configuration_files"
        assert source.priority == SourcePriority.FILE
        assert len(source.get_file_paths()) == 0
        assert len(source.get_handlers()) == 0

    def test_create_with_file_paths(self, tmp_path):
        """Should create source with provided file paths."""
        file1 = tmp_path / "config1.toml"
        file2 = tmp_path / "config2.toml"
        file1.touch()
        file2.touch()

        source = FileSource(file_paths=[file1, file2])

        paths = source.get_file_paths()
        assert len(paths) == 2
        assert file1 in paths
        assert file2 in paths

    def test_has_file_priority(self):
        """Should have FILE priority (lowest)."""
        source = FileSource()

        assert source.priority == SourcePriority.FILE
        assert source.priority.value == 3

    def test_discover_direct_returns_empty(self):
        """File source should have no direct values."""
        source = FileSource()

        direct_values = source.discover_direct()

        assert len(direct_values) == 0

    def test_discover_from_single_file(self, tmp_path):
        """Should discover values from single file."""
        file_path = tmp_path / "config.toml"
        file_path.touch()

        handler = MockFileHandler(
            {file_path: {"account": "my_account", "user": "my_user"}}, "toml_handler"
        )

        source = FileSource(file_paths=[file_path], handlers=[handler])
        values = source.discover()

        assert len(values) == 2
        assert values["account"].value == "my_account"
        assert values["user"].value == "my_user"

    def test_discover_from_multiple_files(self, tmp_path):
        """Should discover values from multiple files."""
        file1 = tmp_path / "config1.toml"
        file2 = tmp_path / "config2.toml"
        file1.touch()
        file2.touch()

        handler = MockFileHandler(
            {
                file1: {"key1": "value1"},
                file2: {"key2": "value2"},
            },
            "toml_handler",
        )

        source = FileSource(file_paths=[file1, file2], handlers=[handler])
        values = source.discover()

        assert len(values) == 2
        assert values["key1"].value == "value1"
        assert values["key2"].value == "value2"

    def test_file_path_ordering_first_wins(self, tmp_path):
        """First file path with value should win for same key."""
        file1 = tmp_path / "config1.toml"
        file2 = tmp_path / "config2.toml"
        file1.touch()
        file2.touch()

        handler = MockFileHandler(
            {
                file1: {"account": "account_from_file1"},
                file2: {"account": "account_from_file2"},
            },
            "toml_handler",
        )

        source = FileSource(file_paths=[file1, file2], handlers=[handler])
        values = source.discover(key="account")

        assert values["account"].value == "account_from_file1"

    def test_handler_ordering_first_wins(self, tmp_path):
        """First handler that can read file should win for same key."""
        file_path = tmp_path / "config.toml"
        file_path.touch()

        handler1 = MockFileHandler(
            {file_path: {"account": "handler1_account"}}, "snowcli_toml"
        )
        handler2 = MockFileHandler(
            {file_path: {"account": "handler2_account"}}, "legacy_toml"
        )

        source = FileSource(file_paths=[file_path], handlers=[handler1, handler2])
        values = source.discover(key="account")

        assert values["account"].value == "handler1_account"
        assert values["account"].source_name == "snowcli_toml"

    def test_skips_nonexistent_files(self, tmp_path):
        """Should skip files that don't exist."""
        existing_file = tmp_path / "exists.toml"
        nonexistent_file = tmp_path / "does_not_exist.toml"
        existing_file.touch()

        handler = MockFileHandler(
            {
                existing_file: {"key1": "value1"},
                nonexistent_file: {"key2": "value2"},
            },
            "handler",
        )

        source = FileSource(
            file_paths=[nonexistent_file, existing_file], handlers=[handler]
        )
        values = source.discover()

        # Should only get value from existing file
        assert len(values) == 1
        assert "key1" in values
        assert "key2" not in values

    def test_skips_files_handler_cannot_handle(self, tmp_path):
        """Should skip files that handler cannot handle."""
        toml_file = tmp_path / "config.toml"
        json_file = tmp_path / "config.json"
        toml_file.touch()
        json_file.touch()

        # Handler only handles .toml files
        handler = MockFileHandler(
            {
                toml_file: {"key1": "value1"},
                json_file: {"key2": "value2"},
            },
            "toml_handler",
            file_extensions=[".toml"],
        )

        source = FileSource(file_paths=[toml_file, json_file], handlers=[handler])
        values = source.discover()

        # Should only get value from .toml file
        assert len(values) == 1
        assert "key1" in values
        assert "key2" not in values

    def test_migration_scenario_snowcli_overrides_snowsql(self, tmp_path):
        """
        Migration scenario: SnowCLI files should override SnowSQL files.
        Simulates file ordering for migration support.
        """
        snowcli_file = tmp_path / "connections.toml"
        snowsql_file = tmp_path / "snowsql_config"
        snowcli_file.touch()
        snowsql_file.touch()

        # SnowCLI handler only handles .toml files
        snowcli_handler = MockFileHandler(
            {snowcli_file: {"account": "new_account", "user": "new_user"}},
            "snowcli_toml",
            file_extensions=[".toml"],
        )
        # SnowSQL handler handles files without extension
        snowsql_handler = MockFileHandler(
            {
                snowsql_file: {
                    "account": "old_account",
                    "user": "old_user",
                    "password": "old_password",
                }
            },
            "snowsql_config",
            file_extensions=[""],  # No extension
        )

        # SnowCLI file comes first (higher precedence)
        source = FileSource(
            file_paths=[snowcli_file, snowsql_file],
            handlers=[snowcli_handler, snowsql_handler],
        )
        values = source.discover()

        # New values from SnowCLI should win
        assert values["account"].value == "new_account"
        assert values["account"].source_name == "snowcli_toml"
        assert values["user"].value == "new_user"

        # Fallback to SnowSQL for unmigrated keys
        assert values["password"].value == "old_password"
        assert values["password"].source_name == "snowsql_config"

    def test_discover_specific_key(self, tmp_path):
        """Should discover specific key when provided."""
        file_path = tmp_path / "config.toml"
        file_path.touch()

        handler = MockFileHandler(
            {file_path: {"account": "my_account", "user": "my_user"}}, "handler"
        )

        source = FileSource(file_paths=[file_path], handlers=[handler])
        values = source.discover(key="account")

        assert len(values) == 1
        assert "account" in values
        assert values["account"].value == "my_account"

    def test_discover_nonexistent_key(self, tmp_path):
        """Should return empty dict for nonexistent key."""
        file_path = tmp_path / "config.toml"
        file_path.touch()

        handler = MockFileHandler({file_path: {"account": "my_account"}}, "handler")

        source = FileSource(file_paths=[file_path], handlers=[handler])
        values = source.discover(key="nonexistent")

        assert len(values) == 0

    def test_supports_key_from_any_handler(self, tmp_path):
        """Should return True if any handler supports the key."""
        handler1 = MockFileHandler({tmp_path / "f1": {"key1": "value1"}}, "handler1")
        handler2 = MockFileHandler({tmp_path / "f2": {"key2": "value2"}}, "handler2")

        source = FileSource(handlers=[handler1, handler2])

        assert source.supports_key("key1") is True
        assert source.supports_key("key2") is True
        assert source.supports_key("nonexistent") is False

    def test_handler_failure_does_not_break_discovery(self, tmp_path):
        """Failed handler should not prevent other handlers from working."""
        file_path = tmp_path / "config.toml"
        file_path.touch()

        class FailingHandler(SourceHandler):
            @property
            def source_name(self) -> str:
                return "failing"

            @property
            def priority(self) -> SourcePriority:
                return SourcePriority.FILE

            @property
            def handler_type(self) -> str:
                return "failing"

            def can_handle(self) -> bool:
                return True

            def can_handle_file(self, file_path: Path) -> bool:
                return True

            def discover_from_file(self, file_path: Path, key=None):
                raise RuntimeError("Handler failed")

            def discover(self, key=None):
                return {}

            def supports_key(self, key: str) -> bool:
                return True

        failing = FailingHandler()
        working = MockFileHandler({file_path: {"account": "my_account"}}, "working")

        source = FileSource(file_paths=[file_path], handlers=[failing, working])
        values = source.discover()

        # Should still get value from working handler
        assert len(values) == 1
        assert values["account"].value == "my_account"

    def test_add_file_path_append(self, tmp_path):
        """Should append file path to end of list."""
        file1 = tmp_path / "config1.toml"
        file2 = tmp_path / "config2.toml"

        source = FileSource(file_paths=[file1])
        source.add_file_path(file2)

        paths = source.get_file_paths()
        assert len(paths) == 2
        assert paths[1] == file2

    def test_add_file_path_prepend(self, tmp_path):
        """Should prepend file path to beginning of list."""
        file1 = tmp_path / "config1.toml"
        file2 = tmp_path / "config2.toml"

        source = FileSource(file_paths=[file1])
        source.add_file_path(file2, position=0)

        paths = source.get_file_paths()
        assert len(paths) == 2
        assert paths[0] == file2

    def test_set_file_paths(self, tmp_path):
        """Should replace all file paths with new list."""
        file1 = tmp_path / "config1.toml"
        file2 = tmp_path / "config2.toml"
        file3 = tmp_path / "config3.toml"

        source = FileSource(file_paths=[file1, file2])
        source.set_file_paths([file3])

        paths = source.get_file_paths()
        assert len(paths) == 1
        assert paths[0] == file3

    def test_get_file_paths_returns_copy(self, tmp_path):
        """get_file_paths should return a copy, not the original list."""
        file1 = tmp_path / "config.toml"
        source = FileSource(file_paths=[file1])

        paths = source.get_file_paths()
        paths.clear()

        # Original list should be unchanged
        assert len(source.get_file_paths()) == 1

    def test_no_files_returns_empty(self):
        """With no file paths, should return empty dict."""
        handler = MockFileHandler({}, "handler")
        source = FileSource(file_paths=[], handlers=[handler])

        values = source.discover()

        assert len(values) == 0

    def test_values_have_correct_priority(self, tmp_path):
        """All values should have FILE priority."""
        file_path = tmp_path / "config.toml"
        file_path.touch()

        handler = MockFileHandler({file_path: {"account": "my_account"}}, "handler")
        source = FileSource(file_paths=[file_path], handlers=[handler])

        values = source.discover()

        assert values["account"].priority == SourcePriority.FILE
