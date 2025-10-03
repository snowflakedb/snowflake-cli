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
Unit tests for Resolution History tracking.

Tests verify:
- ResolutionEntry immutability and fields
- ResolutionHistory creation and properties
- Resolution chain formatting
- History export to dictionary
- Timestamp tracking
"""

from datetime import datetime

import pytest
from snowflake.cli.api.config_ng.core import (
    ConfigValue,
    ResolutionEntry,
    ResolutionHistory,
    SourcePriority,
)


class TestResolutionEntry:
    """Test suite for ResolutionEntry dataclass."""

    def test_create_resolution_entry(self):
        """Should create a ResolutionEntry with all fields."""
        config_value = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        timestamp = datetime.now()
        entry = ResolutionEntry(
            config_value=config_value,
            timestamp=timestamp,
            was_used=True,
        )

        assert entry.config_value == config_value
        assert entry.timestamp == timestamp
        assert entry.was_used is True
        assert entry.overridden_by is None

    def test_create_entry_with_override(self):
        """Should create entry with overridden_by information."""
        config_value = ConfigValue(
            key="account",
            value="file_account",
            source_name="toml:connections",
            priority=SourcePriority.FILE,
        )

        entry = ResolutionEntry(
            config_value=config_value,
            timestamp=datetime.now(),
            was_used=False,
            overridden_by="cli_arguments",
        )

        assert entry.was_used is False
        assert entry.overridden_by == "cli_arguments"

    def test_resolution_entry_is_immutable(self):
        """ResolutionEntry should be immutable (frozen dataclass)."""
        config_value = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        entry = ResolutionEntry(
            config_value=config_value,
            timestamp=datetime.now(),
            was_used=True,
        )

        with pytest.raises(Exception):
            entry.was_used = False

        with pytest.raises(Exception):
            entry.overridden_by = "someone"

    def test_resolution_entry_equality(self):
        """ResolutionEntry instances with same data should be equal."""
        config_value = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        timestamp = datetime.now()

        entry1 = ResolutionEntry(
            config_value=config_value,
            timestamp=timestamp,
            was_used=True,
        )

        entry2 = ResolutionEntry(
            config_value=config_value,
            timestamp=timestamp,
            was_used=True,
        )

        assert entry1 == entry2


class TestResolutionHistory:
    """Test suite for ResolutionHistory dataclass."""

    def test_create_empty_resolution_history(self):
        """Should create an empty ResolutionHistory."""
        history = ResolutionHistory(key="account")

        assert history.key == "account"
        assert len(history.entries) == 0
        assert history.final_value is None
        assert history.default_used is False

    def test_create_resolution_history_with_entries(self):
        """Should create ResolutionHistory with entries."""
        config_value = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        entry = ResolutionEntry(
            config_value=config_value,
            timestamp=datetime.now(),
            was_used=True,
        )

        history = ResolutionHistory(
            key="account",
            entries=[entry],
            final_value="my_account",
        )

        assert len(history.entries) == 1
        assert history.final_value == "my_account"

    def test_sources_consulted_property(self):
        """Should return list of all source names consulted."""
        entries = [
            ResolutionEntry(
                config_value=ConfigValue(
                    key="account",
                    value="file_account",
                    source_name="toml:connections",
                    priority=SourcePriority.FILE,
                ),
                timestamp=datetime.now(),
                was_used=False,
                overridden_by="cli_arguments",
            ),
            ResolutionEntry(
                config_value=ConfigValue(
                    key="account",
                    value="cli_account",
                    source_name="cli_arguments",
                    priority=SourcePriority.CLI_ARGUMENT,
                ),
                timestamp=datetime.now(),
                was_used=True,
            ),
        ]

        history = ResolutionHistory(key="account", entries=entries)

        sources = history.sources_consulted
        assert len(sources) == 2
        assert "toml:connections" in sources
        assert "cli_arguments" in sources

    def test_values_considered_property(self):
        """Should return list of all values considered."""
        entries = [
            ResolutionEntry(
                config_value=ConfigValue(
                    key="account",
                    value="file_account",
                    source_name="toml:connections",
                    priority=SourcePriority.FILE,
                ),
                timestamp=datetime.now(),
                was_used=False,
            ),
            ResolutionEntry(
                config_value=ConfigValue(
                    key="account",
                    value="cli_account",
                    source_name="cli_arguments",
                    priority=SourcePriority.CLI_ARGUMENT,
                ),
                timestamp=datetime.now(),
                was_used=True,
            ),
        ]

        history = ResolutionHistory(key="account", entries=entries)

        values = history.values_considered
        assert len(values) == 2
        assert "file_account" in values
        assert "cli_account" in values

    def test_selected_entry_property(self):
        """Should return the entry that was selected."""
        entry1 = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="file_account",
                source_name="toml:connections",
                priority=SourcePriority.FILE,
            ),
            timestamp=datetime.now(),
            was_used=False,
            overridden_by="cli_arguments",
        )

        entry2 = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="cli_account",
                source_name="cli_arguments",
                priority=SourcePriority.CLI_ARGUMENT,
            ),
            timestamp=datetime.now(),
            was_used=True,
        )

        history = ResolutionHistory(key="account", entries=[entry1, entry2])

        selected = history.selected_entry
        assert selected == entry2
        assert selected.config_value.value == "cli_account"

    def test_selected_entry_returns_none_when_no_selection(self):
        """Should return None when no entry was selected."""
        entry = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="file_account",
                source_name="toml:connections",
                priority=SourcePriority.FILE,
            ),
            timestamp=datetime.now(),
            was_used=False,
        )

        history = ResolutionHistory(key="account", entries=[entry])

        assert history.selected_entry is None

    def test_overridden_entries_property(self):
        """Should return all entries that were overridden."""
        entry1 = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="file_account",
                source_name="toml:connections",
                priority=SourcePriority.FILE,
            ),
            timestamp=datetime.now(),
            was_used=False,
            overridden_by="cli_arguments",
        )

        entry2 = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="env_account",
                source_name="snowflake_cli_env",
                priority=SourcePriority.ENVIRONMENT,
            ),
            timestamp=datetime.now(),
            was_used=False,
            overridden_by="cli_arguments",
        )

        entry3 = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="cli_account",
                source_name="cli_arguments",
                priority=SourcePriority.CLI_ARGUMENT,
            ),
            timestamp=datetime.now(),
            was_used=True,
        )

        history = ResolutionHistory(key="account", entries=[entry1, entry2, entry3])

        overridden = history.overridden_entries
        assert len(overridden) == 2
        assert entry1 in overridden
        assert entry2 in overridden
        assert entry3 not in overridden

    def test_format_chain_simple(self):
        """Should format a simple resolution chain."""
        entry = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="my_account",
                source_name="cli_arguments",
                priority=SourcePriority.CLI_ARGUMENT,
            ),
            timestamp=datetime.now(),
            was_used=True,
        )

        history = ResolutionHistory(
            key="account",
            entries=[entry],
            final_value="my_account",
        )

        chain = history.format_chain()

        assert "account resolution chain (1 sources)" in chain
        assert "cli_arguments" in chain
        assert "my_account" in chain
        assert "(SELECTED)" in chain
        assert "✅" in chain

    def test_format_chain_with_override(self):
        """Should format resolution chain showing override."""
        entry1 = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="file_account",
                source_name="toml:connections",
                priority=SourcePriority.FILE,
            ),
            timestamp=datetime.now(),
            was_used=False,
            overridden_by="cli_arguments",
        )

        entry2 = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="cli_account",
                source_name="cli_arguments",
                priority=SourcePriority.CLI_ARGUMENT,
            ),
            timestamp=datetime.now(),
            was_used=True,
        )

        history = ResolutionHistory(
            key="account",
            entries=[entry1, entry2],
            final_value="cli_account",
        )

        chain = history.format_chain()

        assert "account resolution chain (2 sources)" in chain
        assert "toml:connections" in chain
        assert "cli_arguments" in chain
        assert "overridden by cli_arguments" in chain
        assert "(SELECTED)" in chain
        assert "❌" in chain
        assert "✅" in chain

    def test_format_chain_with_conversion(self):
        """Should show conversion in formatted chain."""
        entry = ResolutionEntry(
            config_value=ConfigValue(
                key="port",
                value=443,
                source_name="snowflake_cli_env",
                priority=SourcePriority.ENVIRONMENT,
                raw_value="443",
            ),
            timestamp=datetime.now(),
            was_used=True,
        )

        history = ResolutionHistory(
            key="port",
            entries=[entry],
            final_value=443,
        )

        chain = history.format_chain()

        assert "port resolution chain" in chain
        assert "→" in chain
        assert "443" in chain

    def test_format_chain_with_default(self):
        """Should show default value in formatted chain."""
        history = ResolutionHistory(
            key="account",
            entries=[],
            final_value="default_account",
            default_used=True,
        )

        chain = history.format_chain()

        assert "account resolution chain (0 sources)" in chain
        assert "Default value used: default_account" in chain

    def test_to_dict_conversion(self):
        """Should convert history to dictionary for JSON export."""
        entry = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="my_account",
                source_name="cli_arguments",
                priority=SourcePriority.CLI_ARGUMENT,
            ),
            timestamp=datetime.now(),
            was_used=True,
        )

        history = ResolutionHistory(
            key="account",
            entries=[entry],
            final_value="my_account",
        )

        data = history.to_dict()

        assert data["key"] == "account"
        assert data["final_value"] == "my_account"
        assert data["default_used"] is False
        assert "cli_arguments" in data["sources_consulted"]
        assert len(data["entries"]) == 1

        entry_data = data["entries"][0]
        assert entry_data["source"] == "cli_arguments"
        assert entry_data["value"] == "my_account"
        assert entry_data["priority"] == "CLI_ARGUMENT"
        assert entry_data["was_used"] is True

    def test_to_dict_with_multiple_entries(self):
        """Should convert complex history to dictionary."""
        entries = [
            ResolutionEntry(
                config_value=ConfigValue(
                    key="account",
                    value="file_account",
                    source_name="toml:connections",
                    priority=SourcePriority.FILE,
                    raw_value="file_account",
                ),
                timestamp=datetime.now(),
                was_used=False,
                overridden_by="cli_arguments",
            ),
            ResolutionEntry(
                config_value=ConfigValue(
                    key="account",
                    value="cli_account",
                    source_name="cli_arguments",
                    priority=SourcePriority.CLI_ARGUMENT,
                ),
                timestamp=datetime.now(),
                was_used=True,
            ),
        ]

        history = ResolutionHistory(
            key="account",
            entries=entries,
            final_value="cli_account",
        )

        data = history.to_dict()

        assert len(data["entries"]) == 2
        assert data["entries"][0]["overridden_by"] == "cli_arguments"
        assert data["entries"][1]["was_used"] is True

    def test_resolution_history_is_mutable(self):
        """ResolutionHistory should be mutable (not frozen)."""
        history = ResolutionHistory(key="account")

        entry = ResolutionEntry(
            config_value=ConfigValue(
                key="account",
                value="my_account",
                source_name="cli_arguments",
                priority=SourcePriority.CLI_ARGUMENT,
            ),
            timestamp=datetime.now(),
            was_used=True,
        )

        history.entries.append(entry)
        history.final_value = "my_account"

        assert len(history.entries) == 1
        assert history.final_value == "my_account"

    def test_empty_history_properties(self):
        """Empty history should return empty lists for properties."""
        history = ResolutionHistory(key="account")

        assert history.sources_consulted == []
        assert history.values_considered == []
        assert history.selected_entry is None
        assert history.overridden_entries == []
