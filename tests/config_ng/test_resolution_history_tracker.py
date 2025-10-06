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
Unit tests for ResolutionHistoryTracker.

Tests verify:
- Discovery recording
- Selection marking
- Default value tracking
- History retrieval
- Summary statistics
"""

from datetime import datetime

from snowflake.cli.api.config_ng.core import ConfigValue, SourcePriority
from snowflake.cli.api.config_ng.resolver import ResolutionHistoryTracker


class TestResolutionHistoryTracker:
    """Test suite for ResolutionHistoryTracker."""

    def test_create_tracker(self):
        """Should create empty tracker with tracking enabled."""
        tracker = ResolutionHistoryTracker()

        assert tracker.is_enabled() is True
        assert len(tracker.get_all_histories()) == 0

    def test_enable_disable_tracking(self):
        """Should enable and disable tracking."""
        tracker = ResolutionHistoryTracker()

        tracker.disable()
        assert tracker.is_enabled() is False

        tracker.enable()
        assert tracker.is_enabled() is True

    def test_record_discovery(self):
        """Should record value discoveries."""
        tracker = ResolutionHistoryTracker()

        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        tracker.record_discovery("account", cv)

        # Discovery recorded but history not finalized yet
        assert len(tracker.get_all_histories()) == 0

    def test_mark_selected_creates_history(self):
        """Should create history when value is marked as selected."""
        tracker = ResolutionHistoryTracker()

        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        tracker.record_discovery("account", cv)
        tracker.mark_selected("account", "cli_arguments")

        history = tracker.get_history("account")
        assert history is not None
        assert history.key == "account"
        assert history.final_value == "my_account"
        assert len(history.entries) == 1
        assert history.entries[0].was_used is True

    def test_multiple_discoveries_single_selection(self):
        """Should track multiple discoveries with one selected."""
        tracker = ResolutionHistoryTracker()

        # Record discoveries from multiple sources
        cv_file = ConfigValue(
            key="account",
            value="file_account",
            source_name="toml:connections",
            priority=SourcePriority.FILE,
        )
        cv_env = ConfigValue(
            key="account",
            value="env_account",
            source_name="snowflake_cli_env",
            priority=SourcePriority.ENVIRONMENT,
        )
        cv_cli = ConfigValue(
            key="account",
            value="cli_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        tracker.record_discovery("account", cv_file)
        tracker.record_discovery("account", cv_env)
        tracker.record_discovery("account", cv_cli)

        # Mark CLI as selected
        tracker.mark_selected("account", "cli_arguments")

        history = tracker.get_history("account")
        assert history is not None
        assert len(history.entries) == 3
        assert history.final_value == "cli_account"

        # Check which was selected
        selected = [e for e in history.entries if e.was_used]
        assert len(selected) == 1
        assert selected[0].config_value.source_name == "cli_arguments"

        # Check overridden entries
        overridden = [e for e in history.entries if not e.was_used]
        assert len(overridden) == 2

    def test_mark_default_used(self):
        """Should mark when default value is used."""
        tracker = ResolutionHistoryTracker()

        tracker.mark_default_used("missing_key", "default_value")

        history = tracker.get_history("missing_key")
        assert history is not None
        assert history.default_used is True
        assert history.final_value == "default_value"
        assert len(history.entries) == 0

    def test_mark_default_after_discoveries(self):
        """Should update history when default is used after discoveries."""
        tracker = ResolutionHistoryTracker()

        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        tracker.record_discovery("account", cv)
        tracker.mark_selected("account", "cli_arguments")
        tracker.mark_default_used("account", "default_account")

        history = tracker.get_history("account")
        assert history.default_used is True
        assert history.final_value == "default_account"

    def test_get_history_nonexistent_key(self):
        """Should return None for keys not tracked."""
        tracker = ResolutionHistoryTracker()

        history = tracker.get_history("nonexistent")
        assert history is None

    def test_get_all_histories(self):
        """Should return all tracked histories."""
        tracker = ResolutionHistoryTracker()

        cv1 = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )
        cv2 = ConfigValue(
            key="user",
            value="my_user",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        tracker.record_discovery("account", cv1)
        tracker.mark_selected("account", "cli_arguments")

        tracker.record_discovery("user", cv2)
        tracker.mark_selected("user", "cli_arguments")

        histories = tracker.get_all_histories()
        assert len(histories) == 2
        assert "account" in histories
        assert "user" in histories

    def test_clear_history(self):
        """Should clear all recorded history."""
        tracker = ResolutionHistoryTracker()

        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        tracker.record_discovery("account", cv)
        tracker.mark_selected("account", "cli_arguments")

        assert len(tracker.get_all_histories()) == 1

        tracker.clear()

        assert len(tracker.get_all_histories()) == 0

    def test_disabled_tracker_does_not_record(self):
        """Should not record when tracking is disabled."""
        tracker = ResolutionHistoryTracker()
        tracker.disable()

        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        tracker.record_discovery("account", cv)
        tracker.mark_selected("account", "cli_arguments")

        assert len(tracker.get_all_histories()) == 0

    def test_summary_with_no_histories(self):
        """Should return empty summary when no histories exist."""
        tracker = ResolutionHistoryTracker()

        summary = tracker.get_summary()

        assert summary["total_keys_resolved"] == 0
        assert summary["keys_with_overrides"] == 0
        assert summary["keys_using_defaults"] == 0
        assert len(summary["source_usage"]) == 0
        assert len(summary["source_wins"]) == 0

    def test_summary_with_single_source(self):
        """Should calculate correct summary for single source."""
        tracker = ResolutionHistoryTracker()

        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        tracker.record_discovery("account", cv)
        tracker.mark_selected("account", "cli_arguments")

        summary = tracker.get_summary()

        assert summary["total_keys_resolved"] == 1
        assert summary["keys_with_overrides"] == 0
        assert summary["source_usage"]["cli_arguments"] == 1
        assert summary["source_wins"]["cli_arguments"] == 1

    def test_summary_with_multiple_sources(self):
        """Should calculate correct summary with overrides."""
        tracker = ResolutionHistoryTracker()

        # File source provides account
        cv_file = ConfigValue(
            key="account",
            value="file_account",
            source_name="toml:connections",
            priority=SourcePriority.FILE,
        )
        # Env source overrides account
        cv_env = ConfigValue(
            key="account",
            value="env_account",
            source_name="snowflake_cli_env",
            priority=SourcePriority.ENVIRONMENT,
        )

        tracker.record_discovery("account", cv_file)
        tracker.record_discovery("account", cv_env)
        tracker.mark_selected("account", "snowflake_cli_env")

        summary = tracker.get_summary()

        assert summary["total_keys_resolved"] == 1
        assert summary["keys_with_overrides"] == 1
        assert summary["source_usage"]["toml:connections"] == 1
        assert summary["source_usage"]["snowflake_cli_env"] == 1
        assert summary["source_wins"]["snowflake_cli_env"] == 1
        assert summary["source_wins"].get("toml:connections", 0) == 0

    def test_summary_with_defaults(self):
        """Should count keys using defaults."""
        tracker = ResolutionHistoryTracker()

        tracker.mark_default_used("missing_key", "default_value")

        summary = tracker.get_summary()

        assert summary["total_keys_resolved"] == 1
        assert summary["keys_using_defaults"] == 1

    def test_entries_have_timestamps(self):
        """Resolution entries should have timestamps."""
        tracker = ResolutionHistoryTracker()

        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        before = datetime.now()
        tracker.record_discovery("account", cv)
        tracker.mark_selected("account", "cli_arguments")
        after = datetime.now()

        history = tracker.get_history("account")
        entry_timestamp = history.entries[0].timestamp

        assert before <= entry_timestamp <= after

    def test_overridden_by_is_set_correctly(self):
        """Should set overridden_by field correctly."""
        tracker = ResolutionHistoryTracker()

        cv_file = ConfigValue(
            key="account",
            value="file_account",
            source_name="toml:connections",
            priority=SourcePriority.FILE,
        )
        cv_cli = ConfigValue(
            key="account",
            value="cli_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        tracker.record_discovery("account", cv_file)
        tracker.record_discovery("account", cv_cli)
        tracker.mark_selected("account", "cli_arguments")

        history = tracker.get_history("account")

        # File entry should be overridden by CLI
        file_entry = [
            e
            for e in history.entries
            if e.config_value.source_name == "toml:connections"
        ][0]
        assert file_entry.was_used is False
        assert file_entry.overridden_by == "cli_arguments"

        # CLI entry should be selected
        cli_entry = [
            e for e in history.entries if e.config_value.source_name == "cli_arguments"
        ][0]
        assert cli_entry.was_used is True
        assert cli_entry.overridden_by is None

    def test_get_all_histories_returns_copy(self):
        """get_all_histories should return a copy."""
        tracker = ResolutionHistoryTracker()

        cv = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        tracker.record_discovery("account", cv)
        tracker.mark_selected("account", "cli_arguments")

        histories1 = tracker.get_all_histories()
        histories1.clear()

        histories2 = tracker.get_all_histories()
        assert len(histories2) == 1

    def test_multiple_keys_tracked_independently(self):
        """Should track multiple keys independently."""
        tracker = ResolutionHistoryTracker()

        cv_account = ConfigValue(
            key="account",
            value="my_account",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )
        cv_user_file = ConfigValue(
            key="user",
            value="file_user",
            source_name="toml:connections",
            priority=SourcePriority.FILE,
        )
        cv_user_cli = ConfigValue(
            key="user",
            value="cli_user",
            source_name="cli_arguments",
            priority=SourcePriority.CLI_ARGUMENT,
        )

        # Account from CLI only
        tracker.record_discovery("account", cv_account)
        tracker.mark_selected("account", "cli_arguments")

        # User from File and CLI
        tracker.record_discovery("user", cv_user_file)
        tracker.record_discovery("user", cv_user_cli)
        tracker.mark_selected("user", "cli_arguments")

        # Check account history
        account_history = tracker.get_history("account")
        assert len(account_history.entries) == 1

        # Check user history
        user_history = tracker.get_history("user")
        assert len(user_history.entries) == 2
