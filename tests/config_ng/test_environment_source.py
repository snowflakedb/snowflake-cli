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
Unit tests for EnvironmentSource.

Tests verify:
- Medium priority source (ENVIRONMENT)
- Handler-based discovery (no direct values)
- Handler ordering for migration support
- Multiple handler support
"""

from typing import Any, Dict, Optional

from snowflake.cli.api.config_ng.core import ConfigValue, SourcePriority
from snowflake.cli.api.config_ng.handlers import SourceHandler
from snowflake.cli.api.config_ng.sources import EnvironmentSource


class MockEnvHandler(SourceHandler):
    """Mock environment variable handler for testing."""

    def __init__(self, data: Dict[str, Any], name: str = "mock_env_handler"):
        self._data = data
        self._name = name

    @property
    def source_name(self) -> str:
        return self._name

    @property
    def priority(self) -> SourcePriority:
        return SourcePriority.ENVIRONMENT

    @property
    def handler_type(self) -> str:
        return "mock_env"

    def can_handle(self) -> bool:
        return len(self._data) > 0

    def discover(self, key: Optional[str] = None) -> Dict[str, ConfigValue]:
        if key is None:
            return {
                k: ConfigValue(
                    key=k,
                    value=v,
                    source_name=self.source_name,
                    priority=self.priority,
                )
                for k, v in self._data.items()
            }
        elif key in self._data:
            return {
                key: ConfigValue(
                    key=key,
                    value=self._data[key],
                    source_name=self.source_name,
                    priority=self.priority,
                )
            }
        return {}

    def supports_key(self, key: str) -> bool:
        return key in self._data


class TestEnvironmentSource:
    """Test suite for EnvironmentSource."""

    def test_create_with_no_handlers(self):
        """Should create source with empty handler list."""
        source = EnvironmentSource()

        assert source.source_name == "environment"
        assert source.priority == SourcePriority.ENVIRONMENT
        assert len(source.get_handlers()) == 0

    def test_create_with_handlers(self):
        """Should create source with provided handlers."""
        handler1 = MockEnvHandler({"key1": "value1"}, "handler1")
        handler2 = MockEnvHandler({"key2": "value2"}, "handler2")

        source = EnvironmentSource(handlers=[handler1, handler2])

        handlers = source.get_handlers()
        assert len(handlers) == 2

    def test_has_environment_priority(self):
        """Should have ENVIRONMENT priority (medium)."""
        source = EnvironmentSource()

        assert source.priority == SourcePriority.ENVIRONMENT
        assert source.priority.value == 2

    def test_discover_direct_returns_empty(self):
        """Environment source should have no direct values."""
        handler = MockEnvHandler({"key1": "value1"}, "handler")
        source = EnvironmentSource(handlers=[handler])

        direct_values = source.discover_direct()

        assert len(direct_values) == 0

    def test_discover_from_single_handler(self):
        """Should discover values from single handler."""
        handler = MockEnvHandler(
            {"account": "my_account", "user": "my_user"}, "handler"
        )
        source = EnvironmentSource(handlers=[handler])

        values = source.discover()

        assert len(values) == 2
        assert values["account"].value == "my_account"
        assert values["user"].value == "my_user"

    def test_discover_from_multiple_handlers(self):
        """Should discover values from multiple handlers."""
        handler1 = MockEnvHandler({"key1": "value1"}, "handler1")
        handler2 = MockEnvHandler({"key2": "value2"}, "handler2")

        source = EnvironmentSource(handlers=[handler1, handler2])
        values = source.discover()

        assert len(values) == 2
        assert values["key1"].value == "value1"
        assert values["key2"].value == "value2"

    def test_handler_ordering_first_wins(self):
        """First handler with value should win for same key."""
        handler1 = MockEnvHandler({"account": "handler1_account"}, "snowflake_cli_env")
        handler2 = MockEnvHandler({"account": "handler2_account"}, "snowsql_env")

        source = EnvironmentSource(handlers=[handler1, handler2])
        values = source.discover(key="account")

        assert values["account"].value == "handler1_account"
        assert values["account"].source_name == "snowflake_cli_env"

    def test_migration_scenario_snowflake_overrides_snowsql(self):
        """
        Migration scenario: SNOWFLAKE_* vars should override SNOWSQL_* vars.
        Simulates handler ordering for migration support.
        """
        # Handler order: SnowCLI first (higher priority), SnowSQL second (fallback)
        snowflake_handler = MockEnvHandler(
            {"account": "new_account", "user": "new_user"}, "snowflake_cli_env"
        )
        snowsql_handler = MockEnvHandler(
            {"account": "old_account", "user": "old_user", "password": "old_password"},
            "snowsql_env",
        )

        source = EnvironmentSource(handlers=[snowflake_handler, snowsql_handler])
        values = source.discover()

        # New values should win
        assert values["account"].value == "new_account"
        assert values["account"].source_name == "snowflake_cli_env"
        assert values["user"].value == "new_user"
        assert values["user"].source_name == "snowflake_cli_env"

        # Fallback to legacy for unmigrated keys
        assert values["password"].value == "old_password"
        assert values["password"].source_name == "snowsql_env"

    def test_discover_specific_key(self):
        """Should discover specific key when provided."""
        handler = MockEnvHandler(
            {"account": "my_account", "user": "my_user"}, "handler"
        )
        source = EnvironmentSource(handlers=[handler])

        values = source.discover(key="account")

        assert len(values) == 1
        assert "account" in values
        assert values["account"].value == "my_account"

    def test_discover_nonexistent_key(self):
        """Should return empty dict for nonexistent key."""
        handler = MockEnvHandler({"account": "my_account"}, "handler")
        source = EnvironmentSource(handlers=[handler])

        values = source.discover(key="nonexistent")

        assert len(values) == 0

    def test_supports_key_from_any_handler(self):
        """Should return True if any handler supports the key."""
        handler1 = MockEnvHandler({"key1": "value1"}, "handler1")
        handler2 = MockEnvHandler({"key2": "value2"}, "handler2")

        source = EnvironmentSource(handlers=[handler1, handler2])

        assert source.supports_key("key1") is True
        assert source.supports_key("key2") is True
        assert source.supports_key("nonexistent") is False

    def test_no_handlers_returns_empty(self):
        """With no handlers, should return empty dict."""
        source = EnvironmentSource(handlers=[])

        values = source.discover()

        assert len(values) == 0

    def test_values_have_correct_priority(self):
        """All values should have ENVIRONMENT priority."""
        handler = MockEnvHandler({"account": "my_account"}, "handler")
        source = EnvironmentSource(handlers=[handler])

        values = source.discover()

        assert values["account"].priority == SourcePriority.ENVIRONMENT

    def test_add_handler_dynamically(self):
        """Should be able to add handlers after creation."""
        source = EnvironmentSource(handlers=[])
        handler = MockEnvHandler({"account": "my_account"}, "handler")

        source.add_handler(handler)
        values = source.discover()

        assert len(values) == 1
        assert values["account"].value == "my_account"

    def test_set_handlers_replaces_all(self):
        """Should replace all handlers with new list."""
        handler1 = MockEnvHandler({"key1": "value1"}, "handler1")
        handler2 = MockEnvHandler({"key2": "value2"}, "handler2")
        handler3 = MockEnvHandler({"key3": "value3"}, "handler3")

        source = EnvironmentSource(handlers=[handler1, handler2])
        source.set_handlers([handler3])

        values = source.discover()

        assert len(values) == 1
        assert "key3" in values
        assert "key1" not in values

    def test_handler_failure_does_not_break_discovery(self):
        """Failed handler should not prevent other handlers from working."""

        class FailingHandler(SourceHandler):
            @property
            def source_name(self) -> str:
                return "failing"

            @property
            def priority(self) -> SourcePriority:
                return SourcePriority.ENVIRONMENT

            @property
            def handler_type(self) -> str:
                return "failing"

            def can_handle(self) -> bool:
                return True

            def discover(self, key=None):
                raise RuntimeError("Handler failed")

            def supports_key(self, key: str) -> bool:
                return True

        failing = FailingHandler()
        working = MockEnvHandler({"account": "my_account"}, "working")

        source = EnvironmentSource(handlers=[failing, working])
        values = source.discover()

        # Should still get value from working handler
        assert len(values) == 1
        assert values["account"].value == "my_account"

    def test_empty_handler_returns_no_values(self):
        """Handler with no data should contribute no values."""
        empty_handler = MockEnvHandler({}, "empty")
        full_handler = MockEnvHandler({"account": "my_account"}, "full")

        source = EnvironmentSource(handlers=[empty_handler, full_handler])
        values = source.discover()

        assert len(values) == 1
        assert values["account"].value == "my_account"
