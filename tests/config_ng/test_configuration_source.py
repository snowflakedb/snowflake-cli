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
Unit tests for ConfigurationSource abstract base class.

Tests verify:
- Abstract class cannot be instantiated without implementing abstract methods
- Handler ordering and precedence
- Handler management (add, set, get)
- Direct value precedence over handler values
- Handler failure handling
"""

from typing import Any, Dict, Optional

import pytest
from snowflake.cli.api.config_ng.core import ConfigValue, SourcePriority
from snowflake.cli.api.config_ng.handlers import SourceHandler
from snowflake.cli.api.config_ng.sources import ConfigurationSource


class MockHandler(SourceHandler):
    """Mock handler for testing."""

    def __init__(self, data: Dict[str, Any], name: str = "mock_handler"):
        self._data = data
        self._name = name

    @property
    def source_name(self) -> str:
        return self._name

    @property
    def priority(self) -> SourcePriority:
        return SourcePriority.FILE

    @property
    def handler_type(self) -> str:
        return "mock"

    def can_handle(self) -> bool:
        return True

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


class TestConfigurationSourceInterface:
    """Test suite for ConfigurationSource abstract base class."""

    def test_cannot_instantiate_abstract_class(self):
        """Should not be able to instantiate ConfigurationSource directly."""
        with pytest.raises(TypeError):
            ConfigurationSource()

    def test_must_implement_discover_direct(self):
        """Concrete implementations must implement discover_direct method."""

        class IncompleteSource(ConfigurationSource):
            @property
            def source_name(self) -> str:
                return "test"

            @property
            def priority(self) -> SourcePriority:
                return SourcePriority.FILE

            def supports_key(self, key: str) -> bool:
                return True

        with pytest.raises(TypeError):
            IncompleteSource()

    def test_complete_implementation(self):
        """Should be able to instantiate with all methods implemented."""

        class CompleteSource(ConfigurationSource):
            @property
            def source_name(self) -> str:
                return "test_source"

            @property
            def priority(self) -> SourcePriority:
                return SourcePriority.FILE

            def discover_direct(self, key=None) -> Dict[str, ConfigValue]:
                return {}

            def supports_key(self, key: str) -> bool:
                return True

        source = CompleteSource()
        assert source.source_name == "test_source"
        assert source.priority == SourcePriority.FILE


class TestConfigurationSourceHandlers:
    """Test handler management in ConfigurationSource."""

    class TestSource(ConfigurationSource):
        """Test implementation of ConfigurationSource."""

        def __init__(self, direct_values=None, handlers=None):
            super().__init__(handlers=handlers)
            self._direct_values = direct_values or {}

        @property
        def source_name(self) -> str:
            return "test_source"

        @property
        def priority(self) -> SourcePriority:
            return SourcePriority.FILE

        def discover_direct(self, key=None) -> Dict[str, ConfigValue]:
            if key is None:
                return {
                    k: ConfigValue(
                        key=k,
                        value=v,
                        source_name=self.source_name,
                        priority=self.priority,
                    )
                    for k, v in self._direct_values.items()
                }
            elif key in self._direct_values:
                return {
                    key: ConfigValue(
                        key=key,
                        value=self._direct_values[key],
                        source_name=self.source_name,
                        priority=self.priority,
                    )
                }
            return {}

        def supports_key(self, key: str) -> bool:
            return key in self._direct_values or any(
                h.supports_key(key) for h in self._handlers
            )

    def test_initialize_with_no_handlers(self):
        """Should initialize with empty handler list."""
        source = self.TestSource()
        assert len(source.get_handlers()) == 0

    def test_initialize_with_handlers(self):
        """Should initialize with provided handlers."""
        handler1 = MockHandler({"key1": "value1"}, "handler1")
        handler2 = MockHandler({"key2": "value2"}, "handler2")

        source = self.TestSource(handlers=[handler1, handler2])
        handlers = source.get_handlers()

        assert len(handlers) == 2
        assert handlers[0] == handler1
        assert handlers[1] == handler2

    def test_handler_ordering_first_wins(self):
        """First handler with value should win for same key."""
        handler1 = MockHandler({"account": "handler1_account"}, "handler1")
        handler2 = MockHandler({"account": "handler2_account"}, "handler2")

        source = self.TestSource(handlers=[handler1, handler2])
        values = source.discover(key="account")

        assert values["account"].value == "handler1_account"
        assert values["account"].source_name == "handler1"

    def test_handlers_complement_each_other(self):
        """Handlers should provide different keys."""
        handler1 = MockHandler({"key1": "value1"}, "handler1")
        handler2 = MockHandler({"key2": "value2"}, "handler2")

        source = self.TestSource(handlers=[handler1, handler2])
        values = source.discover()

        assert len(values) == 2
        assert values["key1"].value == "value1"
        assert values["key2"].value == "value2"

    def test_direct_values_override_handlers(self):
        """Direct values should take precedence over handler values."""
        handler = MockHandler({"account": "handler_account"}, "handler")
        direct_values = {"account": "direct_account"}

        source = self.TestSource(direct_values=direct_values, handlers=[handler])
        values = source.discover(key="account")

        assert values["account"].value == "direct_account"
        assert values["account"].source_name == "test_source"

    def test_discover_all_values_from_handlers(self):
        """Should discover all values when key is None."""
        handler1 = MockHandler({"key1": "value1", "key2": "value2"}, "handler1")
        handler2 = MockHandler({"key3": "value3"}, "handler2")

        source = self.TestSource(handlers=[handler1, handler2])
        values = source.discover()

        assert len(values) == 3
        assert "key1" in values
        assert "key2" in values
        assert "key3" in values

    def test_discover_specific_key_from_handlers(self):
        """Should discover specific key when provided."""
        handler = MockHandler({"key1": "value1", "key2": "value2"}, "handler")

        source = self.TestSource(handlers=[handler])
        values = source.discover(key="key1")

        assert len(values) == 1
        assert "key1" in values
        assert values["key1"].value == "value1"

    def test_handler_failure_does_not_break_discovery(self):
        """Failed handler should not prevent other handlers from working."""

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

            def discover(self, key=None):
                raise RuntimeError("Handler failed")

            def supports_key(self, key: str) -> bool:
                return True

        failing = FailingHandler()
        working = MockHandler({"key1": "value1"}, "working")

        source = self.TestSource(handlers=[failing, working])
        values = source.discover()

        # Should still get value from working handler
        assert len(values) == 1
        assert values["key1"].value == "value1"

    def test_add_handler_append(self):
        """Should append handler to end of list."""
        handler1 = MockHandler({"key1": "value1"}, "handler1")
        handler2 = MockHandler({"key2": "value2"}, "handler2")

        source = self.TestSource(handlers=[handler1])
        source.add_handler(handler2)

        handlers = source.get_handlers()
        assert len(handlers) == 2
        assert handlers[1] == handler2

    def test_add_handler_prepend(self):
        """Should prepend handler to beginning of list."""
        handler1 = MockHandler({"key1": "value1"}, "handler1")
        handler2 = MockHandler({"key2": "value2"}, "handler2")

        source = self.TestSource(handlers=[handler1])
        source.add_handler(handler2, position=0)

        handlers = source.get_handlers()
        assert len(handlers) == 2
        assert handlers[0] == handler2

    def test_add_handler_at_position(self):
        """Should insert handler at specific position."""
        handler1 = MockHandler({"key1": "value1"}, "handler1")
        handler2 = MockHandler({"key2": "value2"}, "handler2")
        handler3 = MockHandler({"key3": "value3"}, "handler3")

        source = self.TestSource(handlers=[handler1, handler3])
        source.add_handler(handler2, position=1)

        handlers = source.get_handlers()
        assert len(handlers) == 3
        assert handlers[1] == handler2

    def test_set_handlers(self):
        """Should replace all handlers with new list."""
        handler1 = MockHandler({"key1": "value1"}, "handler1")
        handler2 = MockHandler({"key2": "value2"}, "handler2")
        handler3 = MockHandler({"key3": "value3"}, "handler3")

        source = self.TestSource(handlers=[handler1, handler2])
        source.set_handlers([handler3])

        handlers = source.get_handlers()
        assert len(handlers) == 1
        assert handlers[0] == handler3

    def test_get_handlers_returns_copy(self):
        """get_handlers should return a copy, not the original list."""
        handler = MockHandler({"key1": "value1"}, "handler1")
        source = self.TestSource(handlers=[handler])

        handlers = source.get_handlers()
        handlers.clear()

        # Original list should be unchanged
        assert len(source.get_handlers()) == 1

    def test_empty_handlers_returns_direct_values_only(self):
        """With no handlers, should return only direct values."""
        direct_values = {"account": "direct_account"}
        source = self.TestSource(direct_values=direct_values, handlers=[])

        values = source.discover()

        assert len(values) == 1
        assert values["account"].value == "direct_account"

    def test_supports_key_checks_handlers(self):
        """supports_key should check handlers."""
        handler = MockHandler({"key1": "value1"}, "handler")
        source = self.TestSource(handlers=[handler])

        assert source.supports_key("key1") is True
        assert source.supports_key("nonexistent") is False
