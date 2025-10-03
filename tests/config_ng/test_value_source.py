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
Unit tests for ValueSource interface.

Tests verify:
- Abstract interface cannot be instantiated
- All abstract methods must be implemented
- Concrete implementations work correctly
- Common protocol is enforced
"""

import pytest
from snowflake.cli.api.config_ng.core import ConfigValue, SourcePriority, ValueSource


class TestValueSourceInterface:
    """Test suite for ValueSource abstract interface."""

    def test_cannot_instantiate_abstract_class(self):
        """Should not be able to instantiate ValueSource directly."""
        with pytest.raises(TypeError):
            ValueSource()

    def test_must_implement_source_name(self):
        """Concrete implementations must implement source_name property."""

        class IncompleteSource(ValueSource):
            @property
            def priority(self) -> SourcePriority:
                return SourcePriority.FILE

            def discover(self, key=None):
                return {}

            def supports_key(self, key: str) -> bool:
                return True

        with pytest.raises(TypeError):
            IncompleteSource()

    def test_must_implement_priority(self):
        """Concrete implementations must implement priority property."""

        class IncompleteSource(ValueSource):
            @property
            def source_name(self) -> str:
                return "test"

            def discover(self, key=None):
                return {}

            def supports_key(self, key: str) -> bool:
                return True

        with pytest.raises(TypeError):
            IncompleteSource()

    def test_must_implement_discover(self):
        """Concrete implementations must implement discover method."""

        class IncompleteSource(ValueSource):
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

    def test_must_implement_supports_key(self):
        """Concrete implementations must implement supports_key method."""

        class IncompleteSource(ValueSource):
            @property
            def source_name(self) -> str:
                return "test"

            @property
            def priority(self) -> SourcePriority:
                return SourcePriority.FILE

            def discover(self, key=None):
                return {}

        with pytest.raises(TypeError):
            IncompleteSource()

    def test_complete_implementation(self):
        """Should be able to instantiate with all methods implemented."""

        class CompleteSource(ValueSource):
            @property
            def source_name(self) -> str:
                return "test_source"

            @property
            def priority(self) -> SourcePriority:
                return SourcePriority.FILE

            def discover(self, key=None):
                return {}

            def supports_key(self, key: str) -> bool:
                return True

        source = CompleteSource()
        assert source.source_name == "test_source"
        assert source.priority == SourcePriority.FILE


class TestValueSourceConcreteImplementation:
    """Test a concrete implementation of ValueSource."""

    class MockSource(ValueSource):
        """Mock source for testing."""

        def __init__(self, data: dict):
            self._data = data

        @property
        def source_name(self) -> str:
            return "mock_source"

        @property
        def priority(self) -> SourcePriority:
            return SourcePriority.FILE

        def discover(self, key=None):
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
            else:
                return {}

        def supports_key(self, key: str) -> bool:
            return key in self._data

    def test_discover_all_values(self):
        """Should discover all values when key is None."""
        source = self.MockSource({"account": "test_account", "user": "test_user"})

        values = source.discover()

        assert len(values) == 2
        assert "account" in values
        assert "user" in values
        assert values["account"].value == "test_account"
        assert values["user"].value == "test_user"

    def test_discover_specific_key(self):
        """Should discover specific key when provided."""
        source = self.MockSource({"account": "test_account", "user": "test_user"})

        values = source.discover(key="account")

        assert len(values) == 1
        assert "account" in values
        assert values["account"].value == "test_account"

    def test_discover_nonexistent_key(self):
        """Should return empty dict for nonexistent key."""
        source = self.MockSource({"account": "test_account"})

        values = source.discover(key="nonexistent")

        assert len(values) == 0

    def test_supports_existing_key(self):
        """Should return True for existing key."""
        source = self.MockSource({"account": "test_account"})

        assert source.supports_key("account") is True

    def test_supports_nonexistent_key(self):
        """Should return False for nonexistent key."""
        source = self.MockSource({"account": "test_account"})

        assert source.supports_key("nonexistent") is False

    def test_source_name_is_accessible(self):
        """Should be able to access source_name property."""
        source = self.MockSource({})

        assert source.source_name == "mock_source"

    def test_priority_is_accessible(self):
        """Should be able to access priority property."""
        source = self.MockSource({})

        assert source.priority == SourcePriority.FILE

    def test_discovered_values_have_correct_metadata(self):
        """Discovered values should have correct metadata."""
        source = self.MockSource({"account": "test_account"})

        values = source.discover(key="account")
        config_value = values["account"]

        assert config_value.source_name == "mock_source"
        assert config_value.priority == SourcePriority.FILE
        assert config_value.key == "account"
        assert config_value.value == "test_account"

    def test_discover_returns_dict_of_config_values(self):
        """discover() should return Dict[str, ConfigValue]."""
        source = self.MockSource({"account": "test_account"})

        values = source.discover()

        assert isinstance(values, dict)
        for key, value in values.items():
            assert isinstance(key, str)
            assert isinstance(value, ConfigValue)

    def test_empty_source_discover(self):
        """Should handle empty source gracefully."""
        source = self.MockSource({})

        values = source.discover()

        assert len(values) == 0
        assert isinstance(values, dict)

    def test_multiple_sources_with_different_priorities(self):
        """Should be able to create sources with different priorities."""

        class HighPrioritySource(ValueSource):
            @property
            def source_name(self) -> str:
                return "high_priority"

            @property
            def priority(self) -> SourcePriority:
                return SourcePriority.CLI_ARGUMENT

            def discover(self, key=None):
                return {}

            def supports_key(self, key: str) -> bool:
                return False

        class LowPrioritySource(ValueSource):
            @property
            def source_name(self) -> str:
                return "low_priority"

            @property
            def priority(self) -> SourcePriority:
                return SourcePriority.FILE

            def discover(self, key=None):
                return {}

            def supports_key(self, key: str) -> bool:
                return False

        high = HighPrioritySource()
        low = LowPrioritySource()

        assert high.priority.value < low.priority.value
