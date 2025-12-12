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

"""Tests for configuration merge operations."""

from snowflake.cli.api.config_ng.merge_operations import (
    create_default_connection_from_params,
    extract_root_level_connection_params,
    merge_params_into_connections,
)


class TestExtractRootLevelConnectionParams:
    """Test extract_root_level_connection_params function."""

    def test_extract_connection_params_from_mixed_config(self):
        """Test extracting connection params from config with sections."""
        config = {
            "account": "test_account",
            "user": "test_user",
            "connections": {"dev": {"database": "db"}},
            "cli": {"enable_diag": True},
        }

        conn_params, remaining = extract_root_level_connection_params(config)

        assert conn_params == {"account": "test_account", "user": "test_user"}
        assert "connections" in remaining
        assert "cli" in remaining
        assert "account" not in remaining
        assert "user" not in remaining

    def test_extract_with_no_connection_params(self):
        """Test extraction when no root-level connection params exist."""
        config = {
            "connections": {"dev": {"account": "acc"}},
            "variables": {"key": "value"},
        }

        conn_params, remaining = extract_root_level_connection_params(config)

        assert conn_params == {}
        assert remaining == config

    def test_extract_with_only_connection_params(self):
        """Test extraction when only connection params exist."""
        config = {"account": "acc", "user": "usr", "password": "pwd"}

        conn_params, remaining = extract_root_level_connection_params(config)

        assert conn_params == config
        assert remaining == {}

    def test_extract_ignores_internal_cli_parameters(self):
        """Test that internal CLI parameters are not treated as connection params."""
        config = {
            "account": "acc",
            "enable_diag": True,
            "temporary_connection": True,
            "default_connection_name": "dev",
        }

        conn_params, remaining = extract_root_level_connection_params(config)

        assert conn_params == {"account": "acc"}
        assert "enable_diag" in remaining
        assert "temporary_connection" in remaining
        assert "default_connection_name" in remaining

    def test_extract_recognizes_all_sections(self):
        """Test that all ConfigSection values are recognized as sections."""
        config = {
            "account": "acc",
            "connections": {},
            "variables": {},
            "cli": {},
        }

        conn_params, remaining = extract_root_level_connection_params(config)

        assert conn_params == {"account": "acc"}
        assert "connections" in remaining
        assert "variables" in remaining
        assert "cli" in remaining

    def test_extract_with_nested_section_names(self):
        """Test extraction with nested section names like cli.logs."""
        config = {
            "account": "acc",
            "cli.logs": {"save_logs": True},
            "cli.features": {"feature1": True},
        }

        conn_params, remaining = extract_root_level_connection_params(config)

        assert conn_params == {"account": "acc"}
        assert "cli.logs" in remaining
        assert "cli.features" in remaining

    def test_extract_empty_config(self):
        """Test extraction with empty config."""
        conn_params, remaining = extract_root_level_connection_params({})

        assert conn_params == {}
        assert remaining == {}

    def test_extract_preserves_nested_structures(self):
        """Test that nested structures in sections are preserved."""
        config = {
            "account": "acc",
            "connections": {"dev": {"nested": {"deep": "value"}}},
        }

        conn_params, remaining = extract_root_level_connection_params(config)

        assert conn_params == {"account": "acc"}
        assert remaining["connections"]["dev"]["nested"]["deep"] == "value"


class TestMergeParamsIntoConnections:
    """Test merge_params_into_connections function."""

    def test_merge_params_into_single_connection(self):
        """Test merging params into a single connection."""
        connections = {"dev": {"account": "dev_acc", "user": "dev_user"}}
        params = {"password": "new_pass"}

        result = merge_params_into_connections(connections, params)

        assert result["dev"]["account"] == "dev_acc"
        assert result["dev"]["user"] == "dev_user"
        assert result["dev"]["password"] == "new_pass"

    def test_merge_params_into_multiple_connections(self):
        """Test merging params into multiple connections."""
        connections = {
            "dev": {"account": "dev_acc"},
            "prod": {"account": "prod_acc"},
        }
        params = {"user": "global_user", "password": "global_pass"}

        result = merge_params_into_connections(connections, params)

        assert result["dev"]["user"] == "global_user"
        assert result["dev"]["password"] == "global_pass"
        assert result["prod"]["user"] == "global_user"
        assert result["prod"]["password"] == "global_pass"

    def test_merge_params_override_connection_values(self):
        """Test that params override existing connection values."""
        connections = {"dev": {"account": "old_acc", "user": "old_user"}}
        params = {"user": "new_user"}

        result = merge_params_into_connections(connections, params)

        assert result["dev"]["account"] == "old_acc"
        assert result["dev"]["user"] == "new_user"

    def test_merge_empty_params(self):
        """Test merging with empty params."""
        connections = {"dev": {"account": "acc"}}
        params = {}

        result = merge_params_into_connections(connections, params)

        assert result == connections

    def test_merge_into_empty_connections(self):
        """Test merging params into empty connections dict."""
        connections = {}
        params = {"account": "acc"}

        result = merge_params_into_connections(connections, params)

        assert result == {}

    def test_merge_preserves_original_connections(self):
        """Test that original connections dict is not modified."""
        connections = {"dev": {"account": "acc"}}
        params = {"user": "usr"}

        result = merge_params_into_connections(connections, params)

        # Original should be unchanged
        assert "user" not in connections["dev"]
        # Result should have merged values
        assert result["dev"]["user"] == "usr"

    def test_merge_nested_connection_values(self):
        """Test merging with nested connection structures."""
        connections = {"dev": {"account": "acc", "nested": {"key": "value"}}}
        params = {"nested": {"key": "new_value", "new_key": "new"}}

        result = merge_params_into_connections(connections, params)

        assert result["dev"]["nested"]["key"] == "new_value"
        assert result["dev"]["nested"]["new_key"] == "new"

    def test_merge_handles_non_dict_connection(self):
        """Test that non-dict connection values are preserved."""
        connections = {"dev": {"account": "acc"}, "invalid": "not_a_dict"}
        params = {"user": "usr"}

        result = merge_params_into_connections(connections, params)

        assert result["dev"]["user"] == "usr"
        assert result["invalid"] == "not_a_dict"


class TestCreateDefaultConnectionFromParams:
    """Test create_default_connection_from_params function."""

    def test_create_default_connection(self):
        """Test creating default connection from params."""
        params = {"account": "test_acc", "user": "test_user"}

        result = create_default_connection_from_params(params)

        assert "default" in result
        assert result["default"] == params

    def test_create_default_with_single_param(self):
        """Test creating default connection with single param."""
        params = {"account": "test_acc"}

        result = create_default_connection_from_params(params)

        assert result == {"default": {"account": "test_acc"}}

    def test_create_default_with_empty_params(self):
        """Test creating default connection with empty params."""
        result = create_default_connection_from_params({})

        assert result == {}

    def test_create_default_preserves_original_params(self):
        """Test that original params dict is not modified."""
        params = {"account": "acc"}

        result = create_default_connection_from_params(params)

        # Modify result
        result["default"]["user"] = "usr"

        # Original should be unchanged
        assert "user" not in params

    def test_create_default_with_complex_params(self):
        """Test creating default connection with nested params."""
        params = {
            "account": "acc",
            "user": "usr",
            "nested": {"key": "value"},
        }

        result = create_default_connection_from_params(params)

        assert result["default"]["nested"]["key"] == "value"
