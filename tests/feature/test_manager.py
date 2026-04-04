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

"""Tests for FeatureManager — mocks the decl library."""

from unittest import mock

import pytest


@pytest.fixture
def mock_execute_query():
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager.execute_query"
    ) as m:
        m.return_value = iter([])
        yield m


@pytest.fixture
def mock_decl():
    """Patch the entire decl api module used inside the manager."""
    with mock.patch("snowflake.cli._plugins.feature.manager.decl_api") as m:
        m.load_specs.return_value = mock.MagicMock(name="batch")
        m.fetch_applied_state.return_value = mock.MagicMock(name="state")
        m.validate_specs.return_value = []
        m.generate_plan.return_value = mock.MagicMock(name="plan", ops=[])
        yield m


class TestFeatureManagerApply:
    def test_apply_dry_run_returns_dict(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dry_run=True,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert isinstance(result, dict)

    def test_apply_dry_run_does_not_execute_sql(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dry_run=True,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        # execute_query should only be called for SHOW queries (state fetch), not for plan ops
        for call in mock_execute_query.call_args_list:
            sql = call[0][0] if call[0] else call[1].get("query", "")
            assert "SHOW" in sql.upper() or sql == ""

    def test_apply_calls_load_specs(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        mock_decl.load_specs.assert_called_once()

    def test_apply_not_implemented_error_returns_placeholder(
        self, mock_execute_query, mock_decl
    ):
        """If load_specs raises NotImplementedError, apply should still return a dict."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.load_specs.side_effect = NotImplementedError("Phase 1 not done")
        mgr = FeatureManager()
        result = mgr.apply(
            input_files=["specs.yaml"],
            config=None,
            dry_run=False,
            dev_mode=False,
            overwrite=False,
            allow_recreate=False,
        )
        assert isinstance(result, dict)
        assert "status" in result or "message" in result or "error" in result


class TestFeatureManagerListSpecs:
    def test_list_specs_returns_dict(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.list_specs(input_files=(), config=None)
        assert isinstance(result, dict)

    def test_list_specs_not_implemented_returns_placeholder(
        self, mock_execute_query, mock_decl
    ):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mock_decl.load_specs.side_effect = NotImplementedError("Phase 1 not done")
        mgr = FeatureManager()
        result = mgr.list_specs(input_files=("specs.yaml",), config=None)
        assert isinstance(result, dict)


class TestFeatureManagerDescribe:
    def test_describe_returns_dict(self, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.describe(name="MY_ENTITY")
        assert isinstance(result, dict)


class TestFeatureManagerDrop:
    def test_drop_returns_dict(self, mock_execute_query):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.drop(names=("MY_ENTITY",))
        assert isinstance(result, dict)


class TestFeatureManagerConvert:
    def test_convert_returns_dict(self, mock_execute_query, mock_decl):
        from snowflake.cli._plugins.feature.manager import FeatureManager

        mgr = FeatureManager()
        result = mgr.convert(
            input_files=["specs.py"],
            file_format="yaml",
            output_dir=None,
            recursive=False,
            config=None,
        )
        assert isinstance(result, dict)
