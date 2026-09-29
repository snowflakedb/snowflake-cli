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

"""Tests for ``FeatureManager.sync`` — Step 5 of the TDD cycle.

All tests in this file initially fail because ``FeatureManager.sync``
does not exist yet.  They pass once Phase 2 (Step 6) is implemented.
"""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest import mock

import pytest

# ---------------------------------------------------------------------------
# Manifest helpers (duplicated from test_manager.py; cross-repo imports
# are prohibited — tests in each repo reference only paths inside that repo)
# ---------------------------------------------------------------------------

_DEFAULT_MANIFEST_YAML = textwrap.dedent(
    """\
    manifest_version: 1
    type: feature_store
    default_target: DEFAULT
    targets:
      DEFAULT:
        account_identifier: TEST_ORG-TEST_ACCT
        database: TEST_DB
        schema: TEST_SCHEMA
        role: TEST_ROLE
    """
)


def _write_manifest(
    project_root: Path, *, yaml_text: str = _DEFAULT_MANIFEST_YAML
) -> Path:
    project_root.mkdir(parents=True, exist_ok=True)
    manifest = project_root / "manifest.yml"
    manifest.write_text(yaml_text)
    return manifest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_execute_query():
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager.execute_query"
    ) as m:
        m.return_value = iter([])
        yield m


@pytest.fixture
def mock_decl():
    with mock.patch("snowflake.cli._plugins.feature.manager.decl_api") as m:
        _export_result = {
            "status": "exported",
            "directory": "/tmp/sources",
            "files": ["a.yaml"],
        }
        m.export_specs.return_value = _export_result
        m.export_specs_as_python.return_value = _export_result
        m.assert_feature_store_initialized = mock.MagicMock(
            name="assert_feature_store_initialized",
            return_value=mock.MagicMock(name="FeatureStore"),
        )
        yield m


@pytest.fixture(autouse=True)
def mock_cli_context():
    with mock.patch("snowflake.cli._plugins.feature.manager.get_cli_context") as m:
        ctx = mock.MagicMock()
        ctx.connection.database = "TEST_DB"
        ctx.connection.schema = "TEST_SCHEMA"
        ctx.connection.warehouse = "TEST_WH"
        ctx.connection.role = "TEST_ROLE"
        ctx.connection.account = "TEST_ORG-TEST_ACCT"
        m.return_value = ctx
        yield m


@pytest.fixture(autouse=True)
def mock_account_identifier():
    from snowflake.cli.api.identifiers import AccountIdentifier

    with mock.patch(
        "snowflake.cli._plugins.feature.manager.get_account_identifier",
        return_value=AccountIdentifier("TEST_ORG", "TEST_ACCT"),
    ) as m:
        yield m


@pytest.fixture(autouse=True)
def mock_build_session():
    with mock.patch(
        "snowflake.cli._plugins.feature.manager.FeatureManager._build_session",
        return_value=mock.MagicMock(name="session"),
    ):
        yield


# ---------------------------------------------------------------------------
# TestFeatureManagerSync
# ---------------------------------------------------------------------------


class TestFeatureManagerSync:
    def test_sync_requires_manifest(
        self, tmp_path: Path, mock_decl, mock_execute_query
    ):
        """No manifest.yml → ClickException directing user to run init first."""
        from click.exceptions import ClickException
        from snowflake.cli._plugins.feature.manager import FeatureManager

        # tmp_path has no manifest.yml
        with pytest.raises(ClickException):
            FeatureManager().sync(
                from_dir=tmp_path,
                target_name=None,
                name_filter=None,
                python=False,
            )

    def test_sync_calls_assert_initialized(
        self, tmp_path: Path, mock_decl, mock_execute_query
    ):
        """sync calls decl_api.assert_feature_store_initialized after resolving the project."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        FeatureManager().sync(
            from_dir=tmp_path,
            target_name=None,
            name_filter=None,
            python=False,
        )
        mock_decl.assert_feature_store_initialized.assert_called_once()

    def test_sync_calls_export_specs(
        self, tmp_path: Path, mock_decl, mock_execute_query
    ):
        """sync calls decl_api.export_specs with correct db, schema, and layout."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        FeatureManager().sync(
            from_dir=tmp_path,
            target_name=None,
            name_filter=None,
            python=False,
        )
        mock_decl.export_specs.assert_called_once()
        # database and schema are passed positionally: (show_rows, {}, output_dir, db, schema, ...)
        call_args = mock_decl.export_specs.call_args
        assert call_args.args[3] == "TEST_DB"
        assert call_args.args[4] == "TEST_SCHEMA"

    def test_sync_threads_name_filter(
        self, tmp_path: Path, mock_decl, mock_execute_query
    ):
        """name_filter='MY_FV' reaches decl_api.export_specs(name_filter='MY_FV')."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        FeatureManager().sync(
            from_dir=tmp_path,
            target_name=None,
            name_filter="MY_FV",
            python=False,
        )
        mock_decl.export_specs.assert_called_once()
        call_kwargs = mock_decl.export_specs.call_args.kwargs
        assert call_kwargs.get("name_filter") == "MY_FV"

    def test_sync_python_calls_export_as_python(
        self, tmp_path: Path, mock_decl, mock_execute_query
    ):
        """python=True routes to decl_api.export_specs_as_python, not export_specs."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        FeatureManager().sync(
            from_dir=tmp_path,
            target_name=None,
            name_filter=None,
            python=True,
        )
        mock_decl.export_specs_as_python.assert_called_once()
        mock_decl.export_specs.assert_not_called()

    def test_sync_result_envelope(self, tmp_path: Path, mock_decl, mock_execute_query):
        """Return dict has status='synced', files, directory, target_database, target_schema."""
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)
        result = FeatureManager().sync(
            from_dir=tmp_path,
            target_name=None,
            name_filter=None,
            python=False,
        )
        assert result["status"] == "synced"
        assert "files" in result
        assert "directory" in result
        assert result["target_database"] == "TEST_DB"
        assert result["target_schema"] == "TEST_SCHEMA"

    def test_sync_does_not_call_init_feature_store(
        self, tmp_path: Path, mock_decl, mock_execute_query
    ):
        """sync must NOT construct FeatureStore with CREATE_IF_NOT_EXIST mode.

        FeatureStore is lazy-imported inside init(); patching at source module level.
        """
        from snowflake.cli._plugins.feature.manager import FeatureManager

        _write_manifest(tmp_path)

        with mock.patch(
            "snowflake.ml.feature_store.feature_store.FeatureStore",
            autospec=True,
        ) as mock_fs_class:
            FeatureManager().sync(
                from_dir=tmp_path,
                target_name=None,
                name_filter=None,
                python=False,
            )
        mock_fs_class.assert_not_called()
