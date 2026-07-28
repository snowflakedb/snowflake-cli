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

from unittest import mock

import pytest
from snowflake.cli._plugins.dcm.env import collect_env_vars
from snowflake.cli._plugins.dcm.models import DCMTemplating

from tests_common import IS_WINDOWS


def test_collect_env_vars_collects_present_names(monkeypatch):
    monkeypatch.setenv("DB_HOST", "prod.analytics.internal")
    monkeypatch.setenv("WH_SIZE", "XLARGE")

    result = collect_env_vars({"DB_HOST", "WH_SIZE"})

    assert result == {"DB_HOST": "prod.analytics.internal", "WH_SIZE": "XLARGE"}


def test_collect_env_vars_omits_absent_names(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.setenv("WH_SIZE", "XLARGE")

    result = collect_env_vars({"DB_HOST", "WH_SIZE"})

    assert result == {"WH_SIZE": "XLARGE"}


def test_collect_env_vars_ignores_undeclared_names(monkeypatch):
    monkeypatch.setenv("DB_HOST", "prod.analytics.internal")
    monkeypatch.setenv("SOME_UNRELATED_VAR", "should-not-leak")

    result = collect_env_vars({"DB_HOST"})

    assert result == {"DB_HOST": "prod.analytics.internal"}


def test_collect_env_vars_empty_declared_names_returns_empty_dict(monkeypatch):
    monkeypatch.setenv("DB_HOST", "prod.analytics.internal")

    result = collect_env_vars(set())

    assert result == {}


def test_collect_env_vars_none_present_returns_empty_dict(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.delenv("WH_SIZE", raising=False)

    result = collect_env_vars({"DB_HOST", "WH_SIZE"})

    assert result == {}


def test_collect_env_vars_warns_about_missing_names(monkeypatch):
    monkeypatch.delenv("DB_HOST", raising=False)
    monkeypatch.setenv("WH_SIZE", "XLARGE")

    with mock.patch("snowflake.cli._plugins.dcm.env.cli_console") as mock_console:
        result = collect_env_vars({"DB_HOST", "WH_SIZE"})

    assert result == {"WH_SIZE": "XLARGE"}
    mock_console.warning.assert_called_once()
    warning_message = mock_console.warning.call_args[0][0]
    assert "the shell environment" in warning_message
    assert "DB_HOST" in warning_message


def test_collect_env_vars_does_not_warn_when_all_present(monkeypatch):
    monkeypatch.setenv("DB_HOST", "prod.analytics.internal")

    with mock.patch("snowflake.cli._plugins.dcm.env.cli_console") as mock_console:
        collect_env_vars({"DB_HOST"})

    mock_console.warning.assert_not_called()


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="os.environ folds env-var name case on Windows; DB_HOST/db_host collapse to one slot",
)
def test_collect_env_vars_treats_different_case_names_as_distinct(monkeypatch):
    """DB_HOST and db_host are two different declared names (exact string
    equality, matching how GS matches them server-side) -- on this
    case-sensitive platform they resolve independently, to two different
    values, not collapsed into one."""
    monkeypatch.setenv("DB_HOST", "upper-value")
    monkeypatch.setenv("db_host", "lower-value")

    result = collect_env_vars({"DB_HOST", "db_host"})

    assert result == {"DB_HOST": "upper-value", "db_host": "lower-value"}


@pytest.mark.skipif(
    not IS_WINDOWS,
    reason="Windows-only: os.environ folds env-var name case there, so a "
    "manifest-declared name resolves against a differently-cased host "
    "process variable. The mirror-image assumption "
    "(different-case names are DISTINCT) is pinned for Linux/Mac by "
    "test_collect_env_vars_treats_different_case_names_as_distinct above; "
    "this is the same mechanism from the other side, on the platform "
    "where it actually applies.",
)
def test_collect_env_vars_resolves_lowercase_manifest_names_against_uppercase_host_variables(
    monkeypatch,
):
    """A manifest declaring 'db_host' (env_vars) and 'api_key'
    (env_secrets), both lowercase, must resolve against real Windows
    process variables actually set as DB_HOST/API_KEY (uppercase) -- the
    CLI doesn't distinguish env_vars from env_secrets once they're
    collected into one declared-name set (DCMTemplating.declared_variable_names),
    so this exercises the same collect_env_vars() resolution for both."""
    monkeypatch.setenv("DB_HOST", "prod.analytics.internal")
    monkeypatch.setenv("API_KEY", "shhh-secret-value")
    templating = DCMTemplating.from_dict(
        {"env_vars": [{"db_host": None}], "env_secrets": [{"api_key": None}]}
    )

    result = collect_env_vars(templating.declared_variable_names)

    assert result == {
        "db_host": "prod.analytics.internal",
        "api_key": "shhh-secret-value",
    }
