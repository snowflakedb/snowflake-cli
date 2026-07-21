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

from snowflake.cli._plugins.dcm.env import collect_env_vars


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
