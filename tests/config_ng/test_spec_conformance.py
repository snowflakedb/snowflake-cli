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
Conformance test runner for snowflake-config-spec.

Loads YAML-driven test cases from the external snowflake-config-spec repo
and verifies that the snowflake-cli Ngconfig resolver produces the same
results as the reference implementation.

The spec defines a 5-level source stack that maps to Ngconfig sources:
  1. config_toml   -> CliConfigFile      (FILE)
  2. connections_toml -> ConnectionsConfigFile (FILE)
  3. general_env   -> CliEnvironment     (OVERLAY)
  4. connection_specific_env -> ConnectionSpecificEnvironment (OVERLAY)
  5. cli_arguments -> CliParameters      (OVERLAY)

Usage:
    pytest tests/config_ng/test_spec_conformance.py -v
"""

from __future__ import annotations

import copy
import os
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

from snowflake.cli.api.config_ng.resolver import ConfigurationResolver
from snowflake.cli.api.config_ng.sources import (
    CliConfigFile,
    CliEnvironment,
    CliParameters,
    ConnectionsConfigFile,
    ConnectionSpecificEnvironment,
)

SPEC_ROOT = Path(__file__).resolve().parent.parent.parent.parent / "snowflake-config-spec"
TEST_CASES_DIR = SPEC_ROOT / "test_cases"


def _load_all_test_cases() -> List[Dict[str, Any]]:
    if not TEST_CASES_DIR.exists():
        return []

    cases: List[Dict[str, Any]] = []
    for yaml_file in sorted(TEST_CASES_DIR.glob("*.yaml")):
        with open(yaml_file) as f:
            suite_data = yaml.safe_load(f)

        suite_name = suite_data.get("suite", yaml_file.stem)
        for tc in suite_data.get("test_cases", []):
            tc["_suite"] = suite_name
            tc["_file"] = yaml_file.name
            cases.append(tc)

    return cases


ALL_CASES = _load_all_test_cases()


def _case_id(case: Dict[str, Any]) -> str:
    return f"{case['_file']}::{case['id']}"


def _resolve_connection_via_ngconfig(
    connection_name: str,
    config_toml: str | None = None,
    connections_toml: str | None = None,
    env_vars: Dict[str, str] | None = None,
    cli_args: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """
    Build the spec's 5-level source stack using Ngconfig classes and resolve.

    Environment variables are injected into os.environ temporarily because
    CliEnvironment and ConnectionSpecificEnvironment read from os.environ.
    """
    sources = []

    if config_toml is not None:
        sources.append(CliConfigFile.from_string(config_toml))

    if connections_toml is not None:
        sources.append(ConnectionsConfigFile.from_string(connections_toml))

    # Env-based sources always participate (they read os.environ)
    sources.append(CliEnvironment())
    sources.append(ConnectionSpecificEnvironment())

    if cli_args:
        filtered = {k: v for k, v in cli_args.items() if v is not None}
        sources.append(CliParameters(cli_context=filtered))
    else:
        sources.append(CliParameters(cli_context={}))

    resolver = ConfigurationResolver(sources=sources, enable_history=False)
    config = resolver.resolve()
    return config.get("connections", {}).get(connection_name, {})


skipif_no_spec = pytest.mark.skipif(
    not TEST_CASES_DIR.exists(),
    reason=f"snowflake-config-spec not found at {SPEC_ROOT}",
)


@skipif_no_spec
@pytest.mark.parametrize("case", ALL_CASES, ids=[_case_id(c) for c in ALL_CASES])
def test_spec_conformance(case: Dict[str, Any]) -> None:
    sources = case.get("sources", {}) or {}

    config_toml = sources.get("config_toml")
    connections_toml = sources.get("connections_toml")
    env_vars = sources.get("env_vars") or {}
    cli_args = sources.get("cli_args") or {}

    connection_name = case["resolve"]["connection"]

    original_env = copy.deepcopy(dict(os.environ))
    try:
        # Clear any pre-existing SNOWFLAKE_* env vars that could interfere
        for key in list(os.environ.keys()):
            if key.startswith("SNOWFLAKE_"):
                del os.environ[key]

        os.environ.update(env_vars)

        result = _resolve_connection_via_ngconfig(
            connection_name=connection_name,
            config_toml=config_toml,
            connections_toml=connections_toml,
            env_vars=env_vars,
            cli_args=cli_args,
        )
    finally:
        os.environ.clear()
        os.environ.update(original_env)

    expected = case.get("expected", {})

    expected_fields = expected.get("fields", {})
    if expected_fields:
        for key, value in expected_fields.items():
            assert key in result, (
                f"[{case['id']}] Expected key '{key}' not found in resolved connection. "
                f"Got keys: {list(result.keys())}"
            )
            assert result[key] == value, (
                f"[{case['id']}] Key '{key}': expected {value!r}, got {result[key]!r}"
            )

    absent_keys = expected.get("absent", [])
    for key in absent_keys:
        assert key not in result, (
            f"[{case['id']}] Key '{key}' should be absent but found with value {result.get(key)!r}"
        )
