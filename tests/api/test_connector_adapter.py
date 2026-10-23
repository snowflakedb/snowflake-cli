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

from __future__ import annotations

import ast
import importlib
import sys
from pathlib import Path
from unittest import mock

from snowflake.cli.api.connector_adapter import connector_adapter

_FALLBACK = "********"


def test_mask_secrets_uses_connector_detector():
    secret = "Hunter2Hunter2"
    masked = connector_adapter.mask_secrets(
        f"password={secret}",
        fallback=_FALLBACK,
    )
    assert secret not in masked
    assert masked != _FALLBACK


def test_mask_secrets_returns_fallback_when_module_missing(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def _blocked(name, global_ns=None, local_ns=None, fromlist=(), level=0):
        if name == "snowflake.connector.secret_detector":
            raise ModuleNotFoundError(
                "No module named 'snowflake.connector.secret_detector'"
            )
        return real_import(name, global_ns, local_ns, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _blocked)
    note = "password=Hunter2Hunter2"
    assert connector_adapter.mask_secrets(note, fallback=_FALLBACK) == _FALLBACK


def test_mask_secrets_returns_fallback_when_detector_reports_an_error():
    fake = mock.Mock()
    fake.masked_text = "password=Hunter2Hunter2"
    fake.error_str = "boom"
    with mock.patch(
        "snowflake.connector.secret_detector.SecretDetector.mask_secrets",
        return_value=fake,
    ):
        assert (
            connector_adapter.mask_secrets(
                "password=Hunter2Hunter2", fallback=_FALLBACK
            )
            == _FALLBACK
        )


def test_mask_secrets_keeps_text_when_detector_returns_no_masked_text():
    fake = mock.Mock()
    fake.masked_text = None
    fake.error_str = None
    with mock.patch(
        "snowflake.connector.secret_detector.SecretDetector.mask_secrets",
        return_value=fake,
    ):
        assert (
            connector_adapter.mask_secrets("plain note", fallback=_FALLBACK)
            == "plain note"
        )


def test_importing_adapter_does_not_import_secret_detector():
    sys.modules.pop("snowflake.connector.secret_detector", None)
    reloaded = importlib.reload(
        importlib.import_module("snowflake.cli.api.connector_adapter")
    )
    assert "snowflake.connector.secret_detector" not in sys.modules
    assert reloaded.connector_adapter.mask_secrets is not None


def test_secret_detector_import_stays_inside_mask_secrets():
    source = (
        Path(__file__).resolve().parents[2]
        / "src/snowflake/cli/api/connector_adapter.py"
    ).read_text(encoding="utf-8")
    tree = ast.parse(source)
    module_imports = [
        node
        for node in tree.body
        if isinstance(node, (ast.Import, ast.ImportFrom))
        and _imports_secret_detector(node)
    ]
    assert module_imports == []
    method = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "ConnectorAdapter"
        for node in node.body
        if isinstance(node, ast.FunctionDef) and node.name == "mask_secrets"
    )
    nested = [
        node
        for node in ast.walk(method)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        and _imports_secret_detector(node)
    ]
    assert len(nested) == 1


def _imports_secret_detector(node: ast.AST) -> bool:
    if isinstance(node, ast.ImportFrom):
        return node.module == "snowflake.connector.secret_detector"
    if isinstance(node, ast.Import):
        return any(
            alias.name == "snowflake.connector.secret_detector" for alias in node.names
        )
    return False
