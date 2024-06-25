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

import importlib
import inspect
import os
import pkgutil
from dataclasses import dataclass
from typing import Optional

from pygls.server import LanguageServer
from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.cli.app.snow_connector import connect_to_snowflake
from snowflake.snowpark import Session


@dataclass
class ConnectionParams:
    session_token: Optional[str]
    master_token: Optional[str]
    account: Optional[str]


class LSPPluginContext:
    """
    Contains context for LSP Plugins
    """

    def __init__(self) -> None:
        self._connection = self.create_connection()

    def get_connection(self):
        return self._connection

    def update_connection(self, params: ConnectionParams):
        self._connection = self._create_connection(params)

    def _create_connection(self, params: ConnectionParams):
        connection_attributes = {
            "account": params.account,
            "session_token": params.session_token,
            "master_token": params.master_token,
        }
        return connect_to_snowflake(temporary_connection=True, **connection_attributes)

    def create_connection(self) -> Session:
        params = ConnectionParams(
            session_token=os.environ.get("SESSION_TOKEN"),
            master_token=os.environ.get("MASTER_TOKEN"),
            account=os.environ.get("SF_ACCOUNT"),
        )

        return self._create_connection(params)


class RpcManager(SqlExecutionMixin):
    """
    Base class LSP Controller
    """

    def __init__(self):
        super().__init__()
        server = LanguageServer(name="lsp_controller", version="v0.0.1")
        context = LSPPluginContext()

        plugins = self.discover_lsp_plugins("snowflake.cli.plugins")
        for plugin_func in plugins:
            plugin_func(server, context)
        server.start_io()

    def discover_lsp_plugins(self, package_name):
        plugins = []
        package = importlib.import_module(package_name)
        for _, module_name, is_pkg in pkgutil.iter_modules(package.__path__):
            if is_pkg:
                submodule = importlib.import_module(f"{package_name}.{module_name}")
                for _, submodule_name, is_pkg in pkgutil.iter_modules(
                    submodule.__path__
                ):
                    module = importlib.import_module(
                        f"{package_name}.{module_name}.{submodule_name}"
                    )
                    for _name, obj in inspect.getmembers(module, inspect.isfunction):
                        if hasattr(obj, "_lsp_context"):
                            plugins.append(obj)
        return plugins
