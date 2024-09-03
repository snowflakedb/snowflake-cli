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

from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Optional

from snowflake.cli.api.connections import ConnectionContext, OpenConnectionCache
from snowflake.cli.api.exceptions import MissingConfiguration
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.rendering.jinja import CONTEXT_KEY
from snowflake.connector import SnowflakeConnection

if TYPE_CHECKING:
    from snowflake.cli.api.project.definition_manager import DefinitionManager
    from snowflake.cli.api.project.schemas.project_definition import ProjectDefinition

_CONNECTION_CACHE = OpenConnectionCache()


class _CliGlobalContextManager:
    _definition_manager: Optional[DefinitionManager]
    _override_project_definition: Optional[ProjectDefinition]

    def __init__(self):
        self._connection_context = ConnectionContext()
        self._definition_manager = None
        self._enable_tracebacks = True
        self._output_format = OutputFormat.TABLE
        self._verbose = False
        self._experimental = False
        self._project_path_arg = None
        self._project_is_optional = True
        self._project_env_overrides_args = {}
        self._override_project_definition = (
            None  # TODO: remove; for implicit v1 <-> v2 conversion
        )
        self._silent: bool = False

    def reset(self):
        self.__init__()

    def clone(self) -> _CliGlobalContextManager:
        mgr = _CliGlobalContextManager()
        mgr.set_connection_context(self.connection_context.clone())
        mgr._set_definition_manager(self.definition_manager)  # noqa: SLF001
        mgr.set_enable_tracebacks(self.enable_tracebacks)
        mgr.set_output_format(self.output_format)
        mgr.set_verbose(self.verbose)
        mgr.set_experimental(self.experimental)
        mgr.set_project_path_arg(self.project_path_arg)
        mgr.set_project_env_overrides_args(self.project_env_overrides_args.copy())
        mgr.set_override_project_definition(self.override_project_definition)
        mgr.set_silent(self.silent)
        return mgr

    @property
    def connection_context(self) -> ConnectionContext:
        return self._connection_context

    def set_connection_context(self, connection_context: ConnectionContext):
        self._connection_context = connection_context

    @property
    def enable_tracebacks(self) -> bool:
        return self._enable_tracebacks

    def set_enable_tracebacks(self, value: bool):
        self._enable_tracebacks = value

    @property
    def output_format(self) -> OutputFormat:
        return self._output_format

    def set_output_format(self, value: OutputFormat):
        self._output_format = value

    @property
    def verbose(self) -> bool:
        return self._verbose

    def set_verbose(self, value: bool):
        self._verbose = value

    @property
    def experimental(self) -> bool:
        return self._experimental

    def set_experimental(self, value: bool):
        self._experimental = value

    @property
    def definition_manager(self) -> Optional[DefinitionManager]:
        return self._definition_manager

    def _set_definition_manager(self, definition_manager: Optional[DefinitionManager]):
        """
        This should only be called by the clone() method.
        """
        self._definition_manager = definition_manager

    @property
    def override_project_definition(self):
        return self._override_project_definition

    def set_override_project_definition(
        self, override_project_definition: Optional[ProjectDefinition]
    ):
        # TODO: remove; for implicit v1 <-> v2 conversion
        self._override_project_definition = override_project_definition

    @property
    def project_definition(self) -> Optional[ProjectDefinition]:
        # TODO: remove; for implicit v1 <-> v2 conversion
        if self._override_project_definition:
            return self._override_project_definition

        self._ensure_definition_manager()
        return (
            self._definition_manager.project_definition
            if self._definition_manager
            else None
        )

    @property
    def project_root(self) -> Optional[Path]:
        self._ensure_definition_manager()
        return (
            Path(self._definition_manager.project_root)
            if self._definition_manager
            else None
        )

    @property
    def template_context(self) -> dict:
        self._ensure_definition_manager()
        return (
            self._definition_manager.template_context
            if self._definition_manager
            else {}
        )

    @property
    def project_is_optional(self) -> bool:
        return self._project_is_optional

    def set_project_is_optional(self, project_is_optional: bool):
        self._project_is_optional = project_is_optional

    @property
    def project_path_arg(self) -> Optional[str]:
        return self._project_path_arg

    def set_project_path_arg(self, project_path_arg: Optional[str]):
        self._clear_definition_manager()
        self._project_path_arg = project_path_arg

    @property
    def project_env_overrides_args(self) -> dict[str, str]:
        return self._project_env_overrides_args

    def set_project_env_overrides_args(
        self, project_env_overrides_args: dict[str, str]
    ):
        # force re-calculation of DefinitionManager + dependent attrs
        self._clear_definition_manager()
        self._project_env_overrides_args = project_env_overrides_args

    @property
    def silent(self) -> bool:
        return self._silent

    def set_silent(self, value: bool):
        self._silent = value

    @property
    def connection(self) -> SnowflakeConnection:
        """
        Returns a connection for our configured context from the global active
        connection cache singleton, possibly creating a new one and caching it.
        """
        self.connection_context.validate_and_complete()
        return _CONNECTION_CACHE[self.connection_context]

    def _ensure_definition_manager(self):
        """
        (Re-)parses project definition based on project args (project_path_arg and
        project_env_overrides_args).
        """
        from snowflake.cli.api.project.definition_manager import DefinitionManager

        if self._definition_manager:
            # don't need to re-parse definition if we already have one
            return

        dm = DefinitionManager(
            self.project_path_arg,
            {CONTEXT_KEY: {"env": self.project_env_overrides_args}},
        )
        if not dm.has_definition_file and not self.project_is_optional:
            raise MissingConfiguration(
                "Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."
            )

        self._definition_manager = dm

    def _clear_definition_manager(self):
        """
        Force re-calculation of definition_manager and its dependent attributes
        (template_context, project_definition, and project_root).
        """
        self._definition_manager = None


class _CliGlobalContextAccess:
    def __init__(self, manager: _CliGlobalContextManager):
        self._manager = manager

    @property
    def connection(self) -> SnowflakeConnection:
        return self._manager.connection

    @property
    def connection_context(self) -> ConnectionContext:
        return self._manager.connection_context

    @property
    def enable_tracebacks(self) -> bool:
        return self._manager.enable_tracebacks

    @property
    def output_format(self) -> OutputFormat:
        return self._manager.output_format

    @property
    def verbose(self) -> bool:
        return self._manager.verbose

    @property
    def experimental(self) -> bool:
        return self._manager.experimental

    @property
    def project_definition(self) -> Optional[ProjectDefinition]:
        return self._manager.project_definition

    @property
    def project_root(self) -> Optional[Path]:
        return self._manager.project_root

    @property
    def template_context(self) -> dict:
        return self._manager.template_context

    @property
    def silent(self) -> bool:
        if self._should_force_mute_intermediate_output:
            return True
        return self._manager.silent

    @property
    def _should_force_mute_intermediate_output(self) -> bool:
        """Computes whether cli_console output should be muted."""
        return self._manager.output_format == OutputFormat.JSON


_CLI_CONTEXT_MANAGER: ContextVar[_CliGlobalContextManager] = ContextVar(
    "cli_context", default=_CliGlobalContextManager()
)


def get_cli_context_manager() -> _CliGlobalContextManager:
    return _CLI_CONTEXT_MANAGER.get()


def get_cli_context() -> _CliGlobalContextAccess:
    return _CliGlobalContextAccess(get_cli_context_manager())


@contextmanager
def fork_cli_context(
    connection_overrides: Optional[dict] = None,
    env: Optional[dict[str, str]] = None,
    project_is_optional: Optional[bool] = None,
    project_path: Optional[str] = None,
) -> Iterator[_CliGlobalContextAccess]:
    """
    Forks the global CLI context, making changes that are only visible
    while inside this context manager.
    """
    new_manager = _CLI_CONTEXT_MANAGER.get().clone()
    token = _CLI_CONTEXT_MANAGER.set(new_manager)

    if connection_overrides:
        new_manager.connection_context.update(**connection_overrides)

    if env:
        new_manager.project_env_overrides_args.update(env)

    if project_is_optional is not None:
        new_manager.set_project_is_optional(project_is_optional)

    if project_path:
        new_manager.set_project_path_arg(project_path)

    yield _CliGlobalContextAccess(new_manager)
    _CLI_CONTEXT_MANAGER.reset(token)
