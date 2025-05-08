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
from dataclasses import dataclass, field, replace
from functools import wraps
from pathlib import Path
from typing import TYPE_CHECKING, Iterator, Optional

from snowflake.cli.api.connections import ConnectionContext, OpenConnectionCache
from snowflake.cli.api.exceptions import MissingConfigurationError
from snowflake.cli.api.metrics import CLIMetrics
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.rendering.jinja import CONTEXT_KEY
from snowflake.connector import SnowflakeConnection

if TYPE_CHECKING:
    from snowflake.cli.api.project.definition_manager import DefinitionManager
    from snowflake.cli.api.project.schemas.project_definition import ProjectDefinition
    from snowflake.core import Root

_CONNECTION_CACHE = OpenConnectionCache()


@dataclass
class _CliGlobalContextManager:
    connection_context: ConnectionContext = field(default_factory=ConnectionContext)
    connection_cache: OpenConnectionCache = (
        _CONNECTION_CACHE  # by default, use global cache
    )

    output_format: OutputFormat = OutputFormat.TABLE
    silent: bool = False
    verbose: bool = False
    experimental: bool = False
    enable_tracebacks: bool = True
    is_repl: bool = False

    metrics: CLIMetrics = field(default_factory=CLIMetrics)

    project_path_arg: str | None = None
    project_is_optional: bool = True
    project_env_overrides_args: dict[str, str] = field(default_factory=dict)

    # FIXME: this property only exists to help implement
    # nativeapp_definition_v2_to_v1 and single_app_and_package.
    # Consider changing the way this calculation is provided to commands
    # in order to remove this logic (then make project_definition a non-cloned @property)
    override_project_definition: ProjectDefinition | None = None

    _definition_manager: DefinitionManager | None = None
    enhanced_exit_codes: bool = False

    # which properties invalidate our current DefinitionManager?
    DEFINITION_MANAGER_DEPENDENCIES = [
        "project_path_arg",
        "project_is_optional",
        "project_env_overrides_args",
    ]

    def reset(self):
        self.__init__()

    def clone(self) -> _CliGlobalContextManager:
        return replace(
            self,
            connection_context=self.connection_context.clone(),
            project_env_overrides_args=self.project_env_overrides_args.copy(),
            metrics=self.metrics.clone(),
        )

    def __setattr__(self, prop, val):
        if prop in self.DEFINITION_MANAGER_DEPENDENCIES:
            self._clear_definition_manager()

        super().__setattr__(prop, val)

    @property
    def project_definition(self) -> ProjectDefinition | None:
        if self.override_project_definition:
            return self.override_project_definition

        return self._definition_manager_or_raise().project_definition

    @property
    def project_root(self) -> Path:
        return Path(self._definition_manager_or_raise().project_root)

    @property
    def template_context(self) -> dict:
        return self._definition_manager_or_raise().template_context

    @property
    def connection(self) -> SnowflakeConnection:
        """
        Returns a connection for our configured context from the configured cache.
        By default, this is the global _CONNECTION_CACHE. If a matching connection
        does not already exist, creates a new connection and caches it.
        """
        self.connection_context.validate_and_complete()
        return self.connection_cache[self.connection_context]

    def _definition_manager_or_raise(self) -> DefinitionManager:
        """
        (Re-)parses project definition based on project args (project_path_arg and
        project_env_overrides_args). If we cannot provide a project definition
        (i.e. no snowflake.yml) and require one, raises MissingConfiguration.
        """
        from snowflake.cli.api.project.definition_manager import DefinitionManager

        # don't need to re-parse definition if we already have one
        if not self._definition_manager:
            dm = DefinitionManager(
                self.project_path_arg,
                {CONTEXT_KEY: {"env": self.project_env_overrides_args}},
            )
            if not dm.has_definition_file and not self.project_is_optional:
                raise MissingConfigurationError(
                    "Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."
                )
            self._definition_manager = dm

        return self._definition_manager

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
    def metrics(self) -> CLIMetrics:
        return self._manager.metrics

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
    def project_definition(self) -> ProjectDefinition | None:
        return self._manager.project_definition

    @property
    def project_root(self) -> Path | None:
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

    @property
    def snow_api_root(
        self,
    ) -> Optional[Root]:
        from snowflake.core import Root

        if self.connection:
            return Root(self.connection)
        else:
            return None

    @property
    def enhanced_exit_codes(self) -> bool:
        return self._manager.enhanced_exit_codes

    @property
    def is_repl(self) -> bool:
        return self._manager.is_repl


_CLI_CONTEXT_MANAGER: ContextVar[_CliGlobalContextManager | None] = ContextVar(
    "cli_context", default=None
)


def get_cli_context_manager() -> _CliGlobalContextManager:
    mgr = _CLI_CONTEXT_MANAGER.get()
    if not mgr:
        mgr = _CliGlobalContextManager()
        _CLI_CONTEXT_MANAGER.set(mgr)
    return mgr


def get_cli_context() -> _CliGlobalContextAccess:
    return _CliGlobalContextAccess(get_cli_context_manager())


def span(span_name: str):
    """
    Decorator to start a command metrics span that encompasses a whole function

    Must be used instead of directly calling @get_cli_context().metrics.span(span_name)
    as a decorator to ensure that the cli context is grabbed at run time instead of at
    module load time, which would not reflect forking
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            with get_cli_context().metrics.span(span_name):
                return func(*args, **kwargs)

        return wrapper

    return decorator


@contextmanager
def fork_cli_context(
    connection_overrides: dict | None = None,
    project_env_overrides: dict[str, str] | None = None,
    project_is_optional: bool | None = None,
    project_path: str | None = None,
) -> Iterator[_CliGlobalContextAccess]:
    """
    Forks the global CLI context, making changes that are only visible
    (e.g. via get_cli_context()) while inside this context manager.

    Please note that environment variable changes are only visible through
    the project definition; os.getenv / os.environ / get_env_value are not
    affected by these new values.
    """
    old_manager = get_cli_context_manager()
    new_manager = old_manager.clone()
    token = _CLI_CONTEXT_MANAGER.set(new_manager)

    if connection_overrides:
        new_manager.connection_context.update(**connection_overrides)

    if project_env_overrides:
        new_manager.project_env_overrides_args.update(project_env_overrides)

    if project_is_optional is not None:
        new_manager.project_is_optional = project_is_optional

    if project_path:
        new_manager.project_path_arg = project_path

    yield _CliGlobalContextAccess(new_manager)
    _CLI_CONTEXT_MANAGER.reset(token)
