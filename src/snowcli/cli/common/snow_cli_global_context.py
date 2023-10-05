from copy import deepcopy
from dataclasses import dataclass
import functools
from typing import Callable, Optional, Union
from snowcli.cli.project.definition_manager import DefinitionManager
from snowcli.cli.common.utils import connection_to_definition_mapping

from snowflake.connector import SnowflakeConnection

from snowcli.output.formats import OutputFormat
from snowcli.snow_connector import connect_to_snowflake


@dataclass
class ProjectDefinitionDetails:
    project_path: Optional[str] = None
    environment_override: Optional[str] = None
    definition_manager: DefinitionManager = None

    def get_definition_manager(self):
        from snowcli.cli.common.decorators import PROJECT_DEFINITION_OPTIONS

        for option in PROJECT_DEFINITION_OPTIONS:
            override = option.name
            if override == "project":
                self.project_path = getattr(self, override)
            else:
                self.environment_override = getattr(self, override)

        self.definition_manager = DefinitionManager(
            self.project_path, self.environment_override
        )
        return self.definition_manager

    @staticmethod
    def _project_definition_update(param_name: str, value: str):
        def modifications(context: SnowCliGlobalContext) -> SnowCliGlobalContext:
            setattr(context.project_definition_details, param_name, value)
            # In addition to setting the attribute, do I need to call self.load_definition here?
            return context

        snow_cli_global_context_manager.update_global_context_for_project_definition(
            modifications
        )
        return value

    @staticmethod
    def update_callback(param_name: str):
        return lambda value: ProjectDefinitionDetails._project_definition_update(
            param_name=param_name, value=value
        )


@dataclass
class ConnectionDetails:
    connection_name: Optional[str] = None
    account: Optional[str] = None
    database: Optional[str] = None
    role: Optional[str] = None
    schema: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    warehouse: Optional[str] = None
    temporary_connection: bool = False

    def _resolve_connection_params(self, project_definition: Optional[dict]):
        from snowcli.cli.common.decorators import GLOBAL_CONNECTION_OPTIONS

        params = {}

        for option in GLOBAL_CONNECTION_OPTIONS:
            override = option.name
            if override == "connection" or override == "temporary_connection":
                continue

            override_value = getattr(self, override)
            if override_value is not None:
                params[override] = override_value
            elif project_definition:
                if (
                    override in connection_to_definition_mapping
                    and connection_to_definition_mapping[override] in project_definition
                ):
                    params[override] = project_definition[
                        connection_to_definition_mapping[override]
                    ]
        return params

    def build_connection(self, project_definition: Optional[dict]):
        return connect_to_snowflake(
            temporary_connection=self.temporary_connection,
            connection_name=self.connection_name,
            **self._resolve_connection_params(project_definition)
        )

    @staticmethod
    def _connection_update(param_name: str, value: str):
        def modifications(context: SnowCliGlobalContext) -> SnowCliGlobalContext:
            setattr(context.connection, param_name, value)
            return context

        snow_cli_global_context_manager.update_global_context(modifications)
        return value

    @staticmethod
    def update_callback(param_name: str):  # This is just for setting attribute values
        return lambda value: ConnectionDetails._connection_update(
            param_name=param_name, value=value
        )


@dataclass
class SnowCliGlobalContext:
    """
    Global state accessible in whole CLI code.
    """

    enable_tracebacks: bool
    connection: ConnectionDetails
    project_definition_details: ProjectDefinitionDetails
    output_format: OutputFormat
    verbose: bool


class SnowCliGlobalContextManager:
    """
    A manager responsible for retrieving and updating global state.
    """

    _cached_connector: Optional[SnowflakeConnection]
    _cached_definition_manager: Optional[DefinitionManager] = None
    _definition_schema_name: Optional[str] = None

    def __init__(self, global_context_with_default_values: SnowCliGlobalContext):
        self._global_context = deepcopy(global_context_with_default_values)
        self._cached_connector = None

    def get_global_context_copy(self) -> SnowCliGlobalContext:
        """
        Returns deep copy of global state.
        """
        return deepcopy(self._global_context)

    def update_global_context_for_project_definition(
        self, update: Callable[[SnowCliGlobalContext], SnowCliGlobalContext]
    ) -> None:
        """
        Updates global state using provided function.
        The resulting object will be deep copied before storing in the manager.
        """
        self._global_context = deepcopy(update(self.get_global_context_copy()))
        self._cached_definition_manager = None

    def update_global_context(
        self, update: Callable[[SnowCliGlobalContext], SnowCliGlobalContext]
    ) -> None:
        """
        Updates global state using provided function.
        The resulting object will be deep copied before storing in the manager.
        """
        self._global_context = deepcopy(update(self.get_global_context_copy()))
        self._cached_connector = None

    def load_definition_manager(
        self, schema: Optional[str], force_rebuild=False
    ) -> DefinitionManager:
        self._definition_schema_name = schema
        if force_rebuild or not self._cached_definition_manager:
            self._cached_definition_manager = (
                self.get_global_context_copy().project_definition_details.get_definition_manager()
            )
        return self._cached_definition_manager

    @functools.cached_property
    def get_definition_manager(self) -> DefinitionManager:
        return self._cached_definition_manager

    def get_connection(self, force_rebuild=False) -> SnowflakeConnection:
        """
        Returns a SnowflakeConnection, representing an open connection to Snowflake
        given the context in this manager. This connection is shared with subsequent
        calls to this method until updates are made or force_rebuild is True, in which
        case a new connector will be returned.
        """
        schema_definition = {}
        # self.get_definition_manager is not None at this point because self.load_definition_manager should have already been called from the decorator
        schema_definition = self.get_definition_manager.project_definition.get(
            self._definition_schema_name, None
        )

        if force_rebuild or not self._cached_connector:
            self._cached_connector = (
                self.get_global_context_copy().connection.build_connection(
                    schema_definition
                )
            )
        return self._cached_connector


def _create_snow_cli_global_context_manager_with_default_values() -> (
    SnowCliGlobalContextManager
):
    """
    Creates a manager with global state filled with default values.
    """
    return SnowCliGlobalContextManager(
        SnowCliGlobalContext(
            enable_tracebacks=True,
            connection=ConnectionDetails(),
            project_definition_details=ProjectDefinitionDetails(),
            output_format=OutputFormat.TABLE,
            verbose=False,
        )
    )


def setup_global_context(param_name: str, value: Union[bool, str]):
    """
    Setup global state (accessible in whole CLI code) using options passed in SNOW CLI invocation.
    """

    def modifications(context: SnowCliGlobalContext) -> SnowCliGlobalContext:
        setattr(context, param_name, value)
        return context

    snow_cli_global_context_manager.update_global_context(modifications)


def update_callback(param_name: str):
    return lambda value: setup_global_context(param_name=param_name, value=value)


snow_cli_global_context_manager = (
    _create_snow_cli_global_context_manager_with_default_values()
)
