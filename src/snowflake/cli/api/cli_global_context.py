import re
from pathlib import Path
from typing import Dict, Optional

from snowflake.cli.api.exceptions import InvalidSchemaError
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.connector import SnowflakeConnection


class _ConnectionContext:
    def __init__(self):
        self._cached_connection: Optional[SnowflakeConnection] = None

        self._connection_name: Optional[str] = None
        self._account: Optional[str] = None
        self._database: Optional[str] = None
        self._role: Optional[str] = None
        self._schema: Optional[str] = None
        self._user: Optional[str] = None
        self._password: Optional[str] = None
        self._authenticator: Optional[str] = None
        self._private_key_path: Optional[str] = None
        self._warehouse: Optional[str] = None
        self._temporary_connection: bool = False

    def __setattr__(self, key, value):
        """
        We invalidate connection cache every time connection attributes change.
        """
        super.__setattr__(self, key, value)
        if key != "_cached_connection":
            self._cached_connection = None

    @property
    def connection_name(self) -> Optional[str]:
        return self._connection_name

    def set_connection_name(self, value: Optional[str]):
        self._connection_name = value

    @property
    def account(self) -> Optional[str]:
        return self._account

    def set_account(self, value: Optional[str]):
        self._account = value

    @property
    def database(self) -> Optional[str]:
        return self._database

    def set_database(self, value: Optional[str]):
        self._database = value

    @property
    def role(self) -> Optional[str]:
        return self._role

    def set_role(self, value: Optional[str]):
        self._role = value

    @property
    def schema(self) -> Optional[str]:
        return self._schema

    def set_schema(self, value: Optional[str]):
        if (
            value
            and not (value.startswith('"') and value.endswith('"'))
            and re.compile(".+\..+").match(
                value
            )  # if schema is fully qualified name (db.schema)
        ):
            raise InvalidSchemaError(value)
        self._schema = value

    @property
    def user(self) -> Optional[str]:
        return self._user

    def set_user(self, value: Optional[str]):
        self._user = value

    @property
    def password(self) -> Optional[str]:
        return self._password

    def set_password(self, value: Optional[str]):
        self._password = value

    @property
    def authenticator(self) -> Optional[str]:
        return self._authenticator

    def set_authenticator(self, value: Optional[str]):
        self._authenticator = value

    @property
    def private_key_path(self) -> Optional[str]:
        return self._private_key_path

    def set_private_key_path(self, value: Optional[str]):
        self._private_key_path = value

    @property
    def warehouse(self) -> Optional[str]:
        return self._warehouse

    def set_warehouse(self, value: Optional[str]):
        self._warehouse = value

    @property
    def temporary_connection(self) -> bool:
        return self._temporary_connection

    def set_temporary_connection(self, value: bool):
        self._temporary_connection = value

    @property
    def connection(self) -> SnowflakeConnection:
        if not self._cached_connection:
            self._cached_connection = self._build_connection()
        return self._cached_connection

    def _collect_not_empty_connection_attributes(self):
        all_attributes = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "authenticator": self.authenticator,
            "private_key_path": self.private_key_path,
            "database": self.database,
            "schema": self.schema,
            "role": self.role,
            "warehouse": self.warehouse,
        }
        not_empty_attributes = {
            k: v for (k, v) in all_attributes.items() if v is not None
        }
        return not_empty_attributes

    def _build_connection(self):
        from snowflake.cli.app.snow_connector import connect_to_snowflake

        return connect_to_snowflake(
            temporary_connection=self.temporary_connection,
            connection_name=self.connection_name,
            **self._collect_not_empty_connection_attributes(),
        )


class _CliGlobalContextManager:
    def __init__(self):
        self._connection_context = _ConnectionContext()
        self._enable_tracebacks = True
        self._output_format = OutputFormat.TABLE
        self._verbose = False
        self._experimental = False
        self._project_definition = None
        self._project_root = None
        self._silent: bool = False

    def reset(self):
        self.__init__()

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
    def project_definition(self) -> Optional[Dict]:
        return self._project_definition

    def set_project_definition(self, value: Dict):
        self._project_definition = value

    @property
    def project_root(self):
        return self._project_root

    def set_project_root(self, project_root: Path):
        self._project_root = project_root

    @property
    def connection_context(self) -> _ConnectionContext:
        return self._connection_context

    @property
    def connection(self) -> SnowflakeConnection:
        return self.connection_context.connection

    @property
    def silent(self) -> bool:
        return self._silent

    def set_silent(self, value: bool):
        self._silent = value


class _CliGlobalContextAccess:
    def __init__(self, manager: _CliGlobalContextManager):
        self._manager = manager

    @property
    def connection(self) -> SnowflakeConnection:
        return self._manager.connection

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
    def project_definition(self):
        return self._manager.project_definition

    @property
    def project_root(self):
        return self._manager.project_root

    @property
    def silent(self) -> bool:
        return self._manager.silent


cli_context_manager: _CliGlobalContextManager = _CliGlobalContextManager()
cli_context: _CliGlobalContextAccess = _CliGlobalContextAccess(cli_context_manager)
