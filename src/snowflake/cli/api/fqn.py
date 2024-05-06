from __future__ import annotations

import re

from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.exceptions import FQNInconsistencyError, FQNNameError
from snowflake.cli.api.project.schemas.identifier_model import ObjectIdentifierBaseModel
from snowflake.cli.api.project.util import VALID_IDENTIFIER_REGEX, unquote_identifier


class FQN:
    def __init__(self, database: str | None, schema: str | None, name: str | None):
        self._database = database
        self._schema = schema
        self._name = name

    @property
    def database(self):
        return self._database

    @property
    def schema(self):
        return self._schema

    @property
    def name(self):
        return self._name

    @property
    def identifier(self):
        if self.database:
            return f"{self.database}.{self.schema if self.schema else 'PUBLIC'}.{self.name}".upper()
        if self.schema:
            return f"{self.schema}.{self.name}".upper()
        return self.name.upper()

    @property
    def url_identifier(self):
        return ".".join(unquote_identifier(part) for part in self.identifier.split("."))

    def __str__(self):
        return self.identifier

    def __eq__(self, other):
        return self.identifier == other.identifier

    @classmethod
    def from_string(cls, identifier: str) -> "FQN":
        """
        Takes in an object name in the form [[database.]schema.]name. Returns a FQN instance.
        """
        # TODO: Use regex to match object name to a valid identifier or
        #  valid identifier (args). Second case is for sprocs and UDFs
        qualifier_pattern = rf"(?:(?P<first_qualifier>{VALID_IDENTIFIER_REGEX})\.)?(?:(?P<second_qualifier>{VALID_IDENTIFIER_REGEX})\.)?(?P<name>.*)"
        result = re.fullmatch(qualifier_pattern, identifier)

        if result is None:
            raise FQNNameError(f"'{identifier}' is not a valid qualified name")

        unqualified_name = result.group("name")
        if result.group("second_qualifier") is not None:
            database = result.group("first_qualifier")
            schema = result.group("second_qualifier")
        else:
            database = None
            schema = result.group("first_qualifier")
        return cls(name=unqualified_name, schema=schema, database=database)

    @classmethod
    def from_identifier_model(cls, model: ObjectIdentifierBaseModel) -> "FQN":
        if not isinstance(model, ObjectIdentifierBaseModel):
            raise ClickException(
                f"Expected {type(ObjectIdentifierBaseModel)}, got {model}."
            )

        fqn = cls.from_string(model.name)

        if fqn.database and model.database:
            raise FQNInconsistencyError("database", model.name)
        if fqn.schema and model.schema_name:
            raise FQNInconsistencyError("schema", model.name)

        return fqn.set_database(model.database).set_schema(model.schema_name)

    def set_database(self, database: str | None):
        if database:
            self._database = database
        return self

    def set_schema(self, schema: str | None):
        if schema:
            self._schema = schema
        return self

    def set_name(self, name: str):
        self._name = name
        return self

    def using_connection(self, conn):
        # Update the identifier only it if wasn't already a qualified name
        if conn.database and not self.database:
            self.set_database(conn.database)
        if conn.schema and not self.schema:
            self.set_schema(conn.schema)
        return self

    def using_context(self):
        return self.using_connection(cli_context.connection)
