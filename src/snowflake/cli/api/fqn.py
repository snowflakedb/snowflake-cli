from __future__ import annotations

from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context
from snowflake.cli.api.project.schemas.identifier_model import ObjectIdentifierBaseModel


class FQNNameError(ClickException):
    def __init__(self, name: str):
        super().__init__(f"Specified name {name} is invalid.")


class FQNInconsistencyError(ClickException):
    pass


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
            if self.schema:
                return f"{self.database}.{self.schema}.{self.name}".upper()
            else:
                return f"{self.database}.PUBLIC.{self.name}".upper()
        if self.schema:
            return f"{self.schema}.{self.name}".upper()
        return self.name.upper()

    def __str__(self):
        return self.identifier

    def __eq__(self, other):
        return self.identifier == other.identifier

    @classmethod
    def from_string(cls, identifier: str) -> "FQN":
        current_parts = identifier.split(".")
        if len(current_parts) == 3:
            return cls(*current_parts)
        if len(current_parts) == 2:
            return cls(None, *current_parts)
        if len(current_parts) == 1:
            return cls(None, None, *current_parts)

        raise FQNNameError(identifier)

    @staticmethod
    def is_fqn_string_name(name: str) -> bool:
        return len(name.split(".")) == 3

    @classmethod
    def from_identifier_model(cls, model: ObjectIdentifierBaseModel) -> "FQN":
        if not isinstance(model, ObjectIdentifierBaseModel):
            raise ClickException(
                f"Expected {type(ObjectIdentifierBaseModel)}, got {model}."
            )

        fqn = cls.from_string(model.name)

        if FQN.is_fqn_string_name(model.name) and (model.database or model.schema_name):
            raise ClickException(
                f"Database or schema provided but name {model.name} is fully qualified name."
            )

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

    def using_context(self):
        # Update the identifier only it if wasn't already a qualified name
        conn = cli_context.connection
        if conn.database and not self.database:
            self.set_database(conn.database)
        if conn.schema and not self.schema:
            self.set_schema(conn.schema)
        return self
