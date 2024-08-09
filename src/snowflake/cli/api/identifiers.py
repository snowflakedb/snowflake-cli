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

import re

from click import ClickException
from snowflake.cli.api.exceptions import FQNInconsistencyError, FQNNameError
from snowflake.cli.api.project.schemas.identifier_model import (
    Identifier,
    ObjectIdentifierBaseModel,
)
from snowflake.cli.api.project.util import VALID_IDENTIFIER_REGEX, identifier_for_url


class FQN:
    """
    Class representing an identifier and supporting fully qualified names.

    The instance supports builder pattern that allows updating the identifier with database and
    schema from different sources. For example:

    fqn = FQN.from_string("my_schema.object").using_connection(conn)
    fqn = FQN.from_identifier_model(cli_context.project_definition.streamlit).using_context()
    fqn = FQN.from_string("my_name").set_database("db").set_schema("foo")
    """

    def __init__(
        self,
        database: str | None,
        schema: str | None,
        name: str,
        signature: str | None = None,
    ):
        self._database = database
        self._schema = schema
        self._name = name
        self.signature = signature

    @property
    def database(self) -> str | None:
        return self._database

    @property
    def schema(self) -> str | None:
        return self._schema

    @property
    def name(self) -> str:
        return self._name

    @property
    def prefix(self) -> str:
        if self.database:
            return f"{self.database}.{self.schema if self.schema else 'PUBLIC'}"
        if self.schema:
            return f"{self.schema}"
        return ""

    @property
    def identifier(self) -> str:
        if self.prefix:
            return f"{self.prefix}.{self.name}"
        return self.name

    @property
    def url_identifier(self) -> str:
        return ".".join(identifier_for_url(part) for part in self.identifier.split("."))

    @property
    def sql_identifier(self) -> str:
        if self.signature:
            return f"IDENTIFIER('{self.identifier}'){self.signature}"
        return f"IDENTIFIER('{self.identifier}')"

    def __str__(self):
        return self.identifier

    def __eq__(self, other):
        return self.identifier == other.identifier

    @classmethod
    def from_string(cls, identifier: str) -> "FQN":
        """
        Takes in an object name in the form [[database.]schema.]name. Returns a FQN instance.
        """
        qualifier_pattern = rf"(?:(?P<first_qualifier>{VALID_IDENTIFIER_REGEX})\.)?(?:(?P<second_qualifier>{VALID_IDENTIFIER_REGEX})\.)?(?P<name>{VALID_IDENTIFIER_REGEX})(?P<signature>\(.*\))?"
        result = re.fullmatch(qualifier_pattern, identifier)

        if result is None:
            raise FQNNameError(identifier)

        unqualified_name = result.group("name")
        if result.group("second_qualifier") is not None:
            database = result.group("first_qualifier")
            schema = result.group("second_qualifier")
        else:
            database = None
            schema = result.group("first_qualifier")

        signature = None
        if result.group("signature"):
            signature = result.group("signature")
        return cls(
            name=unqualified_name, schema=schema, database=database, signature=signature
        )

    @classmethod
    def from_stage(cls, stage: str) -> "FQN":
        name = stage
        if stage.startswith("@"):
            name = stage[1:]
        return cls.from_string(name)

    @classmethod
    def from_identifier_model_v1(cls, model: ObjectIdentifierBaseModel) -> "FQN":
        """Create an instance from object model."""
        if not isinstance(model, ObjectIdentifierBaseModel):
            raise ClickException(
                f"Expected {type(ObjectIdentifierBaseModel).__name__}, got {model}."
            )

        fqn = cls.from_string(model.name)

        if fqn.database and model.database:
            raise FQNInconsistencyError("database", model.name)
        if fqn.schema and model.schema_name:
            raise FQNInconsistencyError("schema", model.name)

        return fqn.set_database(model.database).set_schema(model.schema_name)

    @classmethod
    def from_identifier_model_v2(cls, model: Identifier) -> "FQN":
        """Create an instance from object model."""
        if not isinstance(model, Identifier):
            raise ClickException(f"Expected {type(Identifier).__name__}, got {model}.")

        fqn = cls.from_string(model.name)

        if fqn.database and model.database:
            raise FQNInconsistencyError("database", model.name)
        if fqn.schema and model.schema_:
            raise FQNInconsistencyError("schema", model.name)

        return fqn.set_database(model.database).set_schema(model.schema_)

    def set_database(self, database: str | None) -> "FQN":
        if database:
            self._database = database
        return self

    def set_schema(self, schema: str | None) -> "FQN":
        if schema:
            self._schema = schema
        return self

    def set_name(self, name: str) -> "FQN":
        self._name = name
        return self

    def using_connection(self, conn) -> "FQN":
        """Update the instance with database and schema from connection."""
        # Update the identifier only it if wasn't already a qualified name
        if conn.database and not self.database:
            self.set_database(conn.database)
        if conn.schema and not self.schema:
            self.set_schema(conn.schema)
        return self

    def using_context(self) -> "FQN":
        """Update the instance with database and schema from connection in current cli context."""
        from snowflake.cli.api.cli_global_context import get_cli_context

        return self.using_connection(get_cli_context().connection)
