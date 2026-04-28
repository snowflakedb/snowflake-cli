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

import json
import typing as t
from abc import ABC, abstractmethod
from enum import IntEnum

from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.connector import DictCursor
from snowflake.connector.cursor import ResultMetadata, SnowflakeCursor


class RowMapper(ABC):
    @abstractmethod
    def map_row(self, row: dict) -> dict:
        ...


class SnowflakeColumnType(IntEnum):
    """Snowflake column type codes for JSON-capable data types."""

    VARIANT = 5
    OBJECT = 9
    ARRAY = 10


class RespectingColumnTypesRowMapper(RowMapper):
    def __init__(self, cursor_description: list[ResultMetadata]):
        self._cursor_description = cursor_description

    def map_row(self, row: dict) -> dict:
        """Parse VARIANT/OBJECT/ARRAY string values into JSON when output format is JSON_EXT."""
        if get_cli_context().output_format != OutputFormat.JSON_EXT:
            return row

        column_types = [col.type_code for col in self._cursor_description]
        processed_row = {}
        for i, (column_name, value) in enumerate(row.items()):
            if i < len(column_types) and column_types[i] in (
                SnowflakeColumnType.VARIANT,
                SnowflakeColumnType.OBJECT,
                SnowflakeColumnType.ARRAY,
            ):
                if column_types[i] in (
                    SnowflakeColumnType.OBJECT,
                    SnowflakeColumnType.ARRAY,
                ) or isinstance(value, str):
                    try:
                        processed_row[column_name] = json.loads(value)
                    except (json.JSONDecodeError, TypeError):
                        processed_row[column_name] = value
                else:
                    processed_row[column_name] = value
            else:
                processed_row[column_name] = value
        return processed_row


class CommandResult:
    @property
    def result(self):
        raise NotImplementedError()


class ObjectResult(CommandResult):
    def __init__(self, element: t.Dict):
        self._element = element

    @property
    def result(self):
        return self._element


class CollectionResult(CommandResult):
    def __init__(
        self,
        elements: t.Iterable[t.Dict] | t.Generator[t.Dict, None, None],
        row_mapper: RowMapper | None = None,
    ):
        self._elements = elements
        self._row_mapper = row_mapper

    @property
    def result(self):
        if self._row_mapper:
            yield from (self._row_mapper.map_row(e) for e in self._elements)
        else:
            yield from self._elements


class MultipleResults(CommandResult):
    def __init__(self, elements: t.Iterable[CommandResult] | None = None):
        self._elements = elements or []

    def add(self, element: CommandResult):
        self._elements.append(element)  # type: ignore

    @property
    def result(self):
        return (element for element in self._elements)


class StreamResult(CommandResult):
    def __init__(self, generator: t.Generator[CommandResult, None, None]):
        self._generator = generator

    @property
    def result(self):
        return self._generator


class QueryResult(CollectionResult):
    def __init__(self, cursor: SnowflakeCursor | DictCursor):
        self.column_names = self._uniquify_column_names(
            [col.name for col in cursor.description]
        )
        super().__init__(
            elements=self._prepare_payload(cursor),
            row_mapper=RespectingColumnTypesRowMapper(cursor.description),
        )
        self._query = cursor.query

    def _prepare_payload(self, cursor: SnowflakeCursor | DictCursor):
        if isinstance(cursor, DictCursor):
            return (k for k in cursor)
        return ({k: v for k, v in zip(self.column_names, row)} for row in cursor)

    @staticmethod
    def _uniquify_column_names(column_names: list[str]) -> list[str]:
        """Disambiguate duplicate column names by appending a counter suffix."""
        unique_names: list[str] = []
        used_names: set[str] = set()
        duplicate_counters: dict[str, int] = {}

        for name in column_names:
            if name not in used_names:
                unique_names.append(name)
                used_names.add(name)
                duplicate_counters[name] = 1
            else:
                duplicate_index = duplicate_counters.get(name, 1) + 1
                candidate = f"{name}_{duplicate_index}"
                while candidate in used_names:
                    duplicate_index += 1
                    candidate = f"{name}_{duplicate_index}"
                unique_names.append(candidate)
                used_names.add(candidate)
                duplicate_counters[name] = duplicate_index

        return unique_names

    @property
    def query(self):
        return self._query


class SingleQueryResult(ObjectResult):
    def __init__(self, cursor: SnowflakeCursor):
        super().__init__(element=self._prepare_payload(cursor))

    def _prepare_payload(self, cursor):
        results = list(QueryResult(cursor).result)
        if results:
            return results[0]
        return None


class QueryJsonValueResult(QueryResult):
    def __init__(self, cursor: SnowflakeCursor):
        super().__init__(cursor)

    def _prepare_payload(self, cursor):
        results = list(QueryResult(cursor).result)
        if results:
            # Return value of the first tuple
            return json.loads(list(results[0].items())[0][1])
        return None


class MessageResult(CommandResult):
    def __init__(self, message: str):
        self._message = message

    @property
    def message(self):
        return self._message

    @property
    def result(self):
        return {"message": self._message}


class EmptyResult(CommandResult):
    @property
    def result(self):
        return None
