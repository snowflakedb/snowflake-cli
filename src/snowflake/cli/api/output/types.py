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

from snowflake.connector import DictCursor
from snowflake.connector.cursor import SnowflakeCursor


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
    def __init__(self, elements: t.Iterable[t.Dict]):
        self._elements = elements

    @property
    def result(self):
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
        self.column_names = [col.name for col in cursor.description]
        super().__init__(elements=self._prepare_payload(cursor))
        self._query = cursor.query

    def _prepare_payload(self, cursor: SnowflakeCursor | DictCursor):
        if isinstance(cursor, DictCursor):
            return (k for k in cursor)
        return ({k: v for k, v in zip(self.column_names, row)} for row in cursor)

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
