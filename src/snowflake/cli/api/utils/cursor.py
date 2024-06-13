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

from typing import Callable, List, Optional

from snowflake.connector.cursor import DictCursor


def _rows_generator(cursor: DictCursor, predicate: Callable[[dict], bool]):
    return (row for row in cursor.fetchall() if predicate(row))


def find_all_rows(cursor: DictCursor, predicate: Callable[[dict], bool]) -> List[dict]:
    return list(_rows_generator(cursor, predicate))


def find_first_row(
    cursor: DictCursor, predicate: Callable[[dict], bool]
) -> Optional[dict]:
    """Returns the first row that matches the predicate, or None."""
    return next(_rows_generator(cursor, predicate), None)
