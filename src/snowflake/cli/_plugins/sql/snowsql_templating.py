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

import string


class _SnowSQLTemplate(string.Template):
    delimiter = "&"
    # Only recognise `&` as a template delimiter when it appears at the start of
    # the text or after a non-word character.  This prevents false matches
    # inside words or identifiers (e.g. `Principal&Interest` embedded in a DDL
    # COMMENT or semantic view synonym), which would otherwise be rewritten to
    # `Principal&{ Interest }` and fail Jinja rendering.  See #2714.
    pattern = r"""
        (?:^|(?<=\W))
        \&(?:
            (?P<escaped>\&)                      |   # escape sequence (&&)
            (?P<named>(?a:[_a-z][_a-z0-9]*))     |   # delimiter and a Python identifier
            {(?P<braced>(?a:[_a-z][_a-z0-9]*))}  |   # delimiter and a braced identifier
            (?P<invalid>)                            # other ill-formed delimiter exprs
        )
    """


class _Mapper:
    def __getitem__(self, item):
        return "&{ " + item + " }"


def transpile_snowsql_templates(text: str) -> str:
    return _SnowSQLTemplate(text).safe_substitute(_Mapper())  # type: ignore[arg-type]
