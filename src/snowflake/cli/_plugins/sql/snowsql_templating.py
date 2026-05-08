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

# The default string.Template pattern matches `&name` anywhere in the input,
# so an ampersand embedded inside an identifier or string literal (for example
# `Principal&Interest` in a semantic view synonym or COMMENT) is treated as a
# variable reference. This pattern requires the bare `&name` form to appear at
# a token boundary — start of input, or after a non-identifier character — so
# that a `&` wedged inside a word is left alone. The braced `&{name}` form
# stays explicit and works anywhere; `&&` is still the literal-ampersand escape.
_SNOWSQL_PATTERN = (
    r"\&(?P<escaped>\&)"
    r"|(?<![A-Za-z0-9_])\&(?P<named>[_a-z][_a-z0-9]*)"
    r"|\&\{(?P<braced>[_a-z][_a-z0-9]*)\}"
    r"|(?<![A-Za-z0-9_])\&(?P<invalid>)"
)


class _SnowSQLTemplate(string.Template):
    delimiter = "&"
    pattern = _SNOWSQL_PATTERN  # type: ignore[assignment]


class _Mapper:
    def __getitem__(self, item):
        return "&{ " + item + " }"


def transpile_snowsql_templates(text: str) -> str:
    return _SnowSQLTemplate(text).safe_substitute(_Mapper())  # type: ignore[arg-type]
