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

import re

# Matches a SnowSQL `&name` variable reference only when the `&` starts a new
# token, i.e. at the start of the string or preceded by a non-word character.
# An `&` embedded inside a word (for example `Principal&Interest` in a comment
# or string literal) is literal text, not a variable reference (see GH#2714).
_UNESCAPED_AMPERSAND_VAR = re.compile(r"(?<!\w)&([A-Za-z_][A-Za-z0-9_]*)")
# Matches the explicit braced form `&{name}` so it is normalized the same way
# `string.Template` with delimiter `&` used to normalize it.
_BRACED_AMPERSAND_VAR = re.compile(r"&\{([A-Za-z_][A-Za-z0-9_]*)\}")

# Placeholder shielding `&&` escapes while `&name` references are transpiled.
_ESCAPED_AMPERSAND = "\0snowflake-cli-escaped-ampersand\0"


def transpile_snowsql_templates(text: str) -> str:
    text = text.replace("&&", _ESCAPED_AMPERSAND)
    text = _UNESCAPED_AMPERSAND_VAR.sub(r"&{ \1 }", text)
    text = _BRACED_AMPERSAND_VAR.sub(r"&{ \1 }", text)
    return text.replace(_ESCAPED_AMPERSAND, "&")
