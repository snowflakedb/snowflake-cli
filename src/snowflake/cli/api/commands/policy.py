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

from enum import Enum

import typer


class _PromptPolicy:
    def __init__(self, always_allow: bool | None):
        self._always_allow = always_allow

    def should_proceed(self, user_prompt: str = "") -> bool:
        if self._always_allow is not None:
            return self._always_allow
        return typer.confirm(user_prompt)


class PromptPolicy(Enum):
    """Policy for prompting the user to continue execution of a Snowflake CLI command."""

    ALLOW = _PromptPolicy(True)
    PROMPT = _PromptPolicy(None)
    DENY = _PromptPolicy(False)
