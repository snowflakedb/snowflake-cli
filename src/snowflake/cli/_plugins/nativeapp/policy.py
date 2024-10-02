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

from abc import ABC, abstractmethod
from typing import Optional

import typer


class PolicyBase(ABC):
    """Abstract Class for various policies that govern if a Snowflake CLI command can continue execution when it asks for a decision."""

    @abstractmethod
    def should_proceed(self, user_prompt: Optional[str]) -> bool:
        pass


class AllowAlwaysPolicy(PolicyBase):
    """Always allow a Snowflake CLI command to continue execution."""

    def should_proceed(self, user_prompt: Optional[str] = None):
        return True


class DenyAlwaysPolicy(PolicyBase):
    """Never allow a Snowflake CLI command to continue execution."""

    def should_proceed(self, user_prompt: Optional[str] = None):
        return False


class AskAlwaysPolicy(PolicyBase):
    """Ask the user whether to continue execution of a Snowflake CLI command."""

    def should_proceed(self, user_prompt: Optional[str]):
        should_continue = typer.confirm(user_prompt)
        return should_continue
