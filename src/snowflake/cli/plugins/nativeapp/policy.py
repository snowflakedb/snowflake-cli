from abc import ABC, abstractmethod
from typing import Optional

import typer


class PolicyBase(ABC):
    """Abtract Class for various policies that govern if a Snowflake CLI command can continue execution when it asks for a decision."""

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
