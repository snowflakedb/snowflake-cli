from dataclasses import dataclass
from sys import stdin, stdout
from typing import Callable, List, Optional

import typer
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


def needs_confirmation(needs_confirm: bool, auto_yes: bool) -> bool:
    return needs_confirm and not auto_yes


class UserConfirmationPolicy:
    def __init__(self, is_force, is_interactive_mode, confirm_with_user):
        self.is_force = is_force
        self.is_interactive_mode = is_interactive_mode and not self.is_force
        self.confirm_with_user = confirm_with_user

    def requires_user_confirmation(self):
        return not self.is_force


@dataclass
class Prompts:
    give_info_and_ask: str
    do_not_continue_message: str
    error_message: str


def ask_user_confirmation(
    user_confirmation_policy: UserConfirmationPolicy, prompts: Prompts
):
    if user_confirmation_policy.requires_user_confirmation():
        if user_confirmation_policy.is_interactive_mode:
            should_continue = user_confirmation_policy.confirm_with_user(
                prompts.give_info_and_ask
            )
            if not should_continue:
                print(prompts.do_not_continue_message)
                raise typer.Exit()
        else:
            print(prompts.error_message)
            raise typer.Exit(1)
