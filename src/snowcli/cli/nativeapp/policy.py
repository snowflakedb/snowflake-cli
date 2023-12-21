from dataclasses import dataclass

import typer


@dataclass
class TaskContext:
    user_prompt: str
    is_interactive_mode: bool


class PolicyBase:
    exit_code: int = 0

    def should_proceed(self, ctx: TaskContext):
        pass


class AllowAlwaysPolicy(PolicyBase):
    def should_proceed(self, ctx: TaskContext):
        return True


class AlwaysAskPolicy(PolicyBase):
    def should_proceed(self, ctx: TaskContext):
        if ctx.is_interactive_mode:
            should_continue = typer.confirm(ctx.user_prompt)
            return should_continue
        else:
            self.exit_code = 1
            return False
