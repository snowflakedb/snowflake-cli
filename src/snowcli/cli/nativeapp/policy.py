import typer


class PolicyBase:
    exit_code: int = 0

    def should_proceed(self, user_prompt: str):
        pass


class AllowAlwaysPolicy(PolicyBase):
    def should_proceed(self, user_prompt: str):
        return True


class DenyAlwaysPolicy(PolicyBase):
    def should_proceed(self, user_prompt: str):
        self.exit_code = 1
        return False


class AskAlwaysPolicy(PolicyBase):
    def should_proceed(self, user_prompt: str):
        should_continue = typer.confirm(user_prompt)
        return should_continue
