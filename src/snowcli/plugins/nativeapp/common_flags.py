import typer

InteractiveOption = typer.Option(
    False,
    "--interactive",
    "-i",
    help=f"""Defaults to unset. If specified, enables user interactions even if the standard input and output are not terminal devices.""",
    is_flag=True,
)

ForceOption = typer.Option(
    False,
    "--force",
    help=f"""Defaults to unset. If specified, allows the CLI to implicitly respond “yes” to any prompts that come up.
    This option should be passed in if you are not in interactive mode and want to perform potentially destructive actions.""",
    is_flag=True,
)
