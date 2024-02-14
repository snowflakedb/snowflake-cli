from sys import stdin, stdout


def needs_confirmation(needs_confirm: bool, auto_yes: bool) -> bool:
    return needs_confirm and not auto_yes


def is_tty_interactive():
    return stdin.isatty() and stdout.isatty()
