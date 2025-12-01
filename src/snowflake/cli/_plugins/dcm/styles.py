from rich.style import Style

# Plan/Deploy
CREATE_STYLE = Style(color="green")
ALTER_STYLE = Style(color="yellow")
DROP_STYLE = Style(color="red")

PASS_STYLE = Style(color="green")
FAIL_STYLE = Style(color="red")
ERROR_STYLE = Style(color="red")
OK_STYLE = Style(color="green")
INFO_STYLE = Style(bold=False)
DOMAIN_STYLE = Style(color="cyan")
BOLD_STYLE = Style(bold=True)

# Refresh
STATUS_STYLE = Style(color="blue")
REMOVED_STYLE = Style(color="red", italic=True)
INSERTED_STYLE = Style(color="green", italic=True)
