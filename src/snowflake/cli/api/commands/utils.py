from typing import List, Optional

from click import ClickException
from snowflake.cli.api.commands.common import Variable


def parse_key_value_variables(variables: Optional[List[str]]) -> List[Variable]:
    """Util for parsing key=value input. Useful for commands accepting multiple input options."""
    result: List[Variable] = []
    if not variables:
        return result
    for p in variables:
        if "=" not in p:
            raise ClickException(f"Invalid variable: '{p}'")

        key, value = p.split("=", 1)
        result.append(Variable(key.strip(), value.strip()))
    return result
