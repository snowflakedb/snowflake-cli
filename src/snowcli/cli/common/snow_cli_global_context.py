from copy import deepcopy
from dataclasses import dataclass
from typing import Callable, Union

import click
import typer

from snowcli.cli.common.flags import FormatOption, Verbose
from snowcli.output.formats import OutputFormat


@dataclass
class SnowCliGlobalContext:
    """
    Global state accessible in whole CLI code.
    """

    enable_tracebacks: bool = True
    output_format: str = OutputFormat.TABLE.value


class SnowCliGlobalContextManager:
    """
    A manager responsible for retrieving and updating global state.
    """

    def __init__(self, global_context_with_default_values: SnowCliGlobalContext):
        self._global_context = deepcopy(global_context_with_default_values)

    def get_global_context_copy(self) -> SnowCliGlobalContext:
        """
        Returns deep copy of global state.
        """
        return deepcopy(self._global_context)

    def update_global_context(
        self, update: Callable[[SnowCliGlobalContext], SnowCliGlobalContext]
    ) -> None:
        """
        Updates global state using provided function.
        The resulting object will be deep copied before storing in the manager.
        """
        self._global_context = deepcopy(update(self.get_global_context_copy()))


snow_cli_global_context_manager = SnowCliGlobalContextManager(SnowCliGlobalContext())


def convert_to_click_option(typer_option: typer.models.OptionInfo):
    opt = click.Option(
        param_decls=typer_option.param_decls,
        is_flag=typer_option.is_flag,
        flag_value=typer_option.flag_value,
    )
    return opt


# context src: flag
GLOBAL_FLAGS = {"output_format": FormatOption, "verbose": Verbose}


def _parse_global_flags(click_context: click.Context):
    # Now we do not expect anything additional, so we set both to False.
    # Thanks to that re-parsing the context will raise errors
    click_context.allow_extra_args = False
    click_context.ignore_unknown_options = False

    parser = click.parser.OptionParser(click_context)
    for option_name, option in GLOBAL_FLAGS.items():
        opt = convert_to_click_option(option)
        parser.add_option(opt, opt.opts, option_name)

    print(click_context.args)
    known_options, _, _ = parser.parse_args(click_context.args)
    return known_options


def process_context():
    click_ctx: click.Context = click.get_current_context()
    known_options = _parse_global_flags(click_context=click_ctx)

    def modifications(context: SnowCliGlobalContext) -> SnowCliGlobalContext:
        for option in GLOBAL_FLAGS:
            if option in known_options:
                setattr(context, option, known_options[option])
        return context

    snow_cli_global_context_manager.update_global_context(modifications)
