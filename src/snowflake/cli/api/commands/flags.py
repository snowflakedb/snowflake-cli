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

import tempfile
from dataclasses import dataclass
from enum import Enum
from inspect import signature
from pathlib import Path
from typing import Any, Callable, List, Optional, Tuple

import click
import typer
from click import ClickException
from snowflake.cli.api.cli_global_context import cli_context_manager
from snowflake.cli.api.commands.typer_pre_execute import register_pre_execute_command
from snowflake.cli.api.console import cli_console
from snowflake.cli.api.exceptions import MissingConfiguration
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.project.definition_manager import DefinitionManager
from snowflake.cli.api.rendering.jinja import CONTEXT_KEY

DEFAULT_CONTEXT_SETTINGS = {"help_option_names": ["--help", "-h"]}

_CONNECTION_SECTION = "Connection configuration"
_CLI_BEHAVIOUR = "Global configuration"


class OnErrorType(Enum):
    BREAK = "break"
    CONTINUE = "continue"


class OverrideableOption:
    """
    Class that allows you to generate instances of typer.models.OptionInfo with some default properties while allowing
    specific values to be overridden.

    Custom parameters:
    - mutually_exclusive (Tuple[str]|List[str]): A list of parameter names that this Option is not compatible with. If this Option has
     a truthy value and any of the other parameters in the mutually_exclusive list has a truthy value, a
     ClickException will be thrown. Note that mutually_exclusive can contain an option's own name but does not require
     it.
    """

    def __init__(
        self,
        default: Any,
        *param_decls: str,
        mutually_exclusive: Optional[List[str] | Tuple[str]] = None,
        **kwargs,
    ):
        self.default = default
        self.param_decls = param_decls
        self.mutually_exclusive = mutually_exclusive
        self.kwargs = kwargs

    def __call__(self, **kwargs) -> typer.models.OptionInfo:
        """
        Returns a typer.models.OptionInfo instance initialized with the specified default values along with any overrides
        from kwargs. Note that if you are overriding param_decls, you must pass an iterable of strings, you cannot use
        positional arguments like you can with typer.Option. Does not modify the original instance.
        """
        default = kwargs.get("default", self.default)
        param_decls = kwargs.get("param_decls", self.param_decls)
        mutually_exclusive = kwargs.get("mutually_exclusive", self.mutually_exclusive)
        if not isinstance(param_decls, list) and not isinstance(param_decls, tuple):
            raise TypeError("param_decls must be a list or tuple")
        passed_kwargs = self.kwargs.copy()
        passed_kwargs.update(kwargs)
        if passed_kwargs.get("callback", None) or mutually_exclusive:
            passed_kwargs["callback"] = self._callback_factory(
                passed_kwargs.get("callback", None), mutually_exclusive
            )
        for non_kwarg in ["default", "param_decls", "mutually_exclusive"]:
            passed_kwargs.pop(non_kwarg, None)
        return typer.Option(default, *param_decls, **passed_kwargs)

    class InvalidCallbackSignature(ClickException):
        def __init__(self, callback):
            super().__init__(
                f"Signature {signature(callback)} is not valid for an OverrideableOption callback function. Must have at most one parameter with each of the following types: (typer.Context, typer.CallbackParam, Any Other Type)"
            )

    def _callback_factory(
        self, callback, mutually_exclusive: Optional[List[str] | Tuple[str]]
    ):
        callback = callback if callback else lambda x: x

        # inspect existing_callback to make sure signature is valid
        existing_params = signature(callback).parameters
        # at most one parameter with each type in [typer.Context, typer.CallbackParam, any other type]
        limits = [
            lambda x: x == typer.Context,
            lambda x: x == typer.CallbackParam,
            lambda x: x != typer.Context and x != typer.CallbackParam,
        ]
        for limit in limits:
            if len([v for v in existing_params.values() if limit(v.annotation)]) > 1:
                raise self.InvalidCallbackSignature(callback)

        def generated_callback(ctx: typer.Context, param: typer.CallbackParam, value):
            if mutually_exclusive:
                for name in mutually_exclusive:
                    if value and ctx.params.get(
                        name, False
                    ):  # if the current parameter is set to True and a previous parameter is also Truthy
                        curr_opt = param.opts[0]
                        other_opt = [x for x in ctx.command.params if x.name == name][
                            0
                        ].opts[0]
                        raise click.ClickException(
                            f"Options '{curr_opt}' and '{other_opt}' are incompatible."
                        )

            # pass args to existing callback based on its signature (this is how Typer infers callback args)
            passed_params = {}
            for existing_param in existing_params:
                annotation = existing_params[existing_param].annotation
                if annotation == typer.Context:
                    passed_params[existing_param] = ctx
                elif annotation == typer.CallbackParam:
                    passed_params[existing_param] = param
                else:
                    passed_params[existing_param] = value
            return callback(**passed_params)

        return generated_callback


from snowflake.cli.api.config import get_all_connections


def _callback(provide_setter: Callable[[], Callable[[Any], Any]]):
    def callback(value):
        set_value = provide_setter()
        set_value(value)
        return value

    return callback


ConnectionOption = typer.Option(
    None,
    "--connection",
    "-c",
    "--environment",
    help=f"Name of the connection, as defined in your `config.toml`. Default: `default`.",
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_connection_name
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    autocompletion=lambda: list(get_all_connections()),
)

TemporaryConnectionOption = typer.Option(
    False,
    "--temporary-connection",
    "-x",
    help="Uses connection defined with command line parameters, instead of one defined in config",
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_temporary_connection
    ),
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

AccountOption = typer.Option(
    None,
    "--account",
    "--accountname",
    help="Name assigned to your Snowflake account. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_account),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

UserOption = typer.Option(
    None,
    "--user",
    "--username",
    help="Username to connect to Snowflake. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_user),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)


PLAIN_PASSWORD_MSG = "WARNING! Using --password via the CLI is insecure. Use environment variables instead."


def _password_callback(value: str):
    if value:
        cli_console.message(PLAIN_PASSWORD_MSG)

    return _callback(lambda: cli_context_manager.connection_context.set_password)(value)


PasswordOption = typer.Option(
    None,
    "--password",
    help="Snowflake password. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_password_callback,
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

AuthenticatorOption = typer.Option(
    None,
    "--authenticator",
    help="Snowflake authenticator. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_authenticator
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

PrivateKeyPathOption = typer.Option(
    None,
    "--private-key-path",
    help="Snowflake private key path. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_private_key_path
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
    dir_okay=False,
)

SessionTokenOption = typer.Option(
    None,
    "--session-token",
    help="Snowflake session token. Can be used only in conjunction with --master-token. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_session_token
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
    dir_okay=False,
    hidden=True,
)

MasterTokenOption = typer.Option(
    None,
    "--master-token",
    help="Snowflake master token. Can be used only in conjunction with --session-token. Overrides the value specified for the connection.",
    hide_input=True,
    callback=_callback(lambda: cli_context_manager.connection_context.set_master_token),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
    dir_okay=False,
    hidden=True,
)

TokenFilePathOption = typer.Option(
    None,
    "--token-file-path",
    help="Path to file with an OAuth token that should be used when connecting to Snowflake",
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_token_file_path
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
    dir_okay=False,
)

DatabaseOption = typer.Option(
    None,
    "--database",
    "--dbname",
    help="Database to use. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_database),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

SchemaOption = typer.Option(
    None,
    "--schema",
    "--schemaname",
    help="Database schema to use. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_schema),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

RoleOption = typer.Option(
    None,
    "--role",
    "--rolename",
    help="Role to use. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_role),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

WarehouseOption = typer.Option(
    None,
    "--warehouse",
    help="Warehouse to use. Overrides the value specified for the connection.",
    callback=_callback(lambda: cli_context_manager.connection_context.set_warehouse),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

MfaPasscodeOption = typer.Option(
    None,
    "--mfa-passcode",
    help="Token to use for multi-factor authentication (MFA)",
    callback=_callback(lambda: cli_context_manager.connection_context.set_mfa_passcode),
    prompt="MFA passcode",
    prompt_required=False,
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
)

EnableDiagOption = typer.Option(
    False,
    "--enable-diag",
    help="Run python connector diagnostic test",
    callback=_callback(lambda: cli_context_manager.connection_context.set_enable_diag),
    show_default=False,
    is_flag=True,
    rich_help_panel=_CONNECTION_SECTION,
)

DiagLogPathOption: Path = typer.Option(
    tempfile.gettempdir(),
    "--diag-log-path",
    help="Diagnostic report path",
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_diag_log_path
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    writable=True,
)

DiagAllowlistPathOption: Path = typer.Option(
    None,
    "--diag-allowlist-path",
    help="Diagnostic report path to optional allowlist",
    callback=_callback(
        lambda: cli_context_manager.connection_context.set_diag_allowlist_path
    ),
    show_default=False,
    rich_help_panel=_CONNECTION_SECTION,
    exists=True,
    file_okay=True,
)

OutputFormatOption = typer.Option(
    OutputFormat.TABLE.value,
    "--format",
    help="Specifies the output format.",
    case_sensitive=False,
    callback=_callback(lambda: cli_context_manager.set_output_format),
    rich_help_panel=_CLI_BEHAVIOUR,
)

SilentOption = typer.Option(
    False,
    "--silent",
    help="Turns off intermediate output to console.",
    callback=_callback(lambda: cli_context_manager.set_silent),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
    is_eager=True,
)

VerboseOption = typer.Option(
    False,
    "--verbose",
    "-v",
    help="Displays log entries for log levels `info` and higher.",
    callback=_callback(lambda: cli_context_manager.set_verbose),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)

DebugOption = typer.Option(
    False,
    "--debug",
    help="Displays log entries for log levels `debug` and higher; debug logs contains additional information.",
    callback=_callback(lambda: cli_context_manager.set_enable_tracebacks),
    is_flag=True,
    rich_help_panel=_CLI_BEHAVIOUR,
)


# If IfExistsOption, IfNotExistsOption, or ReplaceOption are used with names other than those in CREATE_MODE_OPTION_NAMES,
# you must also override mutually_exclusive if you want to retain the validation that at most one of these flags is
# passed.
CREATE_MODE_OPTION_NAMES = ["if_exists", "if_not_exists", "replace"]

IfExistsOption = OverrideableOption(
    False,
    "--if-exists",
    help="Only apply this operation if the specified object exists.",
    mutually_exclusive=CREATE_MODE_OPTION_NAMES,
)

IfNotExistsOption = OverrideableOption(
    False,
    "--if-not-exists",
    help="Only apply this operation if the specified object does not already exist.",
    mutually_exclusive=CREATE_MODE_OPTION_NAMES,
)

ReplaceOption = OverrideableOption(
    False,
    "--replace",
    help="Replace this object if it already exists.",
    mutually_exclusive=CREATE_MODE_OPTION_NAMES,
)

OnErrorOption = typer.Option(
    OnErrorType.BREAK.value,
    "--on-error",
    help="What to do when an error occurs. Defaults to break.",
)

NoInteractiveOption = typer.Option(False, "--no-interactive", help="Disable prompting.")


def variables_option(description: str):
    return typer.Option(
        None,
        "--variable",
        "-D",
        help=description,
        show_default=False,
    )


ExecuteVariablesOption = variables_option(
    'Variables for the execution context. For example: `-D "<key>=<value>"`. '
    "For SQL files variables are use to expand the template and any unknown variable will cause an error. "
    "For Python files variables are used to update os.environ dictionary. Provided keys are capitalized to adhere to best practices."
    "In case of SQL files string values must be quoted in `''` (consider embedding quoting in the file).",
)


def like_option(help_example: str):
    return typer.Option(
        "%%",
        "--like",
        "-l",
        help=f"SQL LIKE pattern for filtering objects by name. For example, {help_example}.",
    )


def _pattern_option_callback(value):
    if value and value.count("'") != value.count("\\'"):
        raise ClickException('All "\'" characters in PATTERN must be escaped: "\\\'"')
    return value


PatternOption = typer.Option(
    None,
    "--pattern",
    help=(
        "Regex pattern for filtering files by name."
        r' For example --pattern ".*\.txt" will filter only files with .txt extension.'
    ),
    show_default=False,
    callback=_pattern_option_callback,
)


def experimental_option(
    experimental_behaviour_description: Optional[str] = None,
) -> typer.Option:
    help_text = (
        f"Turns on experimental behaviour of the command: {experimental_behaviour_description}"
        if experimental_behaviour_description
        else "Turns on experimental behaviour of the command."
    )
    return typer.Option(
        False,
        "--experimental",
        help=help_text,
        hidden=True,
        callback=_callback(lambda: cli_context_manager.set_experimental),
        is_flag=True,
        rich_help_panel=_CLI_BEHAVIOUR,
    )


def identifier_argument(sf_object: str, example: str) -> typer.Argument:
    return typer.Argument(
        ...,
        help=f"Identifier of the {sf_object}. For example: {example}",
        show_default=False,
    )


def execution_identifier_argument(sf_object: str, example: str) -> typer.Argument:
    return typer.Argument(
        ...,
        help=f"Execution identifier of the {sf_object}. For example: {example}",
        show_default=False,
    )


def register_project_definition(is_optional: bool) -> None:
    project_path = cli_context_manager.project_path_arg
    env_overrides_args = cli_context_manager.project_env_overrides_args

    dm = DefinitionManager(project_path, {CONTEXT_KEY: {"env": env_overrides_args}})
    project_definition = dm.project_definition
    project_root = dm.project_root
    template_context = dm.template_context

    if not dm.has_definition_file and not is_optional:
        raise MissingConfiguration(
            "Cannot find project definition (snowflake.yml). Please provide a path to the project or run this command in a valid project directory."
        )

    cli_context_manager.set_project_definition(project_definition)
    cli_context_manager.set_project_root(project_root)
    cli_context_manager.set_template_context(template_context)


def project_definition_option(is_optional: bool):
    def project_definition_callback(project_path: str) -> None:
        cli_context_manager.set_project_path_arg(project_path)
        register_pre_execute_command(lambda: register_project_definition(is_optional))

    return typer.Option(
        None,
        "-p",
        "--project",
        help=f"Path where Snowflake project resides. "
        f"Defaults to current working directory.",
        callback=_callback(lambda: project_definition_callback),
        show_default=False,
    )


def project_env_overrides_option():
    def project_env_overrides_callback(env_overrides_args_list: list[str]) -> None:
        env_overrides_args_map = {
            v.key: v.value for v in parse_key_value_variables(env_overrides_args_list)
        }
        cli_context_manager.set_project_env_overrides_args(env_overrides_args_map)

    return typer.Option(
        [],
        "--env",
        help="String in format of key=value. Overrides variables from env section used for templating.",
        callback=_callback(lambda: project_env_overrides_callback),
        show_default=False,
    )


def readable_file_option(param_name: str, help_str: str) -> typer.Option:
    return typer.Option(
        None,
        param_name,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        help=help_str,
        show_default=False,
    )


def deprecated_flag_callback(msg: str):
    def _warning_callback(ctx: click.Context, param: click.Parameter, value: Any):
        if ctx.get_parameter_source(param.name) != click.core.ParameterSource.DEFAULT:  # type: ignore[attr-defined]
            cli_console.warning(message=msg)
        return value

    return _warning_callback


def deprecated_flag_callback_enum(msg: str):
    def _warning_callback(ctx: click.Context, param: click.Parameter, value: Any):
        if ctx.get_parameter_source(param.name) != click.core.ParameterSource.DEFAULT:  # type: ignore[attr-defined]
            cli_console.warning(message=msg)
        # Typer bug: enums passed through callback are turning into None,
        # unless their explicit value is returned ¯\_(ツ)_/¯
        return value.value

    return _warning_callback


@dataclass
class Variable:
    key: str
    value: str

    def __init__(self, key: str, value: str):
        self.key = key
        self.value = value


def parse_key_value_variables(variables: Optional[List[str]]) -> List[Variable]:
    """Util for parsing key=value input. Useful for commands accepting multiple input options."""
    if not variables:
        return []
    result: List[Variable] = []
    if not variables:
        return result
    for p in variables:
        if "=" not in p:
            raise ClickException(f"Invalid variable: '{p}'")

        key, value = p.split("=", 1)
        result.append(Variable(key.strip(), value.strip()))
    return result
