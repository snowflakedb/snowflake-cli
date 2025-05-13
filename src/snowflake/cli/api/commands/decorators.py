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

import inspect
from functools import wraps
from inspect import Signature
from typing import Callable, Dict, List, Optional, get_type_hints

from snowflake.cli.api.cli_global_context import get_cli_context
from snowflake.cli.api.commands.flags import (
    AccountOption,
    AuthenticatorOption,
    ClientStoreTemporaryCredentialOption,
    ConnectionOption,
    DatabaseOption,
    DebugOption,
    DiagAllowlistPathOption,
    DiagLogPathOption,
    EnableDiagOption,
    EnhancedExitCodesOption,
    HostOption,
    MasterTokenOption,
    MfaPasscodeOption,
    OauthAuthorizationUrlOption,
    OauthClientIdOption,
    OauthClientSecretOption,
    OauthDisablePkceOption,
    OauthEnableRefreshTokensOption,
    OauthEnableSingleUseRefreshTokensOption,
    OauthRedirectUriOption,
    OauthScopeOption,
    OauthTokenRequestUrlOption,
    OutputFormatOption,
    PasswordOption,
    PortOption,
    PrivateKeyPathOption,
    RoleOption,
    SchemaOption,
    SessionTokenOption,
    SilentOption,
    TemporaryConnectionOption,
    TokenFilePathOption,
    UserOption,
    VerboseOption,
    WarehouseOption,
    experimental_option,
    project_definition_option,
    project_env_overrides_option,
)
from snowflake.cli.api.exceptions import CommandReturnTypeError
from snowflake.cli.api.output.formats import OutputFormat
from snowflake.cli.api.output.types import CommandResult


def global_options(func: Callable):
    """
    Decorator providing default flags for overriding global parameters. Values are
    updated in global SnowCLI state.

    To use this decorator your command needs to accept **options as last argument.
    """
    return _global_options_decorator_factory(func, GLOBAL_OPTIONS)


def global_options_with_connection(func: Callable):
    """
    Decorator providing default flags including connection flags for overriding
    global parameters. Values are updated in global SnowCLI state.

    To use this decorator your command needs to accept **options as last argument.
    """
    return _global_options_decorator_factory(
        func, [*GLOBAL_CONNECTION_OPTIONS, *GLOBAL_OPTIONS]
    )


def with_project_definition(is_optional: bool = False):
    def _decorator(func: Callable):
        return _options_decorator_factory(
            func,
            additional_options=[
                inspect.Parameter(
                    "project_definition",
                    inspect.Parameter.KEYWORD_ONLY,
                    annotation=Optional[str],
                    default=project_definition_option(is_optional),
                ),
                inspect.Parameter(
                    "env_overrides",
                    inspect.Parameter.KEYWORD_ONLY,
                    annotation=List[str],
                    default=project_env_overrides_option(),
                ),
            ],
        )

    return _decorator


def with_experimental_behaviour(
    experimental_behaviour_description: Optional[str] = None,
):
    """
    Decorator providing --experimental flag, which allows to use experimental behaviour in commands implementation.

    To use this decorator your command needs to accept **options as last argument.
    """
    additional_options = [
        inspect.Parameter(
            "experimental",
            inspect.Parameter.KEYWORD_ONLY,
            annotation=Optional[bool],
            default=experimental_option(experimental_behaviour_description),
        ),
    ]

    def decorator(func: Callable):
        return _options_decorator_factory(
            func=func,
            additional_options=additional_options,
        )

    return decorator


def _execute_before_command_using_global_options(**options):
    from snowflake.cli._app.loggers import create_loggers

    cli_context = get_cli_context()
    create_loggers(cli_context.verbose, cli_context.enable_tracebacks)


def _global_options_decorator_factory(
    func: Callable, additional_options: List[inspect.Parameter]
):
    return _options_decorator_factory(
        func=func,
        additional_options=additional_options,
        execute_before_command_using_new_options=_execute_before_command_using_global_options,
    )


def _options_decorator_factory(
    func: Callable,
    additional_options: List[inspect.Parameter],
    execute_before_command_using_new_options: Optional[Callable] = None,
):
    """
    execute_before_command_using_new_options executes before command telemetry has been emitted,
    but after command line options have been populated.
    """

    @wraps(func)
    def wrapper(**options):
        if execute_before_command_using_new_options:
            execute_before_command_using_new_options(**options)
        return func(**options)

    wrapper.__signature__ = _extend_signature_with_additional_options(  # type: ignore
        func, additional_options
    )

    return wrapper


def _extend_signature_with_additional_options(
    func: Callable, additional_options: List[inspect.Parameter]
) -> Signature:
    """Extends function signature with additional options"""
    sig = inspect.signature(func)

    # Remove **options from signature
    existing_parameters = tuple(
        [p for p in sig.parameters.values() if not _is_options_parameter(p)]
    )

    type_hints = get_type_hints(func)
    existing_parameters_with_evaluated_types = [
        _evaluate_param_type(p, type_hints) for p in existing_parameters
    ]
    parameters = [
        *existing_parameters_with_evaluated_types,
        *additional_options,
    ]
    return sig.replace(parameters=parameters)


def _is_options_parameter(param: inspect.Parameter) -> bool:
    return param.kind == inspect.Parameter.VAR_KEYWORD and param.name == "options"


def _evaluate_param_type(
    param: inspect.Parameter, type_hints: Dict[str, type]
) -> inspect.Parameter:
    type_annotation = (
        type_hints.get(param.annotation)
        if isinstance(param.annotation, str)
        else param.annotation
    )
    return inspect.Parameter(
        name=param.name,
        kind=param.kind,
        annotation=type_annotation,
        default=param.default,
    )


GLOBAL_CONNECTION_OPTIONS = [
    inspect.Parameter(
        "connection",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=ConnectionOption,
    ),
    inspect.Parameter(
        "host",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=HostOption,
    ),
    inspect.Parameter(
        "port",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[int],
        default=PortOption,
    ),
    inspect.Parameter(
        "account",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=AccountOption,
    ),
    inspect.Parameter(
        "user",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=UserOption,
    ),
    inspect.Parameter(
        "password",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=PasswordOption,
    ),
    inspect.Parameter(
        "authenticator",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=AuthenticatorOption,
    ),
    inspect.Parameter(
        "private_key_file",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=PrivateKeyPathOption,
    ),
    inspect.Parameter(
        "session_token",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=SessionTokenOption,
    ),
    inspect.Parameter(
        "master_token",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=MasterTokenOption,
    ),
    inspect.Parameter(
        "token_file_path",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=TokenFilePathOption,
    ),
    inspect.Parameter(
        "database",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=DatabaseOption,
    ),
    inspect.Parameter(
        "schema",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=SchemaOption,
    ),
    inspect.Parameter(
        "role",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=RoleOption,
    ),
    inspect.Parameter(
        "warehouse",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=WarehouseOption,
    ),
    inspect.Parameter(
        "temporary_connection",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=TemporaryConnectionOption,
    ),
    inspect.Parameter(
        "mfa_passcode",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=MfaPasscodeOption,
    ),
    inspect.Parameter(
        "enable_diag",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=EnableDiagOption,
    ),
    inspect.Parameter(
        "diag_log_path",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=DiagLogPathOption,
    ),
    inspect.Parameter(
        "diag_allowlist_path",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=DiagAllowlistPathOption,
    ),
    inspect.Parameter(
        "oauth_client_id",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=OauthClientIdOption,
    ),
    inspect.Parameter(
        "oauth_client_secret",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=OauthClientSecretOption,
    ),
    inspect.Parameter(
        "oauth_authorization_url",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=OauthAuthorizationUrlOption,
    ),
    inspect.Parameter(
        "oauth_token_request_url",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=OauthTokenRequestUrlOption,
    ),
    inspect.Parameter(
        "oauth_redirect_uri",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=OauthRedirectUriOption,
    ),
    inspect.Parameter(
        "oauth_scope",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[str],
        default=OauthScopeOption,
    ),
    inspect.Parameter(
        "oauth_disable_pkce",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=OauthDisablePkceOption,
    ),
    inspect.Parameter(
        "oauth_enable_refresh_tokens",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=OauthEnableRefreshTokensOption,
    ),
    inspect.Parameter(
        "oauth_enable_single_use_refresh_tokens",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=OauthEnableSingleUseRefreshTokensOption,
    ),
    inspect.Parameter(
        "client_store_temporary_credential",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=ClientStoreTemporaryCredentialOption,
    ),
]

GLOBAL_OPTIONS = [
    inspect.Parameter(
        "format",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=OutputFormat,
        default=OutputFormatOption,
    ),
    inspect.Parameter(
        "verbose",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=VerboseOption,
    ),
    inspect.Parameter(
        "debug",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=DebugOption,
    ),
    inspect.Parameter(
        "silent",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=SilentOption,
    ),
    inspect.Parameter(
        "enhanced_exit_codes",
        inspect.Parameter.KEYWORD_ONLY,
        annotation=Optional[bool],
        default=EnhancedExitCodesOption,
    ),
]


def with_output(func):
    from snowflake.cli._app.printing import print_result

    @wraps(func)
    def wrapper(*args, **kwargs):
        output_data = func(*args, **kwargs)
        if not isinstance(output_data, CommandResult):
            raise CommandReturnTypeError(type(output_data))
        print_result(output_data)

    return wrapper
