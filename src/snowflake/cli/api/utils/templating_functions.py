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

from typing import Any, List, Optional

from snowflake.cli.api.exceptions import InvalidTemplateError
from snowflake.cli.api.project.util import (
    concat_identifiers,
    get_env_username,
    identifier_to_str,
    sanitize_identifier,
    to_identifier,
)


class TemplatingFunctions:
    """
    This class contains all the functions available for templating.
    Any callable not starting with '_' will automatically be available for users to use.
    """

    @staticmethod
    def _verify_str_arguments(
        func_name: str,
        args: List[Any],
        *,
        min_count: Optional[int] = None,
        max_count: Optional[int] = None,
    ):
        if min_count is not None and len(args) < min_count:
            raise InvalidTemplateError(
                f"{func_name} requires at least {min_count} argument(s)"
            )

        if max_count is not None and len(args) > max_count:
            raise InvalidTemplateError(
                f"{func_name} supports at most {max_count} argument(s)"
            )

        for arg in args:
            if not isinstance(arg, str):
                raise InvalidTemplateError(f"{func_name} only accepts String values")

    @staticmethod
    def concat_ids(*args):
        """
        input: one or more string arguments (SQL ID or plain String).
        output: a valid SQL ID (quoted or unquoted)

        Takes on multiple String arguments and concatenate them into one String.
        If any of the Strings is a valid quoted ID, it will be unescaped for the concatenation process.
        The resulting String is then escaped and quoted if:
        - It contains non SQL safe characters
        - Any of the input was a valid quoted identifier.
        """
        TemplatingFunctions._verify_str_arguments("concat_ids", args, min_count=1)
        return concat_identifiers(args)

    @staticmethod
    def str_to_id(*args):
        """
        input: one string argument. (SQL ID or plain String)
        output: a valid SQL ID (quoted or unquoted)

        If the input is a valid quoted or valid unquoted identifier, return it as is.
        Otherwise, if the input contains unsafe characters and is not properly quoted,
        then escape it and quote it.
        """
        TemplatingFunctions._verify_str_arguments(
            "str_to_id", args, min_count=1, max_count=1
        )
        return to_identifier(args[0])

    @staticmethod
    def id_to_str(*args):
        """
        input: one string argument (SQL ID or plain String).
        output: a plain string

        If the input is a valid SQL ID, then unescape it and return the plain String version.
        Otherwise, return the input as is.
        """
        TemplatingFunctions._verify_str_arguments(
            "id_to_str", args, min_count=1, max_count=1
        )
        return identifier_to_str(args[0])

    @staticmethod
    def get_username(*args):
        """
        input: one optional string containing the fallback value
        output: current username detected from the Operating System

        If the current username is not found or is blank, return blank
        or use the fallback value if provided.
        """
        TemplatingFunctions._verify_str_arguments(
            "get_username", args, min_count=0, max_count=1
        )
        fallback_username = args[0] if len(args) > 0 else ""
        return get_env_username() or fallback_username

    @staticmethod
    def sanitize_id(*args):
        """
        input: one string argument
        output: a valid non-quoted SQL ID

        Removes any unsafe SQL characters from the input,
        prepend it with an underscore if it does not start with a valid character,
        and limit the result to 255 characters.
        The result is a valid unquoted SQL ID.
        """
        TemplatingFunctions._verify_str_arguments(
            "sanitize_id", args, min_count=1, max_count=1
        )

        return sanitize_identifier(args[0])


def get_templating_functions():
    """
    Returns a dictionary with all the functions available for templating
    """
    templating_functions = {
        func: getattr(TemplatingFunctions, func)
        for func in dir(TemplatingFunctions)
        if callable(getattr(TemplatingFunctions, func)) and not func.startswith("_")
    }

    return templating_functions
