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

from snowflake.cli.api.exceptions import InvalidTemplate
from snowflake.cli.api.project.util import (
    DEFAULT_USERNAME,
    clean_identifier,
    concat_identifiers,
    get_env_username,
    identifier_to_str,
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
            raise InvalidTemplate(
                f"{func_name} requires at least {min_count} argument(s)"
            )

        if max_count is not None and len(args) > max_count:
            raise InvalidTemplate(
                f"{func_name} supports at most {max_count} argument(s)"
            )

        for arg in args:
            if not isinstance(arg, str):
                raise InvalidTemplate(f"{func_name} only accepts String values")

    @staticmethod
    def id_concat(*args):
        TemplatingFunctions._verify_str_arguments("id_concat", args, min_count=1)
        return concat_identifiers(args)

    @staticmethod
    def str_to_id(*args):
        TemplatingFunctions._verify_str_arguments(
            "str_to_id", args, min_count=1, max_count=1
        )
        return to_identifier(args[0])

    @staticmethod
    def id_to_str(*args):
        TemplatingFunctions._verify_str_arguments(
            "id_to_str", args, min_count=1, max_count=1
        )
        return identifier_to_str(args[0])

    @staticmethod
    def get_username(*args):
        TemplatingFunctions._verify_str_arguments(
            "get_username", args, min_count=0, max_count=0
        )
        return get_env_username() or DEFAULT_USERNAME

    @staticmethod
    def clean_id(*args):
        TemplatingFunctions._verify_str_arguments(
            "clean_id", args, min_count=1, max_count=1
        )

        return clean_identifier(args[0])


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
