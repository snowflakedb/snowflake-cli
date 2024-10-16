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

from textwrap import dedent
from typing import Optional

import jinja2
from click.exceptions import ClickException


class ApplicationPackageAlreadyExistsError(ClickException):
    """An application package not created by Snowflake CLI exists with the same name."""

    def __init__(self, name: str):
        super().__init__(
            f"An Application Package {name} already exists in account that may have been created without Snowflake CLI."
        )


class ApplicationPackageDoesNotExistError(ClickException):
    """An application package of the specified name does not exist in the Snowflake account or the current role isn't authorized."""

    def __init__(self, name: str):
        super().__init__(
            f"Application Package {name} does not exist in the Snowflake account or not authorized."
        )


class ApplicationCreatedExternallyError(ClickException):
    """An application object not created by Snowflake CLI exists with the same name."""

    def __init__(self, name: str):
        super().__init__(
            f'An application object "{name}" not created by Snowflake CLI already exists in the account.'
        )


class MissingScriptError(ClickException):
    """A referenced script was not found."""

    def __init__(self, relpath: str):
        super().__init__(f'Script "{relpath}" does not exist')


class InvalidTemplateInFileError(ClickException):
    """A referenced templated file had syntax error(s)."""

    def __init__(
        self, relpath: str, err: jinja2.TemplateError, lineno: Optional[int] = None
    ):
        lineno_str = f":{lineno}" if lineno is not None else ""
        super().__init__(
            f'File "{relpath}{lineno_str}" does not contain a valid template: {err.message}'
        )
        self.err = err


class MissingSchemaError(ClickException):
    """An identifier is missing a schema qualifier."""

    def __init__(self, identifier: str):
        super().__init__(f'Identifier missing a schema qualifier: "{identifier}"')


class CouldNotDropApplicationPackageWithVersions(ClickException):
    """Application package could not be dropped as it has versions associated with it."""

    def __init__(self, additional_msg: str = ""):
        super().__init__(
            dedent(
                f"""
            {self.__doc__}
            {additional_msg}
            """
            ).strip()
        )


class SetupScriptFailedValidation(ClickException):
    """Snowflake Native App setup script failed validation."""

    def __init__(self):
        super().__init__(self.__doc__)


class NoEventTableForAccount(ClickException):
    """No event table was found for this Snowflake account."""

    INSTRUCTIONS = dedent(
        """\
        Ask your Snowflake administrator to set up an event table for your account by following the docs at
        https://docs.snowflake.com/en/developer-guide/logging-tracing/event-table-setting-up.

        If your account is configured to send events to an organization event account, create a new
        connection to this account using `snow connection add` and re-run this command using the new connection.
        More information on event accounts is available at https://docs.snowflake.com/en/developer-guide/native-apps/setting-up-logging-and-events#configure-an-account-to-store-shared-events."""
    )

    def __init__(self):
        super().__init__(f"{self.__doc__}\n\n{self.INSTRUCTIONS}")


class ObjectPropertyNotFoundError(RuntimeError):
    def __init__(self, property_name: str, object_type: str, object_name: str):
        super().__init__(
            dedent(
                f"""\
                        Could not find the '{property_name}' attribute for {object_type} {object_name} in the output of SQL query:
                        'describe {object_type} {object_name}'
                        """
            )
        )
