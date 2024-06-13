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

from textwrap import dedent

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


class ApplicationAlreadyExistsError(ClickException):
    """An application object not created by Snowflake CLI exists with the same name."""

    def __init__(self, name: str):
        super().__init__(
            f'A application object "{name}" not created in development mode using files on a named stage already exists in the account.'
        )


class UnexpectedOwnerError(ClickException):
    """An operation is blocked because an object is owned by an unexpected role."""

    def __init__(self, item: str, expected_owner: str, actual_owner: str):
        super().__init__(
            f"Cannot operate on {item}: owned by {actual_owner} (expected {expected_owner})"
        )


class MissingPackageScriptError(ClickException):
    """A referenced package script was not found."""

    def __init__(self, relpath: str):
        super().__init__(f'Package script "{relpath}" does not exist')


class InvalidPackageScriptError(ClickException):
    """A referenced package script had syntax error(s)."""

    def __init__(self, relpath: str, err: jinja2.TemplateError):
        super().__init__(f'Package script "{relpath}" is not a valid jinja2 template')
        self.err = err


class MissingSchemaError(ClickException):
    """An identifier is missing a schema qualifier."""

    def __init__(self, identifier: str):
        super().__init__(f'Identifier missing a schema qualifier: "{identifier}"')


class CouldNotDropApplicationPackageWithVersions(ClickException):
    """Application package could not be dropped as it has versions associated with it."""

    def __init__(self):
        super().__init__(
            dedent(
                f"""
            {self.__doc__}
            Versions must be dropped first using “snow app version drop”.
            """
            )
        )


class SetupScriptFailedValidation(ClickException):
    """Snowflake Native App setup script failed validation."""

    def __init__(self):
        super().__init__(self.__doc__)
