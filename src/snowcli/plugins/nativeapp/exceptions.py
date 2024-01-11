from textwrap import dedent

import jinja2
from click.exceptions import ClickException


class ApplicationPackageAlreadyExistsError(ClickException):
    """An application package not created by SnowCLI exists with the same name."""

    def __init__(self, name: str):
        super().__init__(
            f"An Application Package {name} already exists in account that may have been created without snowCLI."
        )


class ApplicationPackageDoesNotExistError(ClickException):
    """An application package of the specified name does not exist in the Snowflake acccount."""

    def __init__(self, name: str):
        super().__init__(
            f"Application Package {name} does not exist in the Snowflake account."
        )


class ApplicationAlreadyExistsError(ClickException):
    """An application not created by SnowCLI exists with the same name."""

    def __init__(self, name: str):
        super().__init__(
            f'A non-dev application "{name}" already exists in the account.'
        )


class UnexpectedOwnerError(ClickException):
    """An operation is blocked becuase an object is owned by an unexpected role."""

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


class CouldNotFindDistributionForAppPackage(ClickException):
    """The 'distribution' attribute for an app package could not be found."""

    def __init__(self, name: str):
        super().__init__(
            f"Could not find the 'distribution' attribute for app package {name}."
        )


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
