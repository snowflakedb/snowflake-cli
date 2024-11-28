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

from typing import NoReturn

from click import ClickException
from snowflake.cli._plugins.nativeapp.sf_facade_constants import UseObjectType
from snowflake.cli.api.errno import (
    APPLICATION_FILE_NOT_FOUND_ON_STAGE,
    APPLICATION_INSTANCE_EMPTY_SETUP_SCRIPT,
    APPLICATION_INSTANCE_FAILED_TO_RUN_SETUP_SCRIPT,
    APPLICATION_INSTANCE_NO_ACTIVE_WAREHOUSE_FOR_CREATE_OR_UPGRADE,
    APPLICATION_NO_LONGER_AVAILABLE,
    APPLICATION_PACKAGE_CANNOT_SET_EXTERNAL_DISTRIBUTION_WITH_SPCS,
    APPLICATION_PACKAGE_MANIFEST_CONTAINER_IMAGE_URL_BAD_VALUE,
    APPLICATION_PACKAGE_MANIFEST_SPECIFIED_FILE_NOT_FOUND,
    APPLICATION_PACKAGE_PATCH_DOES_NOT_EXIST,
    CANNOT_GRANT_NON_MANIFEST_PRIVILEGE,
    CANNOT_GRANT_OBJECT_NOT_IN_APP_PACKAGE,
    CANNOT_GRANT_RESTRICTED_PRIVILEGE_TO_APP_PACKAGE_SHARE,
    CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
    CANNOT_UPGRADE_FROM_VERSION_TO_LOOSE_FILES,
    NATIVE_APPLICATION_MANIFEST_GENERIC_JSON_ERROR,
    NATIVE_APPLICATION_MANIFEST_INVALID_SYNTAX,
    NATIVE_APPLICATION_MANIFEST_UNEXPECTED_VALUE_FOR_PROPERTY,
    NATIVE_APPLICATION_MANIFEST_UNRECOGNIZED_FIELD,
    NO_REFERENCE_SET_FOR_DEFINITION,
    NO_VERSIONS_AVAILABLE_FOR_ACCOUNT,
    NOT_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    ONLY_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    ROLE_NOT_ASSIGNED,
    SNOWSERVICES_IMAGE_MANIFEST_NOT_FOUND,
    SNOWSERVICES_IMAGE_REPOSITORY_FAILS_TO_RETRIEVE_IMAGE_HASH_NEW,
    SNOWSERVICES_IMAGE_REPOSITORY_IMAGE_IMPORT_TO_NATIVE_APP_FAIL,
    VIEW_EXPANSION_FAILED,
)
from snowflake.connector import DatabaseError, Error, ProgrammingError

# Reasons why an `alter application ... upgrade` might fail
UPGRADE_RESTRICTION_CODES = {
    CANNOT_UPGRADE_FROM_LOOSE_FILES_TO_VERSION,
    CANNOT_UPGRADE_FROM_VERSION_TO_LOOSE_FILES,
    ONLY_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    NOT_SUPPORTED_ON_DEV_MODE_APPLICATIONS,
    APPLICATION_NO_LONGER_AVAILABLE,
}

CREATE_OR_UPGRADE_APPLICATION_EXPECTED_USER_ERROR_CODES = {
    APPLICATION_INSTANCE_FAILED_TO_RUN_SETUP_SCRIPT,
    NATIVE_APPLICATION_MANIFEST_GENERIC_JSON_ERROR,
    APPLICATION_INSTANCE_NO_ACTIVE_WAREHOUSE_FOR_CREATE_OR_UPGRADE,
    # when setup script/manifest/readme isn't on the stage
    APPLICATION_FILE_NOT_FOUND_ON_STAGE,
    NATIVE_APPLICATION_MANIFEST_UNRECOGNIZED_FIELD,
    SNOWSERVICES_IMAGE_MANIFEST_NOT_FOUND,
    # user tried to clone tables and it failed
    VIEW_EXPANSION_FAILED,
    # user tried to do something with a role that wasn't assigned to them
    ROLE_NOT_ASSIGNED,
    APPLICATION_PACKAGE_MANIFEST_SPECIFIED_FILE_NOT_FOUND,
    SNOWSERVICES_IMAGE_REPOSITORY_IMAGE_IMPORT_TO_NATIVE_APP_FAIL,
    APPLICATION_PACKAGE_PATCH_DOES_NOT_EXIST,
    APPLICATION_PACKAGE_MANIFEST_CONTAINER_IMAGE_URL_BAD_VALUE,
    SNOWSERVICES_IMAGE_REPOSITORY_FAILS_TO_RETRIEVE_IMAGE_HASH_NEW,
    NATIVE_APPLICATION_MANIFEST_UNEXPECTED_VALUE_FOR_PROPERTY,
    CANNOT_GRANT_NON_MANIFEST_PRIVILEGE,
    NO_REFERENCE_SET_FOR_DEFINITION,
    NATIVE_APPLICATION_MANIFEST_INVALID_SYNTAX,
    CANNOT_GRANT_OBJECT_NOT_IN_APP_PACKAGE,
    APPLICATION_PACKAGE_MANIFEST_SPECIFIED_FILE_NOT_FOUND,
    # user tried installing from release directive and there are none available
    NO_VERSIONS_AVAILABLE_FOR_ACCOUNT,
    APPLICATION_PACKAGE_MANIFEST_CONTAINER_IMAGE_URL_BAD_VALUE,
    APPLICATION_INSTANCE_EMPTY_SETUP_SCRIPT,
    APPLICATION_PACKAGE_CANNOT_SET_EXTERNAL_DISTRIBUTION_WITH_SPCS,
    CANNOT_GRANT_RESTRICTED_PRIVILEGE_TO_APP_PACKAGE_SHARE,
}


def handle_unclassified_error(err: Error | Exception, context: str) -> NoReturn:
    """
    Handles exceptions that are not caught by categorized exceptions in SQLFacade
    @param err: connector error or base exception
    @param context: message to add context to exception
    """
    message = f"{context} {str(err)}"
    if isinstance(err, ProgrammingError):
        raise InvalidSQLError(message) from err

    if isinstance(err, DatabaseError):
        raise UnknownSQLError(message) from err

    if isinstance(err, Error):
        raise UnknownConnectorError(message) from err

    # Not a connector error
    raise Exception(message) from err


class _BaseFacadeError(Exception):
    """Base class for SnowflakeFacade Exceptions"""

    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)

    def __str__(self):
        return self.msg


class InvalidSQLError(_BaseFacadeError):
    """Raised when Snowflake executed a SQL command but encountered an error, for example due to syntax or logical errors"""

    def __init__(self, msg):
        super().__init__(f"Invalid SQL error occurred. {msg}")


class UnknownSQLError(_BaseFacadeError):
    """Raised when Snowflake could not execute the SQL command"""

    def __init__(self, msg):
        super().__init__(f"Unknown SQL error occurred. {msg}")


class UnknownConnectorError(_BaseFacadeError):
    """Raised when there was a problem reaching Snowflake to execute a SQL command"""

    def __init__(self, msg):
        super().__init__(f"Unknown error occurred. {msg}")


class UnexpectedResultError(_BaseFacadeError):
    """Raised when an unexpected result was returned from execution of a SQL command"""

    def __init__(self, msg):
        super().__init__(f"Received unexpected result from query. {msg}")


class UserScriptError(ClickException):
    """Exception raised when user-provided scripts fail."""

    def __init__(self, script_name, msg):
        super().__init__(f"Failed to run script {script_name}. {msg}")


class UserInputError(ClickException):
    """Exception raised when execution of SQL with user input fails."""

    pass


class CouldNotUseObjectError(UserInputError):
    def __init__(self, object_type: UseObjectType, name: str):
        super().__init__(
            f"Could not use {object_type} {name}. Object does not exist, or operation cannot be performed."
        )


class InsufficientPrivilegesError(ClickException):
    """Raised when user does not have sufficient privileges to perform an operation"""

    def __init__(
        self,
        message,
        *,
        role: str | None = None,
        database: str | None = None,
        schema: str | None = None,
    ):
        if schema:
            message += f" in schema: {schema}"
        if database:
            message += f" in database: {database}"
        if role:
            message += f" using role: {role}"
        super().__init__(message)


class UpgradeApplicationRestrictionError(UserInputError):
    """
    Raised when an alter application ... upgrade fails due to user error.
    Must be caught and handled by the caller of an upgrade_application
    """

    pass
