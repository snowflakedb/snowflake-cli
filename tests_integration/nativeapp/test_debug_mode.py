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

import yaml
import uuid
from typing import Optional

from snowflake.cli.api.project.util import generate_user_env
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from tests.project.fixtures import *
from tests_integration.test_utils import pushd

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


class ApplicationNotFoundError(Exception):
    pass


def is_debug_mode(conn: SnowflakeConnection, app_name: str) -> bool:
    """
    Returns the value of debug_mode for a given application, or
    raises ApplicationNotFoundException if no app with the given name is
    found on the given connection.
    """
    try:
        *_, cursor = conn.execute_string(f"describe application {app_name}")
        for row in cursor.fetchall():
            if row[0].lower() == "debug_mode":
                return row[1].lower() == "true"
    except ProgrammingError:
        pass

    raise ApplicationNotFoundError(
        f"Could not find an application with name {app_name}"
    )


def set_yml_application_debug(snowflake_yml: Path, debug: Optional[bool]):
    """
    Updates a snowflake.yml file, either setting or removing the
    native_app.application.debug flag as necessary.
    """
    pdf = yaml.load(snowflake_yml.read_text(), yaml.BaseLoader)

    if "native_app" not in pdf:
        pdf["native_app"] = dict()

    if "application" not in pdf["native_app"]:
        pdf["native_app"]["application"] = dict()

    if debug is None and "debug" in pdf["native_app"]["application"]:
        del pdf["native_app"]["application"]["debug"]
    elif debug is not None:
        pdf["native_app"]["application"]["debug"] = debug

    snowflake_yml.write_text(yaml.dump(pdf))


# Tests that debug mode is enabled by default on create, but not changed
# on upgrade without an explicit setting in snowflake.yml
@pytest.mark.integration
@pytest.mark.parametrize("project_definition_files", ["integration"], indirect=True)
def test_nativeapp_controlled_debug_mode(
    runner,
    snowflake_session,
    project_definition_files: List[Path],
):
    project_name = "integration"
    project_dir = project_definition_files[0].parent
    snowflake_yml = project_dir / "snowflake.yml"
    with pushd(project_dir):

        # make sure our chosen snowflake.yml doesn't have an opinion on debug mode
        set_yml_application_debug(snowflake_yml, None)
        assert "debug:" not in snowflake_yml.read_text()

        # make sure the app doesn't (yet) exist
        app_name = f"{project_name}_{USER_NAME}".upper()
        with pytest.raises(ApplicationNotFoundError):
            is_debug_mode(snowflake_session, app_name)

        # deploy the application
        result = runner.invoke_with_connection_json(
            ["app", "run"],
            env=TEST_ENV,
        )
        assert result.exit_code == 0

        try:
            # debug mode should be true by default on first app deploy,
            # because snowflake.yml doesn't set it explicitly either way ("uncontrolled")
            assert is_debug_mode(snowflake_session, app_name)

            # let's set debug mode to false out-of-band
            result = runner.invoke_with_connection_json(
                ["sql", "-q", f"alter application {app_name} set debug_mode = false"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            assert not is_debug_mode(snowflake_session, app_name)

            # re-deploying the app should not change debug mode
            result = runner.invoke_with_connection_json(
                ["app", "run"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            assert not is_debug_mode(snowflake_session, app_name)

            # modify snowflake.yml to explicitly set debug mode ("controlled")
            set_yml_application_debug(snowflake_yml, True)
            assert "debug:" in snowflake_yml.read_text()

            # now, re-deploying the app will set debug_mode to true
            result = runner.invoke_with_connection_json(
                ["app", "run"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
            assert is_debug_mode(snowflake_session, app_name)

            # make sure we always delete the app
            result = runner.invoke_with_connection_json(
                ["app", "teardown"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0

        finally:
            # teardown is idempotent, so we can execute it again with no ill effects
            result = runner.invoke_with_connection_json(
                ["app", "teardown", "--force"],
                env=TEST_ENV,
            )
            assert result.exit_code == 0
