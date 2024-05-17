import os
import os.path
import uuid
from textwrap import dedent

from snowflake.cli.api.project.util import generate_user_env

from tests.project.fixtures import *
from tests_integration.test_utils import pushd

USER_NAME = f"user_{uuid.uuid4().hex}"
TEST_ENV = generate_user_env(USER_NAME)


# Tests that we disallow polluting the project source through symlinks
@pytest.mark.integration
def test_nativeapp_bundle_does_not_create_files_outside_deploy_root(
    runner,
    snowflake_session,
    temporary_working_directory,
):
    project_name = "myapp"
    result = runner.invoke_json(
        ["app", "init", project_name],
        env=TEST_ENV,
    )
    assert result.exit_code == 0

    with pushd(Path(os.getcwd(), project_name)):
        # overwrite the snowflake.yml rules
        with open("snowflake.yml", "w") as f:
            f.write(
                dedent(
                    f"""
            definition_version: 1
            native_app:
              name: myapp
              artifacts:
                - src: app
                  dest: ./
                - src: snowflake.yml
                  dest: ./app/
            """
                )
            )

        result = runner.invoke_json(
            ["app", "bundle"],
            env=TEST_ENV,
        )
        assert result.exit_code == 1
        assert (
            "The specified destination path is outside of the deploy root"
            in result.output
        )

        assert not os.path.exists("app/snowflake.yml")
