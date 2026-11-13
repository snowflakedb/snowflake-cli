import pytest
import yaml
from snowflake.cli.api.constants import DEFAULT_SIZE_LIMIT_MB
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.project.util import to_identifier
from snowflake.cli.api.secure_path import SecurePath

from tests_integration.testing_utils import FlowTestSetup
from tests_integration.testing_utils.streamlit_utils import (
    WAREHOUSE_RUNTIME_NAME,
    StreamlitTestSteps,
)

# TODO: use below constant instead of hardcoded values
APP_1 = "app_1"


@pytest.mark.integration
@pytest.mark.parametrize("prune", [False, True])
def test_versioned_main_file_change_preserves_app_and_grants(
    _streamlit_test_steps,
    project_directory,
    snowflake_session,
    alter_snowflake_yml,
    tmp_path,
    prune,
):
    """Changing the entrypoint keeps object identity, live stage and manual grants."""
    runner = _streamlit_test_steps.setup.runner
    sql = _streamlit_test_steps.setup.sql_test_helper
    app = FQN(
        database=snowflake_session.database, schema=snowflake_session.schema, name=APP_1
    )

    with project_directory("streamlit_v2") as root:
        alter_snowflake_yml(
            root / "snowflake.yml",
            "entities.app_1.runtime_name",
            WAREHOUSE_RUNTIME_NAME,
        )
        alter_snowflake_yml(
            root / "snowflake.yml",
            "entities.app_1.query_warehouse",
            snowflake_session.warehouse,
        )
        try:
            result = runner.invoke_with_connection_json(["streamlit", "deploy", APP_1])
            assert result.exit_code == 0, result.output
            before = runner.invoke_with_connection_json(
                ["streamlit", "describe", str(app)]
            )
            assert before.exit_code == 0, before.output
            stage = before.json[0]["live_version_location_uri"]
            created = runner.invoke_with_connection_json(
                ["streamlit", "list", "--like", APP_1]
            )
            assert created.exit_code == 0, created.output
            created_on = created.json[0]["created_on"]

            # This grant is intentionally absent from the project definition.
            sql.execute_single_sql(
                f"GRANT USAGE ON STREAMLIT {app.sql_identifier} TO ROLE {to_identifier(snowflake_session.role)}"
            )
            grants_before = sql.execute_single_sql(
                f"SHOW GRANTS ON STREAMLIT {app.sql_identifier}"
            )
            grant_columns = (
                "privilege",
                "granted_to",
                "grantee_name",
                "grant_option",
                "granted_by",
            )
            expected_grants = {
                tuple(row[column] for column in grant_columns) for row in grants_before
            }
            assert any(row["privilege"] == "USAGE" for row in grants_before)

            stage_only = tmp_path / "stage_only.txt"
            SecurePath(stage_only).write_text("keep until an explicit successful prune")
            uploaded = runner.invoke_with_connection_json(
                ["stage", "copy", str(stage_only), stage, "--overwrite"]
            )
            assert uploaded.exit_code == 0, uploaded.output

            SecurePath(root / "renamed_app.py").write_text(
                "import streamlit as st\nst.write('renamed entrypoint')\n"
            )
            alter_snowflake_yml(
                root / "snowflake.yml", "entities.app_1.main_file", "renamed_app.py"
            )
            alter_snowflake_yml(
                root / "snowflake.yml", "entities.app_1.artifacts", ["app_1.py"]
            )
            # Omit the warehouse so the entrypoint update must retain its live value.
            definition = SecurePath(root / "snowflake.yml")
            project = yaml.safe_load(definition.read_text(DEFAULT_SIZE_LIMIT_MB))
            del project["entities"][APP_1]["query_warehouse"]
            definition.write_text(yaml.safe_dump(project))
            command = ["streamlit", "deploy", APP_1, "--replace"]
            if prune:
                command.append("--prune")
            changed = runner.invoke_with_connection_json(command)
            assert changed.exit_code == 0, changed.output

            after = runner.invoke_with_connection_json(
                ["streamlit", "describe", str(app)]
            )
            assert after.exit_code == 0, after.output
            assert after.json[0]["main_file"] == "renamed_app.py"
            for column in (
                "live_version_location_uri",
                "query_warehouse",
                "runtime_name",
            ):
                assert after.json[0][column] == before.json[0][column]
            listed = runner.invoke_with_connection_json(
                ["streamlit", "list", "--like", APP_1]
            )
            assert listed.exit_code == 0, listed.output
            assert listed.json[0]["created_on"] == created_on
            grants_after = sql.execute_single_sql(
                f"SHOW GRANTS ON STREAMLIT {app.sql_identifier}"
            )
            assert {
                tuple(row[column] for column in grant_columns) for row in grants_after
            } == expected_grants
            files = {
                name.removeprefix("/versions/live/")
                for name in _streamlit_test_steps.get_actual_file_staged_in_db(stage)
            }
            assert {"renamed_app.py", "app_1.py"}.issubset(files)
            assert ("stage_only.txt" in files) is not prune
            assert ("streamlit_app.py" in files) is not prune
            _streamlit_test_steps.execute_should_run_streamlit(APP_1, snowflake_session)
        finally:
            sql.execute_single_sql(f"DROP STREAMLIT IF EXISTS {app.sql_identifier}")


@pytest.mark.integration
def test_streamlit_flow(
    _streamlit_test_steps,
    project_directory,
    test_database,
    snowflake_session,
    alter_snowflake_yml,
):
    database = test_database.upper()
    with project_directory("streamlit_v2"):
        # This flow asserts an exact staged file set and runs `streamlit execute`.
        alter_snowflake_yml(
            "snowflake.yml", "entities.app_1.runtime_name", WAREHOUSE_RUNTIME_NAME
        )

        _streamlit_test_steps.list_streamlit_should_return_empty_list()

        _streamlit_test_steps.deploy_should_result_in_error_as_there_are_multiple_entities_in_project_file()

        _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
            "app_1", snowflake_session
        )

        stage_root = f"snow://streamlit/{snowflake_session.database}.{snowflake_session.schema}.app_1/versions/live/"

        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1.py", "streamlit_app.py"], stage_root, uploaded_to_live_version=True
        )
        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.APP_1"], "APP_1"
        )

        _streamlit_test_steps.another_deploy_without_replace_flag_should_end_with_error(
            "app_1", snowflake_session
        )
        _streamlit_test_steps.another_deploy_with_replace_flag_should_succeed(
            "app_1", snowflake_session
        )

        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.APP_1"], "APP_1"
        )

        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1.py", "streamlit_app.py"], stage_root, uploaded_to_live_version=True
        )

        _streamlit_test_steps.streamlit_describe_should_show_proper_streamlit(
            APP_1, snowflake_session
        )

        _streamlit_test_steps.get_url_should_give_proper_url(APP_1, snowflake_session)

        _streamlit_test_steps.execute_should_run_streamlit(APP_1, snowflake_session)

        _streamlit_test_steps.drop_should_succeed(APP_1, snowflake_session)

        _streamlit_test_steps.list_streamlit_should_return_empty_list()


@pytest.mark.integration
def test_streamlit_experimental_flow(
    _streamlit_test_steps,
    project_directory,
    test_database,
    snowflake_session,
    alter_snowflake_yml,
):
    database = test_database.upper()
    with project_directory("streamlit_v2"):
        # This flow asserts an exact staged file set and runs `streamlit execute`.
        alter_snowflake_yml(
            "snowflake.yml", "entities.app_1.runtime_name", WAREHOUSE_RUNTIME_NAME
        )

        _streamlit_test_steps.list_streamlit_should_return_empty_list()

        _streamlit_test_steps.deploy_should_result_in_error_as_there_are_multiple_entities_in_project_file()

        _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
            "app_1", snowflake_session
        )

        stage_root = f"snow://streamlit/{snowflake_session.database}.{snowflake_session.schema}.app_1/versions/live/"

        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1.py", "streamlit_app.py"], stage_root, uploaded_to_live_version=True
        )
        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.APP_1"], "APP_1"
        )

        _streamlit_test_steps.another_deploy_without_replace_flag_should_end_with_error(
            "app_1", snowflake_session
        )
        _streamlit_test_steps.another_deploy_with_replace_flag_should_succeed(
            "app_1", snowflake_session
        )

        _streamlit_test_steps.assert_that_only_those_entities_are_listed(
            [f"{database}.PUBLIC.APP_1"], "APP_1"
        )

        _streamlit_test_steps.assert_that_only_those_files_were_uploaded(
            ["app_1.py", "streamlit_app.py"], stage_root, uploaded_to_live_version=True
        )

        _streamlit_test_steps.streamlit_describe_should_show_proper_streamlit(
            APP_1, snowflake_session
        )

        _streamlit_test_steps.get_url_should_give_proper_url(APP_1, snowflake_session)

        _streamlit_test_steps.execute_should_run_streamlit(APP_1, snowflake_session)

        _streamlit_test_steps.drop_should_succeed(APP_1, snowflake_session)

        _streamlit_test_steps.list_streamlit_should_return_empty_list()


@pytest.fixture
def _test_setup(
    runner, sql_test_helper, test_database, temporary_working_directory, snapshot
):
    yield FlowTestSetup(
        runner=runner,
        sql_test_helper=sql_test_helper,
        test_database=test_database,
        snapshot=snapshot,
    )


@pytest.fixture
def _streamlit_test_steps(_test_setup):
    return StreamlitTestSteps(_test_setup)


@pytest.mark.integration
def test_streamlit_grants_flow(
    _streamlit_test_steps,
    project_directory,
    snowflake_session,
    alter_snowflake_yml,
):
    """Test that streamlit grants are properly applied during deployment."""
    test_role = snowflake_session.role
    entity_id = "app_1"

    with project_directory("streamlit_v2"):
        alter_snowflake_yml(
            "snowflake.yml",
            "entities.app_1.grants",
            [{"privilege": "USAGE", "role": test_role}],
        )

        _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
            entity_id, snowflake_session, experimental=False
        )

        _streamlit_test_steps.verify_grants_applied(entity_id, test_role)

        _streamlit_test_steps.drop_should_succeed(entity_id, snowflake_session)


@pytest.mark.integration
def test_streamlit_tags_flow(
    _streamlit_test_steps,
    project_directory,
    snowflake_session,
    alter_snowflake_yml,
):
    """Test that streamlit tags are properly applied during deployment."""
    entity_id = "app_1"
    tag_name = "cli_test_streamlit_tag"
    tag_value = "integration_test"

    _streamlit_test_steps.setup.sql_test_helper.execute_single_sql(
        f"CREATE TAG IF NOT EXISTS {tag_name}"
    )

    try:
        with project_directory("streamlit_v2"):
            alter_snowflake_yml(
                "snowflake.yml",
                "entities.app_1.tags",
                [{"name": tag_name, "value": tag_value}],
            )

            _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
                entity_id, snowflake_session
            )

            _streamlit_test_steps.verify_tags_applied(
                entity_id, tag_name, tag_value, snowflake_session
            )

            _streamlit_test_steps.drop_should_succeed(entity_id, snowflake_session)
    finally:
        _streamlit_test_steps.setup.sql_test_helper.execute_single_sql(
            f"DROP STREAMLIT IF EXISTS {entity_id}"
        )
        _streamlit_test_steps.setup.sql_test_helper.execute_single_sql(
            f"DROP TAG IF EXISTS {tag_name}"
        )


@pytest.mark.integration
def test_streamlit_tags_alter_flow(
    _streamlit_test_steps,
    project_directory,
    snowflake_session,
    alter_snowflake_yml,
):
    """
    Full tag lifecycle on re-deploy:
      1. Create with tag_a only.
      2. Re-deploy with tag_a + tag_b  → both present.
      3. Re-deploy with tag_b only     → tag_a removed, tag_b kept.
      4. Re-deploy with no tags        → both removed.
    """
    entity_id = "app_1"
    tag_a = "cli_test_streamlit_tag_a"
    tag_b = "cli_test_streamlit_tag_b"
    sql_helper = _streamlit_test_steps.setup.sql_test_helper

    try:
        sql_helper.execute_single_sql(f"CREATE TAG IF NOT EXISTS {tag_a}")
        sql_helper.execute_single_sql(f"CREATE TAG IF NOT EXISTS {tag_b}")

        with project_directory("streamlit_v2"):
            # 1. Fresh deploy with tag_a
            alter_snowflake_yml(
                "snowflake.yml",
                "entities.app_1.tags",
                [{"name": tag_a, "value": "initial"}],
            )
            _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
                entity_id, snowflake_session
            )
            _streamlit_test_steps.verify_tags_applied(
                entity_id, tag_a, "initial", snowflake_session
            )
            _streamlit_test_steps.verify_tag_not_set(
                entity_id, tag_b, snowflake_session
            )

            # 2. Re-deploy with tag_a + tag_b — both must be present
            alter_snowflake_yml(
                "snowflake.yml",
                "entities.app_1.tags",
                [
                    {"name": tag_a, "value": "initial"},
                    {"name": tag_b, "value": "added"},
                ],
            )
            _streamlit_test_steps.another_deploy_with_replace_flag_should_succeed(
                entity_id, snowflake_session
            )
            _streamlit_test_steps.verify_tags_applied(
                entity_id, tag_a, "initial", snowflake_session
            )
            _streamlit_test_steps.verify_tags_applied(
                entity_id, tag_b, "added", snowflake_session
            )

            # 3. Re-deploy with tag_b only — tag_a must be gone
            alter_snowflake_yml(
                "snowflake.yml",
                "entities.app_1.tags",
                [{"name": tag_b, "value": "kept"}],
            )
            _streamlit_test_steps.another_deploy_with_replace_flag_should_succeed(
                entity_id, snowflake_session
            )
            _streamlit_test_steps.verify_tag_not_set(
                entity_id, tag_a, snowflake_session
            )
            _streamlit_test_steps.verify_tags_applied(
                entity_id, tag_b, "kept", snowflake_session
            )

            # 4. Re-deploy with no tags — all must be gone
            alter_snowflake_yml(
                "snowflake.yml",
                "entities.app_1.tags",
                [],
            )
            _streamlit_test_steps.another_deploy_with_replace_flag_should_succeed(
                entity_id, snowflake_session
            )
            _streamlit_test_steps.verify_tag_not_set(
                entity_id, tag_a, snowflake_session
            )
            _streamlit_test_steps.verify_tag_not_set(
                entity_id, tag_b, snowflake_session
            )

            _streamlit_test_steps.drop_should_succeed(entity_id, snowflake_session)
    finally:
        sql_helper.execute_single_sql(f"DROP TAG IF EXISTS {tag_a}")
        sql_helper.execute_single_sql(f"DROP TAG IF EXISTS {tag_b}")


@pytest.mark.integration
def test_streamlit_redeploy_does_not_unset_inherited_schema_tags(
    _streamlit_test_steps,
    project_directory,
    snowflake_session,
):
    """
    Regression test for https://github.com/snowflakedb/snowflake-cli/issues/3142

    When the schema carries governance tags inherited by the Streamlit object,
    re-deploy must not attempt to UNSET those tags and must not destroy the
    inherited tag association.

    The governance tag lives in a separate schema so it can't be resolved by its
    short name in the app schema, reproducing the exact failure condition.
    """
    entity_id = "app_1"
    sql = _streamlit_test_steps.setup.sql_test_helper.execute_single_sql
    runner = _streamlit_test_steps.setup.runner
    db = snowflake_session.database
    # Capture schema before CREATE SCHEMA below implicitly switches the session schema.
    schema = snowflake_session.schema

    gov_schema = "cli_test_gov_schema"
    gov_tag = "cli_test_schema_level_tag"

    def get_gov_tag_value():
        rows = sql(
            f"SELECT SYSTEM$GET_TAG('{db}.{gov_schema}.{gov_tag}', "
            f"'{db}.{schema}.{entity_id}', 'STREAMLIT') AS v"
        )
        return rows[0]["V"]

    try:
        sql(f"CREATE SCHEMA IF NOT EXISTS {db}.{gov_schema}")
        sql(f"CREATE TAG IF NOT EXISTS {db}.{gov_schema}.{gov_tag}")
        # Apply the governance tag at the schema level — all objects in the
        # test schema inherit it, but the tag only resolves via its FQN.
        sql(
            f"ALTER SCHEMA {db}.{schema} "
            f"SET TAG {db}.{gov_schema}.{gov_tag} = 'governed'"
        )

        with project_directory("streamlit_v2"):
            # First deploy: creates the app.
            result = runner.invoke_with_connection_json(
                ["streamlit", "deploy", entity_id]
            )
            assert result.exit_code == 0, result.output

            # Governance tag must be inherited from schema immediately after deploy.
            assert (
                get_gov_tag_value() == "governed"
            ), "Governance tag should be inherited by the streamlit app from its schema"

            # Re-deploy: snowflake.yml has no `tags:` property, so _sync_tags
            # returns early without touching any tags.
            result = runner.invoke_with_connection_json(
                ["streamlit", "deploy", entity_id, "--replace"]
            )
            assert result.exit_code == 0, (
                "Re-deploy must succeed: _sync_tags should skip when tags "
                "property is absent in snowflake.yml\n" + result.output
            )

            # Governance tag must still be inherited after re-deploy.
            assert (
                get_gov_tag_value() == "governed"
            ), "Governance tag must still be inherited after re-deploy"
    finally:
        try:
            sql(f"ALTER SCHEMA {db}.{schema} UNSET TAG {db}.{gov_schema}.{gov_tag}")
        except Exception:
            pass
        sql(f"DROP TAG IF EXISTS {db}.{gov_schema}.{gov_tag}")
        sql(f"DROP SCHEMA IF EXISTS {db}.{gov_schema}")
        sql(f"DROP STREAMLIT IF EXISTS {entity_id}")


@pytest.mark.integration
def test_streamlit_grants_experimental_flow(
    _streamlit_test_steps,
    project_directory,
    snowflake_session,
    alter_snowflake_yml,
):
    """Test that streamlit grants are properly applied during experimental deployment."""
    test_role = snowflake_session.role
    entity_id = "app_1"

    with project_directory("streamlit_v2"):
        alter_snowflake_yml(
            "snowflake.yml",
            "entities.app_1.grants",
            [{"privilege": "USAGE", "role": test_role}],
        )

        _streamlit_test_steps.deploy_with_entity_id_specified_should_succeed(
            entity_id, snowflake_session
        )

        _streamlit_test_steps.verify_grants_applied(entity_id, test_role)

        _streamlit_test_steps.drop_should_succeed(entity_id, snowflake_session)
