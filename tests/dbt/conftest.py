import os
from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dbt.constants import ENV_FILENAME, PROFILES_FILENAME
from snowflake.cli.api.feature_flags import FeatureFlag

from tests_common.feature_flag_utils import with_feature_flags


@pytest.fixture
def clean_dbt_env(monkeypatch):
    """Strip every DBT_* env var from os.environ for the duration of the test.

    monkeypatch.setenv only adds; it does NOT clear pre-existing ones. CI
    runners and dev shells often have stray DBT_* vars (DBT_LOG_PATH,
    DBT_TARGET_PATH, etc.) that would leak into --use-shell-env-vars
    behavior under test.
    """
    for key in list(os.environ):
        if key.startswith("DBT_"):
            monkeypatch.delenv(key, raising=False)
    yield monkeypatch


@pytest.fixture
def mock_validate_role():
    with mock.patch(
        "snowflake.cli._plugins.dbt.manager.DBTManager._validate_role",
        return_value=True,
    ) as _fixture:
        yield _fixture


@pytest.fixture
def enable_dbt_projects_profiles_file():
    with with_feature_flags(
        {FeatureFlag.ENABLE_DBT_PROJECT_PROFILES_FILE_PRECEDENCE: True}
    ):
        yield


@pytest.fixture()
def profile():
    profiles = {
        "dev": {
            "target": "local",
            "outputs": {
                "local": {
                    "database": "testdb",
                    "role": "test_role",
                    "schema": "test_schema",
                    "threads": 4,
                    "type": "snowflake",
                },
                "prod": {
                    "database": "testdb_prod",
                    "role": "test_role",
                    "schema": "test_schema",
                    "threads": 4,
                    "type": "snowflake",
                },
            },
        }
    }
    return profiles


@pytest.fixture()
def env_yml():
    return {
        "env_config": {
            "default_environment": "dev",
            "environments": [
                {"name": "dev", "env": {"DBT_FOO": "bar"}},
                {"name": "prod", "env": {"DBT_FOO": "baz"}},
            ],
        }
    }


@pytest.fixture
def project_path(tmp_path_factory):
    source_path = tmp_path_factory.mktemp("dbt_project")
    yield source_path


@pytest.fixture
def dbt_project_path(project_path, profile):
    dbt_project_file = project_path / "dbt_project.yml"
    dbt_project_file.write_text(yaml.dump({"profile": "dev"}))
    dbt_profiles_file = project_path / PROFILES_FILENAME
    dbt_profiles_file.write_text(yaml.dump(profile))
    yield project_path


@pytest.fixture
def env_yml_dir(tmp_path_factory, env_yml):
    env_path = tmp_path_factory.mktemp("dbt_envs")
    (env_path / ENV_FILENAME).write_text(yaml.dump(env_yml))
    yield env_path


@pytest.fixture
def mock_get_dbt_object_attributes():
    with mock.patch(
        "snowflake.cli._plugins.dbt.manager.DBTManager.get_dbt_object_attributes",
        return_value=None,  # None means object doesn't exist;
    ) as _fixture:
        yield _fixture


@pytest.fixture
def mock_from_resource():
    with mock.patch(
        "snowflake.cli._plugins.dbt.manager.FQN.from_resource",
        return_value="@MockDatabase.MockSchema.DBT_PROJECT_TEST_PIPELINE_1757333281_STAGE",
    ) as _fixture:
        yield _fixture


@pytest.fixture
def mock_execute_query():
    with mock.patch(
        "snowflake.cli._plugins.dbt.manager.DBTManager.execute_query"
    ) as _fixture:
        yield _fixture


@pytest.fixture
def mock_validate_dbt_version():
    with mock.patch(
        "snowflake.cli._plugins.dbt.manager.DBTManager._validate_dbt_version"
    ) as _fixture:
        yield _fixture
