from unittest import mock

import pytest
import yaml
from snowflake.cli._plugins.dbt.constants import PROFILES_FILENAME
from snowflake.cli.api.feature_flags import FeatureFlag


@pytest.fixture
def mock_validate_role():
    with mock.patch(
        "snowflake.cli._plugins.dbt.manager.DBTManager._validate_role",
        return_value=True,
    ) as _fixture:
        yield _fixture


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
    if FeatureFlag.ENABLE_DBT_GA_FEATURES.is_disabled():
        profiles["dev"]["outputs"]["local"]["account"] = "test_account"
        profiles["dev"]["outputs"]["local"]["user"] = "test_user"
        profiles["dev"]["outputs"]["local"]["warehouse"] = "test_wh"
        profiles["dev"]["outputs"]["prod"]["account"] = "test_account"
        profiles["dev"]["outputs"]["prod"]["user"] = "test_user"
        profiles["dev"]["outputs"]["prod"]["warehouse"] = "test_wh"
    return profiles


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
def mock_get_dbt_object_attributes():
    with mock.patch(
        "snowflake.cli._plugins.dbt.manager.DBTManager.get_dbt_object_attributes",
        return_value=None,
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
