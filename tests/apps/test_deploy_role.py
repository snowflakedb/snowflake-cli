# Copyright (c) 2026 Snowflake Inc.
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

import re
from functools import partial
from textwrap import dedent
from unittest.mock import patch

import pytest
from snowflake.cli._plugins.apps.app_yml import APP_YML_FILENAME
from snowflake.cli._plugins.apps.deploy_role import (
    DeployRoleRequest,
    RoleRequest,
    check_deploy_role,
    deploy_role_request,
    entity_role_request,
    other_owners_warning,
)
from snowflake.cli._plugins.apps.manager import SnowflakeAppManager
from snowflake.cli.api.cli_global_context import get_cli_context_manager
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.identifiers import FQN
from snowflake.connector.errors import ProgrammingError

from tests_common import change_directory

_COMMANDS = "snowflake.cli._plugins.apps.commands"
_MANAGER = "snowflake.cli._plugins.apps.manager"

_SERVICE = FQN(database="APPS", schema="PUBLIC", name="MY_APP")
_REPO = FQN(database="APPS", schema="PUBLIC", name="MY_APP_REPO")

_APP_YML = dedent(
    """\
    version: 2
    name: MY_APP
    database: APPS
    schema: PUBLIC
    query_warehouse: WH
    """
)

_SNOWFLAKE_YML_WITH_META_ROLE = dedent(
    """\
    definition_version: '2'
    entities:
      my_app:
        type: snowflake-app
        identifier: my_app
        meta:
          role: OWNER_RL
        artifacts:
          - src: "*"
            dest: ./
    """
)

_ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_PANEL_BORDER = re.compile(r"[|\u2502\u2503\u250c-\u257f]")


def _unwrapped(output: str) -> str:
    """Flatten Rich's output so an assertion does not depend on the terminal.

    Rich wraps the error panel to the terminal width and highlights tokens
    such as ``--role`` when it thinks the terminal supports colour.
    """
    plain = _ANSI_ESCAPE.sub("", output)
    return " ".join(_PANEL_BORDER.sub(" ", plain).split())


def _connection(**fields):
    get_cli_context_manager().connection_context.update(**fields)


class TestDeployRoleRequest:
    def test_role_flag(self):
        _connection(connection_name="default", role="flag_role")

        request = deploy_role_request()

        assert request.requests == (RoleRequest(role="flag_role", source="--role"),)
        assert not request.reuses_session

    def test_role_from_connection_configuration(self):
        _connection(connection_name="default")

        request = deploy_role_request()

        assert request.requests == (
            RoleRequest(role="test_role", source="connection 'default'"),
        )

    def test_connection_role_wins_over_snowflake_role(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_ROLE", "env_role")
        _connection(connection_name="default")

        request = deploy_role_request()

        assert request.requests == (
            RoleRequest(role="test_role", source="connection 'default'"),
        )

    def test_snowflake_role_when_connection_sets_no_role(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_ROLE", "env_role")
        _connection(connection_name="empty")

        request = deploy_role_request()

        assert request.requests == (
            RoleRequest(role="env_role", source="SNOWFLAKE_ROLE"),
        )

    def test_snowflake_role_for_temporary_connection(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_ROLE", "env_role")
        _connection(temporary_connection=True)

        request = deploy_role_request()

        assert request.requests == (
            RoleRequest(role="env_role", source="SNOWFLAKE_ROLE"),
        )

    def test_no_role_requested(self):
        _connection(connection_name="empty")

        assert deploy_role_request().requests == ()

    def test_entity_role_comes_first(self):
        _connection(connection_name="default", role="flag_role")

        request = deploy_role_request(entity_role_request("my_app", "meta_role"))

        assert request.requests == (
            RoleRequest(
                role="meta_role",
                source="meta.role of entity 'my_app'",
                from_entity=True,
            ),
            RoleRequest(role="flag_role", source="--role"),
        )

    def test_entity_without_role_requests_nothing(self):
        assert entity_role_request("my_app", None) is None

    @pytest.mark.parametrize(
        "session_variable, master_variable",
        [
            (
                "SNOWFLAKE_CONNECTIONS_DEFAULT_SESSION_TOKEN",
                "SNOWFLAKE_CONNECTIONS_DEFAULT_MASTER_TOKEN",
            ),
            ("SNOWFLAKE_SESSION_TOKEN", "SNOWFLAKE_MASTER_TOKEN"),
        ],
    )
    def test_tokens_reuse_the_session(
        self, monkeypatch, session_variable, master_variable
    ):
        monkeypatch.setenv(session_variable, "session")
        monkeypatch.setenv(master_variable, "master")
        _connection(connection_name="default")

        assert deploy_role_request().reuses_session

    def test_token_flags_reuse_the_session(self):
        _connection(
            temporary_connection=True, session_token="session", master_token="master"
        )

        assert deploy_role_request().reuses_session

    def test_session_token_alone_does_not_reuse_the_session(self, monkeypatch):
        monkeypatch.setenv("SNOWFLAKE_SESSION_TOKEN", "session")
        _connection(connection_name="default")

        assert not deploy_role_request().reuses_session


class TestCheckDeployRole:
    @pytest.mark.parametrize(
        "requested, active",
        [("engineer", "ENGINEER"), ("ENGINEER", "ENGINEER"), ('"My Role"', "My Role")],
    )
    def test_requested_role_is_active(self, requested, active):
        check_deploy_role(
            active,
            DeployRoleRequest(
                requests=(RoleRequest(role=requested, source="--role"),),
                reuses_session=False,
            ),
        )

    def test_nothing_requested(self):
        check_deploy_role(
            "ENGINEER", DeployRoleRequest(requests=(), reuses_session=True)
        )

    def test_role_flag_on_a_reused_session(self):
        with pytest.raises(CliError) as error:
            check_deploy_role(
                "ENGINEER",
                DeployRoleRequest(
                    requests=(RoleRequest(role="owner_rl", source="--role"),),
                    reuses_session=True,
                ),
            )

        assert error.value.message == (
            "Role OWNER_RL from --role is not active: the connection reuses an "
            "existing session through a session token and master token, and that "
            "session runs as role ENGINEER. Objects this deploy creates would be "
            "owned by ENGINEER. To deploy as OWNER_RL, connect without a session "
            "token and master token. To deploy as ENGINEER, pass --role ENGINEER."
        )

    def test_connection_role_that_did_not_apply(self):
        with pytest.raises(CliError) as error:
            check_deploy_role(
                "ENGINEER",
                DeployRoleRequest(
                    requests=(
                        RoleRequest(role="OWNER_RL", source="connection 'snowhouse'"),
                    ),
                    reuses_session=False,
                ),
            )

        assert error.value.message == (
            "Role OWNER_RL from connection 'snowhouse' is not active: the session "
            "runs as role ENGINEER. Objects this deploy creates would be owned by "
            "ENGINEER. Check that the connection signs in with role OWNER_RL. To "
            "deploy as ENGINEER, pass --role ENGINEER."
        )

    def test_entity_role(self):
        with pytest.raises(CliError) as error:
            check_deploy_role(
                "ENGINEER",
                DeployRoleRequest(
                    requests=(entity_role_request("my_app", "OWNER_RL"),),
                    reuses_session=False,
                ),
            )

        assert error.value.message == (
            "Role OWNER_RL from meta.role of entity 'my_app' is not active: the "
            "session runs as role ENGINEER. Objects this deploy creates would be "
            "owned by ENGINEER. To deploy as OWNER_RL, pass --role OWNER_RL: a "
            "Snowflake App Runtime deploy does not switch to the role in meta.role. "
            "To deploy as ENGINEER, remove meta.role from the entity."
        )

    def test_entity_role_on_a_reused_session(self):
        with pytest.raises(CliError) as error:
            check_deploy_role(
                "ENGINEER",
                DeployRoleRequest(
                    requests=(entity_role_request("my_app", "OWNER_RL"),),
                    reuses_session=True,
                ),
            )

        assert (
            "To deploy as OWNER_RL, connect without a session token and master "
            "token and pass --role OWNER_RL. To deploy as ENGINEER, remove "
            "meta.role from the entity."
        ) in error.value.message

    def test_every_requested_role_is_checked(self):
        with pytest.raises(CliError, match="Role OTHER_RL from --role"):
            check_deploy_role(
                "OWNER_RL",
                DeployRoleRequest(
                    requests=(
                        entity_role_request("my_app", "OWNER_RL"),
                        RoleRequest(role="OTHER_RL", source="--role"),
                    ),
                    reuses_session=False,
                ),
            )


class TestOtherOwnersWarning:
    def test_objects_the_role_owns_or_that_do_not_exist(self):
        assert (
            other_owners_warning(
                "OWNER_RL",
                [
                    ("application service", _SERVICE, "OWNER_RL"),
                    ("artifact repository", _REPO, None),
                ],
            )
            is None
        )

    def test_one_object_owned_by_another_role(self):
        assert other_owners_warning(
            "OWNER_RL",
            [
                ("application service", _SERVICE, "ENGINEER"),
                ("artifact repository", _REPO, "OWNER_RL"),
            ],
        ) == (
            "The application service APPS.PUBLIC.MY_APP (owned by ENGINEER) already "
            "exists. Deploying as role OWNER_RL updates it in place, so it keeps its "
            "owner. To have OWNER_RL own it, drop it and deploy again."
        )

    def test_several_objects_owned_by_another_role(self):
        assert other_owners_warning(
            "OWNER_RL",
            [
                ("application service", _SERVICE, "ENGINEER"),
                ("artifact repository", _REPO, "ENGINEER"),
            ],
        ) == (
            "The application service APPS.PUBLIC.MY_APP (owned by ENGINEER) and the "
            "artifact repository APPS.PUBLIC.MY_APP_REPO (owned by ENGINEER) already "
            "exist. Deploying as role OWNER_RL updates them in place, so they keep "
            "their owner. To have OWNER_RL own them, drop them and deploy again."
        )


class TestManagerDeployRole:
    @patch(f"{_MANAGER}.cli_console")
    def test_reports_the_role(self, mock_console):
        manager = SnowflakeAppManager()
        with patch.object(manager, "current_role", return_value="ENGINEER"):
            role = manager.ensure_deploy_role(
                DeployRoleRequest(
                    requests=(RoleRequest(role="engineer", source="--role"),),
                    reuses_session=False,
                )
            )

        assert role == "ENGINEER"
        mock_console.step.assert_called_once_with("Deploying as role ENGINEER.")

    @patch(f"{_MANAGER}.cli_console")
    def test_refuses_before_reporting(self, mock_console):
        manager = SnowflakeAppManager()
        with patch.object(manager, "current_role", return_value="ENGINEER"):
            with pytest.raises(CliError, match="Role OWNER_RL from --role"):
                manager.ensure_deploy_role(
                    DeployRoleRequest(
                        requests=(RoleRequest(role="OWNER_RL", source="--role"),),
                        reuses_session=True,
                    )
                )

        mock_console.step.assert_not_called()

    @patch(f"{_MANAGER}.cli_console")
    def test_unknown_role_is_not_checked(self, mock_console):
        manager = SnowflakeAppManager()
        with patch.object(manager, "current_role", return_value=None):
            role = manager.ensure_deploy_role(
                DeployRoleRequest(
                    requests=(RoleRequest(role="OWNER_RL", source="--role"),),
                    reuses_session=True,
                )
            )

        assert role is None
        mock_console.step.assert_not_called()

    def test_object_owner(self):
        manager = SnowflakeAppManager()
        with patch.object(
            manager,
            "execute_query",
            return_value=[
                {"name": "MY_APP_2", "owner": "OTHER_RL"},
                {"name": "MY_APP", "owner": "ENGINEER"},
            ],
        ) as mock_execute:
            owner = manager.object_owner("APPLICATION SERVICES", _SERVICE)

        assert owner == "ENGINEER"
        query = mock_execute.call_args.args[0]
        assert query.startswith("SHOW APPLICATION SERVICES LIKE ")
        assert query.endswith(" IN SCHEMA APPS.PUBLIC")

    def test_object_owner_of_missing_object(self):
        manager = SnowflakeAppManager()
        with patch.object(manager, "execute_query", return_value=[]):
            assert manager.object_owner("ARTIFACT REPOSITORIES", _REPO) is None

    def test_object_owner_when_show_fails(self):
        manager = SnowflakeAppManager()
        with patch.object(
            manager, "execute_query", side_effect=ProgrammingError("no access")
        ):
            assert manager.object_owner("ARTIFACT REPOSITORIES", _REPO) is None

    @patch(f"{_MANAGER}.cli_console")
    def test_warns_about_objects_owned_by_other_roles(self, mock_console):
        manager = SnowflakeAppManager()
        with patch.object(manager, "object_owner", return_value="ENGINEER"):
            manager.warn_about_objects_owned_by_other_roles(
                "OWNER_RL", service_fqn=_SERVICE, artifact_repo_fqn=_REPO
            )

        mock_console.warning.assert_called_once_with(
            other_owners_warning(
                "OWNER_RL",
                [
                    ("application service", _SERVICE, "ENGINEER"),
                    ("artifact repository", _REPO, "ENGINEER"),
                ],
            )
        )

    @patch(f"{_MANAGER}.cli_console")
    def test_no_warning_for_objects_the_role_owns(self, mock_console):
        manager = SnowflakeAppManager()
        with patch.object(manager, "object_owner", return_value="OWNER_RL"):
            manager.warn_about_objects_owned_by_other_roles(
                "OWNER_RL", service_fqn=_SERVICE, artifact_repo_fqn=_REPO
            )

        mock_console.warning.assert_not_called()


def _deploy_manager(mock_manager_cls, owner: str):
    """A mocked manager whose role check and ownership warning run for real."""
    manager = mock_manager_cls.return_value
    manager.current_role.return_value = "TEST_ROLE"
    manager.object_owner.return_value = owner
    manager.ensure_deploy_role.side_effect = partial(
        SnowflakeAppManager.ensure_deploy_role, manager
    )
    manager.warn_about_objects_owned_by_other_roles.side_effect = partial(
        SnowflakeAppManager.warn_about_objects_owned_by_other_roles, manager
    )
    manager.build_service_specification.side_effect = (
        SnowflakeAppManager.build_service_specification
    )
    manager.resolve_application_service_url_from_describe.side_effect = (
        SnowflakeAppManager().resolve_application_service_url_from_describe
    )
    return manager


class TestDeployRoleEndToEnd:
    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    def test_deploy_stops_before_creating_anything(
        self, mock_manager_cls, mock_poll, runner, tmp_path
    ):
        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        manager = _deploy_manager(mock_manager_cls, owner="TEST_ROLE")

        with change_directory(tmp_path):
            result = runner.invoke(
                ["app", "deploy", "--promote-only", "--role", "OWNER_RL"]
            )

        assert result.exit_code == 1, result.output
        assert (
            "Role OWNER_RL from --role is not active: the session runs as role "
            "TEST_ROLE." in _unwrapped(result.output)
        )
        manager.create_schema_if_not_exists.assert_not_called()
        manager.create_or_alter_app_service.assert_not_called()

    @patch(f"{_COMMANDS}._poll_until")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    def test_deploy_reports_role_and_other_owners(
        self, mock_manager_cls, mock_poll, runner, tmp_path
    ):
        (tmp_path / APP_YML_FILENAME).write_text(_APP_YML)
        manager = _deploy_manager(mock_manager_cls, owner="ENGINEER")
        mock_poll.return_value = {
            "url": "my-app.snowflakecomputing.app",
            "is_upgrading": "false",
        }

        with change_directory(tmp_path):
            result = runner.invoke(["app", "deploy", "--promote-only"])

        assert result.exit_code == 0, result.output
        output = _unwrapped(result.output)
        assert "Deploying as role TEST_ROLE." in output
        assert (
            "The application service APPS.PUBLIC.MY_APP (owned by ENGINEER) and the "
            "artifact repository APPS.PUBLIC.MY_APP_REPO (owned by ENGINEER) already "
            "exist. Deploying as role TEST_ROLE updates them in place" in output
        )
        manager.create_or_alter_app_service.assert_called_once()

    @patch(f"{_COMMANDS}._resolve_deploy_defaults")
    @patch(f"{_COMMANDS}.SnowflakeAppManager")
    def test_snowflake_yml_meta_role(
        self, mock_manager_cls, mock_defaults, runner, tmp_path
    ):
        (tmp_path / "snowflake.yml").write_text(_SNOWFLAKE_YML_WITH_META_ROLE)
        manager = _deploy_manager(mock_manager_cls, owner="TEST_ROLE")

        with change_directory(tmp_path):
            result = runner.invoke(["app", "deploy", "--promote-only"])

        assert result.exit_code == 1, result.output
        assert (
            "Role OWNER_RL from meta.role of entity 'my_app' is not active: the "
            "session runs as role TEST_ROLE." in _unwrapped(result.output)
        )
        mock_defaults.assert_not_called()
        manager.create_app_service.assert_not_called()
