from unittest import mock

import pytest
from snowflake.cli.api.identifiers import FQN

ProjectManager = "snowflake.cli._plugins.project.commands.ProjectManager"
ObjectManager = "snowflake.cli._plugins.project.commands.ObjectManager"
get_entity_for_operation = (
    "snowflake.cli._plugins.project.commands.get_entity_for_operation"
)


@pytest.fixture
def mock_project_exists():
    with mock.patch(
        "snowflake.cli._plugins.project.commands.ObjectManager.object_exists",
        return_value=True,
    ) as _fixture:
        yield _fixture


@mock.patch(ProjectManager)
@mock.patch(ObjectManager)
@pytest.mark.parametrize("no_version", [False, True])
def test_create(mock_om, mock_pm, runner, project_directory, no_version):
    mock_om().object_exists.return_value = False
    with project_directory("dcm_project"):
        command = ["project", "create"]
        if no_version:
            command.append("--no-version")
        result = runner.invoke(command)
        assert result.exit_code == 0, result.output

        mock_pm().create.assert_called_once()
        create_kwargs = mock_pm().create.mock_calls[0].kwargs
        assert create_kwargs["initialize_version_from_local_files"] == (not no_version)
        assert create_kwargs["project"].fqn == FQN.from_string("my_project")


@mock.patch(ProjectManager)
@pytest.mark.parametrize(
    "prune,_from,expected_prune_value",
    [
        (True, True, False),
        (True, False, True),
        (False, True, False),
        (False, False, False),
        (None, True, False),
        (None, False, True),
    ],
)
def test_add_version(
    mock_pm,
    runner,
    project_directory,
    prune,
    _from,
    expected_prune_value,
    mock_project_exists,
):
    with project_directory("dcm_project") as root:
        command = [
            "project",
            "add-version",
            "my_project",
            "--alias",
            "v1",
            "--comment",
            "fancy",
        ]
        if prune:
            command += ["--prune"]
        elif prune is False:
            command += ["--no-prune"]

        if _from:
            command += ["--from", "@stage"]
        result = runner.invoke(command)
        assert result.exit_code == 0, result.output
        assert not (root / "output").exists()

    assert mock_pm().add_version.call_count == 1
    kwargs = mock_pm().add_version.mock_calls[0].kwargs
    expected_kwargs = {
        "alias": "v1",
        "comment": "fancy",
        "project": kwargs["project"],
        "prune": expected_prune_value,
        "from_stage": "@stage" if _from else None,
    }

    assert expected_kwargs == kwargs


def test_add_version_raises_when_project_does_not_exist(
    runner, project_directory, mock_project_exists
):
    mock_project_exists.return_value = False
    with project_directory("dcm_project") as root:
        command = [
            "project",
            "add-version",
            "my_project",
            "--alias",
            "v1",
            "--comment",
            "fancy",
        ]
        result = runner.invoke(command)
        assert result.exit_code == 1, result.output
        assert (
            "Project 'my_project' does not exist. Use `project create` command first"
            in result.output
        )


@mock.patch(ProjectManager)
def test_execute_project(mock_pm, runner, project_directory):
    result = runner.invoke(["project", "execute", "fooBar"])
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version=None,
        variables=None,
    )


@mock.patch(ProjectManager)
def test_execute_project_with_variables(mock_pm, runner, project_directory):
    result = runner.invoke(
        ["project", "execute", "fooBar", "--version", "v1", "-D", "key=value"]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version="v1",
        variables=["key=value"],
    )


@mock.patch(ProjectManager)
def test_validate_project(mock_pm, runner, project_directory):
    result = runner.invoke(
        ["project", "dry-run", "fooBar", "--version", "v1", "-D", "key=value"]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version="v1",
        dry_run=True,
        variables=["key=value"],
    )


def test_list_command_alias(mock_connect, runner):
    result = runner.invoke(
        [
            "object",
            "list",
            "project",
            "--like",
            "%PROJECT_NAME%",
            "--in",
            "database",
            "my_db",
        ]
    )

    assert result.exit_code == 0, result.output
    result = runner.invoke(
        ["project", "list", "--like", "%PROJECT_NAME%", "--in", "database", "my_db"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    queries = mock_connect.mocked_ctx.get_queries()
    assert len(queries) == 2
    assert (
        queries[0]
        == queries[1]
        == "show projects like '%PROJECT_NAME%' in database my_db"
    )


@mock.patch(ProjectManager)
def test_list_versions(mock_pm, runner):
    result = runner.invoke(["project", "list-versions", "fooBar"])

    assert result.exit_code == 0, result.output

    mock_pm().list_versions.assert_called_once_with(
        project_name=FQN.from_string("fooBar")
    )


def test_drop_project(mock_connect, runner):
    result = runner.invoke(
        [
            "object",
            "drop",
            "project",
            "my_project",
        ]
    )

    assert result.exit_code == 0, result.output

    result = runner.invoke(
        ["project", "drop", "my_project"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    queries = mock_connect.mocked_ctx.get_queries()
    assert len(queries) == 2
    assert queries[0] == queries[1] == "drop project IDENTIFIER('my_project')"
