from unittest import mock

import pytest
from snowflake.cli.api.identifiers import FQN

DCMProjectManager = "snowflake.cli._plugins.dcm.commands.DCMProjectManager"
ObjectManager = "snowflake.cli._plugins.dcm.commands.ObjectManager"
get_entity_for_operation = (
    "snowflake.cli._plugins.dcm.commands.get_entity_for_operation"
)


@pytest.fixture
def mock_project_exists():
    with mock.patch(
        "snowflake.cli._plugins.dcm.commands.ObjectManager.object_exists",
        return_value=True,
    ) as _fixture:
        yield _fixture


@mock.patch(DCMProjectManager)
@mock.patch(ObjectManager)
def test_create(mock_om, mock_pm, runner, project_directory):
    mock_om().object_exists.return_value = False
    with project_directory("dcm_project"):
        command = ["dcm", "create"]
        result = runner.invoke(command)
        assert result.exit_code == 0, result.output

        mock_pm().create.assert_called_once()
        create_kwargs = mock_pm().create.mock_calls[0].kwargs
        assert create_kwargs["project"].fqn == FQN.from_string("my_project")


@mock.patch(DCMProjectManager)
@mock.patch(ObjectManager)
@pytest.mark.parametrize("if_not_exists", [False, True])
def test_create_object_exists(
    mock_om, mock_pm, runner, project_directory, if_not_exists
):
    mock_om().object_exists.return_value = True
    with project_directory("dcm_project"):
        command = ["dcm", "create"]
        if if_not_exists:
            command.append("--if-not-exists")
        result = runner.invoke(command)
        if if_not_exists:
            assert result.exit_code == 0, result.output
            assert "DCM Project 'my_project' already exists." in result.output
        else:
            assert result.exit_code == 1, result.output

        mock_pm().create.assert_not_called()


@mock.patch(DCMProjectManager)
def test_deploy_project(mock_pm, runner, project_directory, mock_cursor):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(["dcm", "deploy", "fooBar"])
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version=None,
        from_stage=None,
        variables=None,
        configuration=None,
    )


@mock.patch(DCMProjectManager)
def test_deploy_project_with_from_stage(
    mock_pm, runner, project_directory, mock_cursor
):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(["dcm", "deploy", "fooBar", "--from", "@my_stage"])
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version=None,
        from_stage="@my_stage",
        variables=None,
        configuration=None,
    )


@mock.patch(DCMProjectManager)
def test_deploy_project_version_and_from_stage_mutually_exclusive(
    mock_pm, runner, project_directory, mock_cursor
):
    result = runner.invoke(
        ["dcm", "deploy", "fooBar", "--version", "v1", "--from", "@my_stage"]
    )
    assert result.exit_code == 1, result.output
    assert "--version and --from are mutually exclusive" in result.output

    mock_pm().execute.assert_not_called()


@mock.patch(DCMProjectManager)
def test_deploy_project_with_variables(mock_pm, runner, project_directory, mock_cursor):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(
        ["dcm", "deploy", "fooBar", "--version", "v1", "-D", "key=value"]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version="v1",
        from_stage=None,
        variables=["key=value"],
        configuration=None,
    )


@mock.patch(DCMProjectManager)
def test_deploy_project_with_configuration(
    mock_pm, runner, project_directory, mock_cursor
):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(
        ["dcm", "deploy", "fooBar", "--configuration", "some_configuration"]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        configuration="some_configuration",
        version=None,
        from_stage=None,
        variables=None,
    )


@mock.patch(DCMProjectManager)
def test_plan_project(mock_pm, runner, project_directory, mock_cursor):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(
        [
            "dcm",
            "plan",
            "fooBar",
            "--version",
            "v1",
            "-D",
            "key=value",
            "--configuration",
            "some_configuration",
        ]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version="v1",
        from_stage=None,
        dry_run=True,
        variables=["key=value"],
        configuration="some_configuration",
    )


@mock.patch(DCMProjectManager)
def test_plan_project_with_from_stage(mock_pm, runner, project_directory, mock_cursor):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(
        [
            "dcm",
            "plan",
            "fooBar",
            "--from",
            "@my_stage",
            "-D",
            "key=value",
            "--configuration",
            "some_configuration",
        ]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version=None,
        from_stage="@my_stage",
        dry_run=True,
        variables=["key=value"],
        configuration="some_configuration",
    )


@mock.patch(DCMProjectManager)
def test_plan_project_version_and_from_stage_mutually_exclusive(
    mock_pm, runner, project_directory, mock_cursor
):
    result = runner.invoke(
        [
            "dcm",
            "plan",
            "fooBar",
            "--version",
            "v1",
            "--from",
            "@my_stage",
        ]
    )
    assert result.exit_code == 1, result.output
    assert "--version and --from are mutually exclusive" in result.output

    mock_pm().execute.assert_not_called()


def test_list_command_alias(mock_connect, runner):
    result = runner.invoke(
        [
            "object",
            "list",
            "dcm",
            "--like",
            "%PROJECT_NAME%",
            "--in",
            "database",
            "my_db",
        ]
    )

    assert result.exit_code == 0, result.output
    result = runner.invoke(
        ["dcm", "list", "--like", "%PROJECT_NAME%", "--in", "database", "my_db"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    queries = mock_connect.mocked_ctx.get_queries()
    assert len(queries) == 2
    assert (
        queries[0]
        == queries[1]
        == "show DCM Projects like '%PROJECT_NAME%' in database my_db"
    )


@mock.patch(DCMProjectManager)
def test_list_versions(mock_pm, runner):
    result = runner.invoke(["dcm", "list-versions", "fooBar"])

    assert result.exit_code == 0, result.output

    mock_pm().list_versions.assert_called_once_with(
        project_name=FQN.from_string("fooBar")
    )


@mock.patch(DCMProjectManager)
@pytest.mark.parametrize("if_exists", [True, False])
def test_drop_version(mock_pm, runner, if_exists):
    command = ["dcm", "drop-version", "fooBar", "v1"]
    if if_exists:
        command.append("--if-exists")

    result = runner.invoke(command)

    assert result.exit_code == 0, result.output
    assert "Version 'v1' dropped from DCM Project 'fooBar'" in result.output

    mock_pm().drop_version.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version_name="v1",
        if_exists=if_exists,
    )


@mock.patch(DCMProjectManager)
@pytest.mark.parametrize(
    "version_name,should_warn",
    [
        ("version", True),
        ("VERSION", True),
        ("Version", True),
        ("VERSION$1", False),
        ("v1", False),
        ("my_version", False),
        ("version1", False),
        ("actual_version", False),
    ],
)
def test_drop_version_shell_expansion_warning(
    mock_pm, runner, version_name, should_warn
):
    """Test that warning is displayed for version names that look like shell expansion results."""
    result = runner.invoke(["dcm", "drop-version", "fooBar", version_name])

    assert result.exit_code == 0, result.output

    if should_warn:
        assert "might be truncated due to shell expansion" in result.output
        assert "try using single quotes" in result.output
    else:
        assert "might be truncated due to shell expansion" not in result.output

    mock_pm().drop_version.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        version_name=version_name,
        if_exists=False,
    )


def test_drop_project(mock_connect, runner):
    result = runner.invoke(
        [
            "object",
            "drop",
            "dcm",
            "my_project",
        ]
    )

    assert result.exit_code == 0, result.output

    result = runner.invoke(
        ["dcm", "drop", "my_project"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    queries = mock_connect.mocked_ctx.get_queries()
    assert len(queries) == 2
    assert queries[0] == queries[1] == "drop DCM Project IDENTIFIER('my_project')"


def test_describe_command_alias(mock_connect, runner):
    result = runner.invoke(
        [
            "object",
            "describe",
            "dcm",
            "PROJECT_NAME",
        ]
    )

    assert result.exit_code == 0, result.output
    result = runner.invoke(
        ["dcm", "describe", "PROJECT_NAME"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    queries = mock_connect.mocked_ctx.get_queries()
    assert len(queries) == 2
    assert queries[0] == queries[1] == "describe DCM Project IDENTIFIER('PROJECT_NAME')"
