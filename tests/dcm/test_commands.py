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

    result = runner.invoke(["dcm", "deploy", "fooBar", "--from", "@my_stage"])
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        configuration=None,
        from_stage="@my_stage",
        variables=None,
        alias=None,
        output_path=None,
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
        configuration=None,
        from_stage="@my_stage",
        variables=None,
        alias=None,
        output_path=None,
    )


@mock.patch(DCMProjectManager)
def test_deploy_project_with_variables(mock_pm, runner, project_directory, mock_cursor):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(
        ["dcm", "deploy", "fooBar", "--from", "@my_stage", "-D", "key=value"]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        configuration=None,
        from_stage="@my_stage",
        variables=["key=value"],
        alias=None,
        output_path=None,
    )


@mock.patch(DCMProjectManager)
def test_deploy_project_with_configuration(
    mock_pm, runner, project_directory, mock_cursor
):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(
        [
            "dcm",
            "deploy",
            "fooBar",
            "--from",
            "@my_stage",
            "--configuration",
            "some_configuration",
        ]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        configuration="some_configuration",
        from_stage="@my_stage",
        variables=None,
        alias=None,
        output_path=None,
    )


@mock.patch(DCMProjectManager)
def test_deploy_project_with_alias(mock_pm, runner, project_directory, mock_cursor):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(
        ["dcm", "deploy", "fooBar", "--from", "@my_stage", "--alias", "my_alias"]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        configuration=None,
        from_stage="@my_stage",
        variables=None,
        alias="my_alias",
        output_path=None,
    )


@mock.patch(DCMProjectManager)
def test_plan_project(mock_pm, runner, project_directory, mock_cursor):
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
        configuration="some_configuration",
        from_stage="@my_stage",
        dry_run=True,
        variables=["key=value"],
        output_path=None,
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
        configuration="some_configuration",
        from_stage="@my_stage",
        dry_run=True,
        variables=["key=value"],
        output_path=None,
    )


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


@pytest.mark.parametrize(
    "terse, limit, expected_query_suffix",
    [
        (True, None, "show terse DCM Projects like '%%'"),
        (False, 10, "show DCM Projects like '%%' limit 10"),
        (False, 5, "show DCM Projects like '%%' limit 5"),
        (True, 10, "show terse DCM Projects like '%%' limit 10"),
    ],
)
def test_dcm_list_with_terse_and_limit_options(
    mock_connect, terse, limit, expected_query_suffix, runner
):
    """Test DCM list command with TERSE and LIMIT options."""
    cmd = ["dcm", "list"]

    if terse:
        cmd.extend(["--terse"])
    if limit is not None:
        cmd.extend(["--limit", str(limit)])

    result = runner.invoke(cmd, catch_exceptions=False)
    assert result.exit_code == 0, result.output

    queries = mock_connect.mocked_ctx.get_queries()
    assert len(queries) == 1
    assert queries[0] == expected_query_suffix


def test_dcm_list_with_all_options_combined(mock_connect, runner):
    """Test DCM list command with all options (like, scope, terse, limit) combined."""
    result = runner.invoke(
        [
            "dcm",
            "list",
            "--like",
            "test%",
            "--in",
            "database",
            "my_db",
            "--terse",
            "--limit",
            "20",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output

    queries = mock_connect.mocked_ctx.get_queries()
    assert len(queries) == 1
    expected_query = "show terse DCM Projects like 'test%' in database my_db limit 20"
    assert queries[0] == expected_query


@mock.patch(DCMProjectManager)
def test_list_deployments(mock_pm, runner):
    result = runner.invoke(["dcm", "list-deployments", "fooBar"])

    assert result.exit_code == 0, result.output

    mock_pm().list_versions.assert_called_once_with(
        project_name=FQN.from_string("fooBar")
    )


@mock.patch(DCMProjectManager)
@pytest.mark.parametrize("if_exists", [True, False])
def test_drop_version(mock_pm, runner, if_exists):
    command = ["dcm", "drop-deployment", "fooBar", "v1"]
    if if_exists:
        command.append("--if-exists")

    result = runner.invoke(command)

    assert result.exit_code == 0, result.output
    assert "Version 'v1' dropped from DCM Project 'fooBar'" in result.output

    mock_pm().drop_deployment.assert_called_once_with(
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
    result = runner.invoke(["dcm", "drop-deployment", "fooBar", version_name])

    assert result.exit_code == 0, result.output

    if should_warn:
        assert "might be truncated due to shell expansion" in result.output
        assert "try using single quotes" in result.output
    else:
        assert "might be truncated due to shell expansion" not in result.output

    mock_pm().drop_deployment.assert_called_once_with(
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


@mock.patch("snowflake.cli._plugins.dcm.commands.sync_artifacts_with_stage")
@mock.patch(DCMProjectManager)
def test_deploy_project_with_sync(
    mock_pm, mock_sync, runner, project_directory, mock_cursor
):
    """Test that files are synced to project stage when from_stage is not provided and project definition exists."""
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    with project_directory("dcm_project"):
        result = runner.invoke(["dcm", "deploy", "my_project"])
        assert result.exit_code == 0, result.output

        # Verify that sync was called
        mock_sync.assert_called_once()

        # Verify that execute was called with the project's stage as from_stage
        call_args = mock_pm().execute.call_args
        assert call_args is not None
        # Since files were synced to the project's stage, that stage should be used as from_stage
        assert call_args.kwargs["project_name"].identifier == "my_project"
        assert call_args.kwargs["from_stage"] == "my_project_stage"


@mock.patch("snowflake.cli._plugins.dcm.commands.sync_artifacts_with_stage")
@mock.patch(DCMProjectManager)
def test_plan_project_with_sync(
    mock_pm, mock_sync, runner, project_directory, mock_cursor
):
    """Test that files are synced to project stage when from_stage is not provided and project definition exists."""
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    with project_directory("dcm_project"):
        result = runner.invoke(["dcm", "plan", "my_project"])
        assert result.exit_code == 0, result.output

        # Verify that sync was called
        mock_sync.assert_called_once()

        # Verify that execute was called with the project's stage as from_stage
        call_args = mock_pm().execute.call_args
        assert call_args is not None
        # Since files were synced to the project's stage, that stage should be used as from_stage
        assert call_args.kwargs["project_name"].identifier == "my_project"
        assert call_args.kwargs["from_stage"] == "my_project_stage"


@mock.patch("snowflake.cli._plugins.dcm.commands.sync_artifacts_with_stage")
@mock.patch(DCMProjectManager)
def test_deploy_project_with_prune(
    mock_pm, mock_sync, runner, project_directory, mock_cursor
):
    """Test that prune flag is passed to sync_artifacts_with_stage when --prune is used."""
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    with project_directory("dcm_project"):
        result = runner.invoke(["dcm", "deploy", "my_project", "--prune"])
        assert result.exit_code == 0, result.output

        # Verify that sync was called with prune=True
        mock_sync.assert_called_once()
        call_args = mock_sync.call_args
        assert call_args.kwargs["prune"] is True


@mock.patch("snowflake.cli._plugins.dcm.commands.sync_artifacts_with_stage")
@mock.patch(DCMProjectManager)
def test_plan_project_with_prune(
    mock_pm, mock_sync, runner, project_directory, mock_cursor
):
    """Test that prune flag is passed to sync_artifacts_with_stage when --prune is used."""
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    with project_directory("dcm_project"):
        result = runner.invoke(["dcm", "plan", "my_project", "--prune"])
        assert result.exit_code == 0, result.output

        # Verify that sync was called with prune=True
        mock_sync.assert_called_once()
        call_args = mock_sync.call_args
        assert call_args.kwargs["prune"] is True


def test_deploy_prune_and_from_mutually_exclusive(runner):
    """Test that --prune and --from flags are mutually exclusive."""
    result = runner.invoke(
        ["dcm", "deploy", "my_project", "--prune", "--from", "@my_stage"]
    )
    assert result.exit_code != 0
    assert "are incompatible and cannot be used" in result.output


def test_plan_prune_and_from_mutually_exclusive(runner):
    """Test that --prune and --from flags are mutually exclusive."""
    result = runner.invoke(
        ["dcm", "plan", "my_project", "--prune", "--from", "@my_stage"]
    )
    assert result.exit_code != 0
    assert "are incompatible and cannot be used" in result.output


@mock.patch("snowflake.cli._plugins.dcm.commands.sync_artifacts_with_stage")
@mock.patch(DCMProjectManager)
def test_deploy_project_without_prune(
    mock_pm, mock_sync, runner, project_directory, mock_cursor
):
    """Test that prune defaults to False when --prune is not used."""
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    with project_directory("dcm_project"):
        result = runner.invoke(["dcm", "deploy", "my_project"])
        assert result.exit_code == 0, result.output

        # Verify that sync was called with prune=False (default)
        mock_sync.assert_called_once()
        call_args = mock_sync.call_args
        assert call_args.kwargs["prune"] is False


@mock.patch("snowflake.cli._plugins.dcm.commands.sync_artifacts_with_stage")
@mock.patch(DCMProjectManager)
def test_plan_project_without_prune(
    mock_pm, mock_sync, runner, project_directory, mock_cursor
):
    """Test that prune defaults to False when --prune is not used."""
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    with project_directory("dcm_project"):
        result = runner.invoke(["dcm", "plan", "my_project"])
        assert result.exit_code == 0, result.output

        # Verify that sync was called with prune=False (default)
        mock_sync.assert_called_once()
        call_args = mock_sync.call_args
        assert call_args.kwargs["prune"] is False


@mock.patch(DCMProjectManager)
def test_plan_project_with_output_path(mock_pm, runner, project_directory, mock_cursor):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(
        [
            "dcm",
            "plan",
            "fooBar",
            "--from",
            "@my_stage",
            "--output-path",
            "@output_stage/results",
        ]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        configuration=None,
        from_stage="@my_stage",
        dry_run=True,
        variables=None,
        output_path="@output_stage/results",
    )


@mock.patch(DCMProjectManager)
def test_plan_project_with_output_path_and_configuration(
    mock_pm, runner, project_directory, mock_cursor
):
    mock_pm().execute.return_value = mock_cursor(rows=[("[]",)], columns=("operations"))

    result = runner.invoke(
        [
            "dcm",
            "plan",
            "fooBar",
            "--from",
            "@my_stage",
            "--configuration",
            "some_config",
            "--output-path",
            "@output_stage",
        ]
    )
    assert result.exit_code == 0, result.output

    mock_pm().execute.assert_called_once_with(
        project_name=FQN.from_string("fooBar"),
        configuration="some_config",
        from_stage="@my_stage",
        dry_run=True,
        variables=None,
        output_path="@output_stage",
    )
