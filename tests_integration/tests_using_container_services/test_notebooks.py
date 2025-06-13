import pytest


@pytest.mark.integration
def test_containerized_notebook(runner, project_directory, test_database):
    notebook_identifier = "containerized_notebook"

    # deploy notebook
    with project_directory("notebook_containerized_v2"):
        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 0, result.output
        assert (
            f"Uploading artifacts to @notebooks/{notebook_identifier}\n"
            in result.output
        )
        assert f"Notebook successfully deployed and available under" in result.output

        # execute notebook without exceptions
        result = runner.invoke_with_connection(
            ["notebook", "execute", notebook_identifier]
        )
        assert result.exit_code in [0, 1], result.output
        if result.exit_code == 0:
            assert result.output == "Notebook containerized_notebook executed.\n"
        else:
            assert "unschedulable in full compute pool." in result.output


@pytest.mark.integration
def test_containerized_notebook_incorrect_runtime_error(
    runner, project_directory, test_database, alter_snowflake_yml
):
    notebook_identifier = "containerized_notebook"
    with project_directory("notebook_containerized_v2") as project_directory:
        # readable error message should be returned when trying to create the notebook
        alter_snowflake_yml(
            project_directory / "snowflake.yml",
            f"entities.{notebook_identifier}.runtime_name",
            "not_existing_runtime_name",
        )
        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 1, result.output
        assert (
            "invalid value 'NOT_EXISTING_RUNTIME_NAME' for property 'RUNTIME_NAME'"
            in result.output
        )
