import pytest


@pytest.mark.integration
def test_deploy_by_id(runner, project_directory, test_database):
    def expected_output_prefix(notebook_id):
        return (
            f"Uploading artifacts to @notebooks/{notebook_id}\n"
            f"  Creating stage notebooks if not exists\n"
            f"  Uploading artifacts\n"
            f"Creating notebook {notebook_id}\n"
            f"Notebook successfully deployed and available under"
        )

    with project_directory("notebooks_multiple_v2"):
        for notebook_id in ["notebook1", "notebook2"]:
            result = runner.invoke_with_connection(["notebook", "deploy", notebook_id])
            assert result.exit_code == 0, result.output
            assert result.output.startswith(expected_output_prefix(notebook_id))

        # default stage file paths should be different even though filenames are equal
        result = runner.invoke_with_connection_json(
            ["stage", "list-files", "notebooks"]
        )
        assert result.exit_code == 0, result.output
        file_paths = set(data["name"] for data in result.json)
        assert file_paths == {
            "notebooks/notebook1/notebook1/my_notebook.ipynb",
            "notebooks/notebook2/notebook2/my_notebook.ipynb",
        }


@pytest.mark.integration
def test_deploy_single_notebook(runner, project_directory, test_database):
    expected_output_prefix = (
        "Uploading artifacts to @custom_stage/particular_notebook_path\n"
        "  Creating stage custom_stage if not exists\n"
        "  Uploading artifacts\n"
        "Creating notebook custom_identifier\n"
        "Notebook successfully deployed and available under"
    )

    with project_directory("notebook_v2"):
        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 0, result.output
        assert result.output.startswith(expected_output_prefix)

        result = runner.invoke_with_connection_json(
            ["stage", "list-files", "custom_stage"]
        )
        assert result.exit_code == 0, result.output
        assert (
            result.json[0]["name"]
            == "custom_stage/particular_notebook_path/notebook.ipynb"
        )

        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 1, result.output
        assert (
            "Notebook custom_identifier already exists. Consider using --replace."
            in result.output
        )

        result = runner.invoke_with_connection(["notebook", "deploy", "--replace"])
        assert result.exit_code == 0, result.output
        assert result.output.startswith(expected_output_prefix)


@pytest.mark.integration
def test_containerized_notebook(runner, project_directory, test_database):
    notebook_identifier = "containerized_notebook"

    # deploy notebook
    with project_directory("notebook_containerized_v2"):
        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 0, result.output
        assert result.output.startswith(
            f"Uploading artifacts to @notebooks/{notebook_identifier}\n"
            f"  Creating stage notebooks if not exists\n"
            f"  Uploading artifacts\n"
            f"Creating notebook {notebook_identifier}\n"
            f"Notebook successfully deployed and available under"
        )

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
