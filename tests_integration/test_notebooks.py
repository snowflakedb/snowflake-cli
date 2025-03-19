import pytest
from typing import List


@pytest.mark.integration
def test_deploy_by_id(runner, project_directory, test_database):
    with project_directory("notebooks_multiple_v2"):
        for notebook_id in ["notebook1", "notebook2"]:
            result = runner.invoke_with_connection(["notebook", "deploy", notebook_id])
            assert result.exit_code == 0, result.output
            assert f"Uploading artifacts to @notebooks/{notebook_id}" in result.output
            assert "Notebook successfully deployed and available under" in result.output

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
    def _assert_file_names_on_stage(stage: str, expected_files: List[str]) -> None:
        result = runner.invoke_with_connection_json(["stage", "list-files", stage])
        assert result.exit_code == 0, result.output
        assert set(file["name"] for file in result.json) == set(expected_files)

    with project_directory("notebook_v2") as project_root:
        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 0, result.output
        assert (
            "Uploading artifacts to @custom_stage/particular_notebook_path\n"
            in result.output
        )
        assert "added:    notebook.ipynb -> notebook.ipynb" in result.output
        assert "Notebook successfully deployed and available under" in result.output
        _assert_file_names_on_stage(
            "custom_stage", ["custom_stage/particular_notebook_path/notebook.ipynb"]
        )

        # upload file to stage to test --purge flag
        file = project_root / "unexpected.txt"
        file.write_text("this was unexpected")
        runner.invoke_with_connection(
            [
                "stage",
                "copy",
                "unexpected.txt",
                "@custom_stage/particular_notebook_path",
            ]
        )
        _assert_file_names_on_stage(
            "custom_stage",
            [
                "custom_stage/particular_notebook_path/notebook.ipynb",
                "custom_stage/particular_notebook_path/unexpected.txt",
            ],
        )

        # no --replace error
        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 1, result.output
        assert (
            "Notebook custom_identifier already exists. Consider using --replace."
            in result.output
        )

        # additional files should stay if --prune is not provided
        result = runner.invoke_with_connection(["notebook", "deploy", "--replace"])
        assert result.exit_code == 0, result.output
        assert "modified: notebook.ipynb -> notebook.ipynb" in result.output
        assert "Use the --prune flag to delete them from the stage." in result.output
        assert "Notebook successfully deployed and available under" in result.output
        _assert_file_names_on_stage(
            "custom_stage",
            [
                "custom_stage/particular_notebook_path/notebook.ipynb",
                "custom_stage/particular_notebook_path/unexpected.txt",
            ],
        )

        # --prune flag should delete additional files
        result = runner.invoke_with_connection(
            ["notebook", "deploy", "--replace", "--prune"]
        )
        assert result.exit_code == 0, result.output
        assert "Notebook successfully deployed and available under" in result.output
        _assert_file_names_on_stage(
            "custom_stage",
            [
                "custom_stage/particular_notebook_path/notebook.ipynb",
            ],
        )


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


@pytest.mark.qa_only
@pytest.mark.integration
def test_containerized_notebook_incorrect_runtime_error_qa(
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


@pytest.mark.no_qa
@pytest.mark.integration
def test_containerized_notebook_incorrect_runtime_error(
    runner, project_directory, test_database, alter_snowflake_yml
):
    notebook_identifier = "containerized_notebook"
    with project_directory("notebook_containerized_v2") as project_directory:
        alter_snowflake_yml(
            project_directory / "snowflake.yml",
            f"entities.{notebook_identifier}.runtime_name",
            "not_existing_runtime_name",
        )
        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 0, (
            "If this test fails here on github integration tests, "
            "remove this test and remove '@qa_only' mark from the test above"
        )

        # readable error message should be returned when trying to execute the notebook
        result = runner.invoke_with_connection(
            ["notebook", "execute", notebook_identifier]
        )
        assert result.exit_code == 1, result.output
        assert "Custom runtime" in result.output
        assert "runtimeName=NOT_EXISTING_RUNTIME_NAME is not supported" in result.output
