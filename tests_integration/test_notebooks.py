import pytest
from tests_integration.test_utils import assert_stage_has_files


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
    with project_directory("notebook_v2") as project_root:
        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 0, result.output
        assert (
            "Uploading artifacts to @custom_stage/particular_notebook_path\n"
            in result.output
        )
        assert "added:    notebook.ipynb -> notebook.ipynb" in result.output
        assert "Notebook successfully deployed and available under" in result.output
        assert_stage_has_files(
            runner,
            "custom_stage",
            ["custom_stage/particular_notebook_path/notebook.ipynb"],
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
        assert_stage_has_files(
            runner,
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
        assert_stage_has_files(
            runner,
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
        assert_stage_has_files(
            runner,
            "custom_stage",
            [
                "custom_stage/particular_notebook_path/notebook.ipynb",
            ],
        )
