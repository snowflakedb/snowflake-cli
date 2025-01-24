import pytest


@pytest.mark.parametrize("notebook_id", ["notebook1", "notebook2"])
@pytest.mark.integration
def test_deploy_by_id(runner, project_directory, test_database, notebook_id):
    expected_output_prefix = (
        f"Deploying notebook {notebook_id}\n"
        f"  Creating stage notebooks if not exists\n"
        f"  Uploading {notebook_id}/my_notebook.ipynb to @notebooks/{notebook_id}\n"
        f"  Creating notebook\n"
    )

    with project_directory("notebooks_multiple_v2"):
        result = runner.invoke_with_connection(["notebook", "deploy", notebook_id])
        assert result.exit_code == 0, result.output
        assert result.output.startswith(expected_output_prefix)

        result = runner.invoke_with_connection(["notebook", "deploy", notebook_id])
        assert result.exit_code == 1, result.output
        assert (
            f"Notebook {notebook_id} already exists. Consider using --replace."
            in result.output
        )

        result = runner.invoke_with_connection(
            ["notebook", "deploy", notebook_id, "--replace"]
        )
        assert result.exit_code == 0, result.output
        assert result.output.startswith(expected_output_prefix)


@pytest.mark.integration
def test_deploy_single_notebook(runner, project_directory, test_database):
    expected_output_prefix = (
        "Deploying notebook my_notebook\n"
        "  Creating stage notebooks if not exists\n"
        "  Uploading notebook.ipynb to @custom_stage/particular_notebook_path\n"
        "  Creating notebook\n"
    )

    with project_directory("notebook_v2"):
        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 0, result.output
        assert result.output.startswith(expected_output_prefix)

        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 1, result.output
        assert (
            "Notebook my_notebook already exists. Consider using --replace."
            in result.output
        )

        result = runner.invoke_with_connection(["notebook", "deploy", "--replace"])
        assert result.exit_code == 0, result.output
        assert result.output.startswith(expected_output_prefix)
