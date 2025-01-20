import pytest


@pytest.mark.integration
def test_deploy(runner, project_directory, test_database):
    with project_directory("notebooks_v2"):
        result = runner.invoke_with_connection_json(["notebook", "deploy", "notebook1"])
        assert result.exit_code == 0
        assert result.json == [
            {
                "object": "notebook1",
                "status": "CREATED",
            },
        ]

        # deploy both, --replace/--if-not-exists needed
        result = runner.invoke_with_connection(["notebook", "deploy"])
        assert result.exit_code == 1, result.output
        assert "Notebook notebook1 already exists. Consider using --replace."

        result = runner.invoke_with_connection_json(
            ["notebook", "deploy", "--if-not-exists"]
        )
        assert result.exit_code == 0, result.output
        assert result.json == [
            {
                "object": "notebook1",
                "status": "SKIPPED",
            },
            {
                "object": "notebook2",
                "status": "CREATED",
            },
        ]

        result = runner.invoke_with_connection_json(["notebook", "deploy", "--replace"])
        assert result.exit_code == 0, result.output
        assert result.json == [
            {
                "object": "notebook1",
                "status": "REPLACED",
            },
            {
                "object": "notebook2",
                "status": "REPLACED",
            },
        ]
