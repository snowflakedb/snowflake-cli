from pathlib import Path

import pytest
from snowflake.connector import ProgrammingError


@pytest.mark.integration_experimental
def test_execute_notebook(runner, test_database, snowflake_session):
    # TODO: replace once there's option to create notebook from outside snowsight
    notebook_name = "notebooks.public.test_notebook"
    result = runner.invoke_with_connection_json(
        ["notebook", "execute", notebook_name, "--format", "json"]
    )
    assert result.exit_code == 0
    assert result.json == {"message": f"Notebook {notebook_name} executed."}


@pytest.mark.integration_experimental
def test_execute_notebook_failure(runner, test_database, snowflake_session):
    # TODO: replace once there's option to create notebook from outside snowsight
    notebook_name = "notebooks.public.test_notebook_error"
    with pytest.raises(ProgrammingError) as err:
        result = runner.invoke_with_connection_json(
            ["notebook", "execute", notebook_name, "--format", "json"]
        )
        assert result.exit_code == 1
        assert "invalid identifier 'FOO'" in err


@pytest.mark.integration_experimental
def test_create_notebook(runner, test_database, snowflake_session):
    notebook_name = "my_notebook"
    stage_name = "notebook_stage"
    local_notebook_file = (
        Path(__file__).parent.parent / "test_data/notebook/my_notebook.ipynb"
    )
    stage_path = f"@{stage_name}/{local_notebook_file.name}"

    snowflake_session.execute_string(
        f"create stage {stage_name};"
        f"put file://{local_notebook_file.absolute()} @{stage_name} AUTO_COMPRESS=FALSE;"
    )

    command = (
        "notebook",
        "create",
        notebook_name,
        "--notebook-file",
        stage_path,
        "--format",
        "json",
    )
    result = runner.invoke_with_connection_json(command)

    assert result.exit_code == 0

    message: str = result.json.get("message", "")
    assert message.startswith("https://app.snowflake.com/")
    assert message.endswith("MY_NOTEBOOK")
