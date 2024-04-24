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
