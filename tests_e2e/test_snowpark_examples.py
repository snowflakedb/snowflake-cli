import subprocess
import time

import pytest


@pytest.mark.e2e
def test_snowpark_examples_functions_work_locally(snowcli):
    project_name = str(time.monotonic_ns())
    subprocess.check_output(
        [snowcli, "snowpark", "init", project_name],
        encoding="utf-8",
    )

    python = snowcli.parent / "python"

    output = subprocess.check_output(
        [python, f"{project_name}/app/functions.py", "FooBar"],
        encoding="utf-8",
    )
    assert output.strip() == "Hello FooBar!"

    output = subprocess.check_output(
        [python, f"{project_name}/app/procedures.py", "BazBar"],
        encoding="utf-8",
    )
    assert output.strip() == "Hello BazBar!"
