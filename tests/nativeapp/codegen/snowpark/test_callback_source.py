import filecmp
import os
from pathlib import Path

from snowflake.cli.api.utils.rendering import jinja_render_from_file

from tests.testing_utils.files_and_dirs import create_named_file

with open(
    "./src/snowflake/cli/plugins/nativeapp/codegen/snowpark/callback_source.py.jinja",
    mode="r",
    encoding="utf-8",
) as udf_code:
    jinja_src = udf_code.read()

with open(
    "./tests/nativeapp/codegen/snowpark/expected_callback_source.py",
    mode="r",
    encoding="utf-8",
) as udf_code:
    rendered_src = udf_code.read()


def test_rendering(temp_dir):
    current_working_directory = os.getcwd()
    jinja_path = create_named_file(
        file_name="callback_source.py.jinja",
        dir_name=current_working_directory,
        contents=[jinja_src],
    )
    callback_path = create_named_file(
        file_name="callback_source.py",
        dir_name=current_working_directory,
        contents=[],
    )
    expected_path = create_named_file(
        file_name="expected_source.py",
        dir_name=current_working_directory,
        contents=[rendered_src],
    )

    jinja_render_from_file(
        template_path=Path(jinja_path),
        data={"py_file": "dummy_file.py"},
        output_file_path=Path(callback_path),
    )

    assert filecmp.cmp(callback_path, expected_path, shallow=False)
