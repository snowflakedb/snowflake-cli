from __future__ import annotations

from snowflake.cli.api.secure_path import SecurePath


def prepare_app_zip(file_path: SecurePath, temp_dir: SecurePath) -> SecurePath:
    # get filename from file path (e.g. app.zip from /path/to/app.zip)
    # TODO: think if no file exceptions are handled correctly
    file_name = file_path.path.name
    temp_path = temp_dir / file_name
    file_path.copy(temp_path.path)
    return temp_path
