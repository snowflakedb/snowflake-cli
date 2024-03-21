from __future__ import annotations

import logging
from functools import wraps
from pathlib import Path

from snowflake.cli.api.constants import PACKAGES_DIR
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.object.stage.manager import StageManager
from snowflake.cli.plugins.snowpark.models import get_package_name
from snowflake.cli.plugins.snowpark.package.utils import prepare_app_zip
from snowflake.cli.plugins.snowpark.zipper import zip_dir

log = logging.getLogger(__name__)


def upload(file: Path, stage: str, overwrite: bool):
    log.info("Uploading %s to Snowflake @%s/%s...", file, stage, file)
    with SecurePath.temporary_directory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(SecurePath(file), temp_dir)
        sm = StageManager()

        sm.create(sm.get_stage_name_from_path(stage))
        put_response = sm.put(
            temp_app_zip_path.path, stage, overwrite=overwrite
        ).fetchone()

    message = f"Package {file} {put_response[6]} to Snowflake @{stage}/{file}."

    if put_response[6] == "SKIPPED":
        message = "Package already exists on stage. Consider using --overwrite to overwrite the file."

    return message


def create_packages_zip(zip_name: str) -> str:
    file_name = f"{get_package_name(zip_name)}.zip"
    zip_dir(dest_zip=Path(file_name), source=Path.cwd() / ".packages")
    return file_name


def cleanup_packages_dir(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        finally:
            if PACKAGES_DIR.exists():
                SecurePath(PACKAGES_DIR).rmdir(recursive=True)

    return wrapper
