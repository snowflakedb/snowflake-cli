from __future__ import annotations

import logging
from pathlib import Path

from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.snowpark.package.utils import prepare_app_zip
from snowflake.cli.plugins.stage.manager import StageManager

log = logging.getLogger(__name__)


def upload(file: Path, stage: str, overwrite: bool):
    log.info("Uploading %s to Snowflake @%s/%s...", file, stage, file)
    with SecurePath.temporary_directory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(SecurePath(file), temp_dir)
        sm = StageManager()

        sm.create(sm.get_stage_from_path(stage))
        put_response = sm.put(
            temp_app_zip_path.path, stage, overwrite=overwrite
        ).fetchone()

    message = f"Package {file} {put_response[6]} to Snowflake @{stage}/{file}."

    if put_response[6] == "SKIPPED":
        message = "Package already exists on stage. Consider using --overwrite to overwrite the file."

    return message
