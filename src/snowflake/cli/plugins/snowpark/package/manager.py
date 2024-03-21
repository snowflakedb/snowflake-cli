from __future__ import annotations

import logging
from functools import wraps
from pathlib import Path

from snowflake.cli.api.constants import PACKAGES_DIR
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.object.stage.manager import StageManager
from snowflake.cli.plugins.snowpark import package_utils
from snowflake.cli.plugins.snowpark.models import (
    PypiOption,
    Requirement,
    SplitRequirements,
    get_package_name,
)
from snowflake.cli.plugins.snowpark.package.anaconda import AnacondaChannel
from snowflake.cli.plugins.snowpark.package.utils import (
    InAnaconda,
    LookupResult,
    NothingFound,
    NotInAnaconda,
    RequiresPackages,
    prepare_app_zip,
)
from snowflake.cli.plugins.snowpark.zipper import zip_dir

log = logging.getLogger(__name__)


def lookup(
    name: str,
    index_url: str | None,
    allow_shared_libraries: PypiOption,
    skip_version_check: bool,
    ignore_anaconda: bool,
) -> LookupResult:

    package = Requirement.parse(name)
    anaconda = None
    if ignore_anaconda:
        package_response = SplitRequirements([], other=[package])
    else:
        anaconda = AnacondaChannel.from_snowflake()
        package_response = anaconda.parse_anaconda_packages(
            packages=[package], skip_version_check=skip_version_check
        )

    if package_response.snowflake and not package_response.other:
        return InAnaconda(package_response, name)
    else:
        status, result = package_utils.download_packages(
            anaconda=anaconda,
            perform_anaconda_check=not ignore_anaconda,
            package_name=name,
            file_name=None,
            index_url=index_url,
            allow_shared_libraries=allow_shared_libraries,
            skip_version_check=skip_version_check,
        )

        if status:
            if result.snowflake:
                return RequiresPackages(result, name)
            else:
                return NotInAnaconda(result, name)

    return NothingFound(SplitRequirements([], []), name)


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


def cleanup_after_install(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        finally:
            if PACKAGES_DIR.exists():
                SecurePath(PACKAGES_DIR).rmdir(recursive=True)

    return wrapper
