# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
from pathlib import Path

from snowflake.cli._plugins.snowpark.package.utils import prepare_app_zip
from snowflake.cli._plugins.stage.manager import StageManager
from snowflake.cli.api.identifiers import FQN
from snowflake.cli.api.secure_path import SecurePath

log = logging.getLogger(__name__)


def upload(file: Path, stage: str, overwrite: bool):
    log.info("Uploading %s to Snowflake @%s/%s...", file, stage, file)
    with SecurePath.temporary_directory() as temp_dir:
        temp_app_zip_path = prepare_app_zip(SecurePath(file), temp_dir)
        sm = StageManager()

        sm.create(FQN.from_string(sm.get_stage_from_path(stage)))
        put_response = sm.put(
            temp_app_zip_path.path, stage, overwrite=overwrite
        ).fetchone()

    message = f"Package {file} {put_response[6]} to Snowflake @{stage}/{file}."

    if put_response[6] == "SKIPPED":
        message = "Package already exists on stage. Consider using --overwrite to overwrite the file."

    return message
