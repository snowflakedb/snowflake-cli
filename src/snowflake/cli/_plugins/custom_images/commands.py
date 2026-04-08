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

from enum import Enum
from pathlib import Path
from typing import Optional

import typer
from click import ClickException
from snowflake.cli._plugins.custom_images.manager import CustomImageManager
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult


class BaseImageType(str, Enum):
    cpu = "cpu"
    gpu = "gpu"


CONFIG_DIR = Path(__file__).parent / "config"
DEFAULT_CONFIG_PATH = CONFIG_DIR / "image_validation.yaml"


app = SnowTyperFactory(
    name="custom-image",
    help="Manages custom images for Snowpark Container Services.",
)


@app.callback()
def _callback():
    pass


@app.command(requires_connection=False)
def register(
    image: str = typer.Argument(
        ...,
        help="Local Docker image to push. Accepts image name (e.g., 'myimage:latest') or image ID/hash.",
    ),
    registry: str = typer.Argument(
        ...,
        help="Full destination registry reference including host (e.g., 'org-acct.registry.snowflakecomputing.com/db/schema/repo/image:tag').",
    ),
    skip_validation: bool = typer.Option(
        False,
        "--skip-validation",
        help="Skip custom image validation and only push the image to the registry.",
    ),
    base_image_type: Optional[BaseImageType] = typer.Option(
        None,
        "--base-image-type",
        help="Base image type for CRE creation (cpu or gpu). Required when --skip-validation is not set.",
    ),
    cre_name: Optional[str] = typer.Option(
        None,
        "--name",
        help="Name for the Custom Runtime Environment. Defaults to 'mlruntimes_<uuid8>' if not provided.",
    ),
    **options,
) -> CommandResult:
    """
    Pushes a local Docker image to an image registry.

    Without --skip-validation, also validates the image by creating a Custom Runtime Environment (CRE) in Snowflake.
    """
    manager = CustomImageManager(config_path=DEFAULT_CONFIG_PATH)
    message = manager.register(
        image=image,
        registry=registry,
        skip_validation=skip_validation,
        base_image_type=base_image_type.value if base_image_type else None,
        cre_name=cre_name,
    )
    return MessageResult(message)


@app.command(requires_connection=False)
def validate(
    image: str = typer.Argument(
        ...,
        help="Local Docker image to validate. Accepts image name (e.g., 'myimage:latest') or image ID/hash.",
    ),
    scan_vulnerabilities: bool = typer.Option(
        False,
        "--scan-vulnerabilities",
        help="Run vulnerability scan using Grype. Requires Grype to be installed.",
    ),
    **options,
) -> CommandResult:
    """
    Validates a Docker image against Snowflake custom image requirements.
    """
    manager = CustomImageManager(config_path=DEFAULT_CONFIG_PATH)
    report, output = manager.validate(
        image=image, scan_vulnerabilities=scan_vulnerabilities
    )

    if not report.all_passed:
        raise ClickException(
            f"Image validation failed with {report.failed_count} error(s).\n{output}"
        )

    return MessageResult(output)
