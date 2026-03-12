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

from pathlib import Path

import typer
from click import ClickException
from snowflake.cli._plugins.custom_images.manager import CustomImageManager
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult

CONFIG_DIR = Path(__file__).parent / "config"
DEFAULT_CONFIG_PATH = CONFIG_DIR / "image_validation.yaml"


app = SnowTyperFactory(
    name="custom-image",
    help="Manages custom images for Snowpark Container Services.",
)


@app.command(requires_connection=False)
def validate(
    image: str = typer.Argument(
        ...,
        help="Local Docker image to validate. Accepts image name (e.g., 'myimage:latest') or image ID/hash.",
    ),
    image_type: str = typer.Option(
        "cpu",
        "--image-type",
        help="Base image type: 'cpu' or 'gpu'. Defaults to 'cpu'.",
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
    image_type_lower = image_type.lower()
    if image_type_lower not in ("cpu", "gpu"):
        raise ClickException(
            f"Invalid image type: {image_type}. Must be 'cpu' or 'gpu'."
        )

    is_gpu = image_type_lower == "gpu"
    manager = CustomImageManager(config_path=DEFAULT_CONFIG_PATH)
    report, output = manager.validate(
        image=image, is_gpu=is_gpu, scan_vulnerabilities=scan_vulnerabilities
    )

    if not report.all_passed:
        raise ClickException(
            f"Image validation failed with {report.failed_count} error(s).\n{output}"
        )

    return MessageResult(output)
