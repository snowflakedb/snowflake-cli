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
    name="validate-custom-image",
    help="Validates a custom Docker image for Snowflake services.",
)


@app.command(requires_connection=False, name="validate-custom-image")
def validate_custom_image(
    image_hash: str = typer.Argument(
        ...,
        help="Local Docker image identifier (image ID / hash).",
    ),
    **options,
) -> CommandResult:
    """
    Validates a local Docker image to ensure it is Snowflake-ready.

    Checks:
      - Base image validation
      - Entrypoint configuration
      - Required environment variables
      - Required Python packages
      - Dependency health (pip check)
      - Vulnerability scan (grype)
    """
    manager = CustomImageManager(config_path=DEFAULT_CONFIG_PATH)
    report, output = manager.validate(image_hash=image_hash)

    if not report.all_passed:
        raise ClickException(
            f"Image validation failed with {report.failed_count} error(s).\n{output}"
        )

    return MessageResult(output)
