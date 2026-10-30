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
from snowflake.cli.api.commands.command_docs import (
    CommandDocs,
    Example,
    code,
    link,
    plain_text,
)
from snowflake.cli.api.commands.snow_typer import SnowTyperFactory
from snowflake.cli.api.output.types import CommandResult, MessageResult

CONFIG_DIR = Path(__file__).parent / "config"
DEFAULT_CONFIG_PATH = CONFIG_DIR / "image_validation.yaml"


app = SnowTyperFactory(
    name="custom-image",
    help="Manages custom images for Snowpark Container Services.",
)


@app.callback()
def _callback():
    pass


@app.command(
    requires_connection=False,
    docs=CommandDocs(
        related=(
            link("/developer-guide/snowflake-cli/index"),
            link(
                "/developer-guide/snowflake-cli/command-reference/overview",
                "Snowflake CLI command reference",
            ),
            link(
                "/developer-guide/snowflake-cli/command-reference/custom-image-commands/overview"
            ),
        ),
        usage_notes=(
            plain_text(
                "The ",
                code("snow custom-image validate"),
                " command validates a custom Docker image against the configured "
                "rules before you register it for use with Snowflake container "
                "services. The command checks the entrypoint configuration, required "
                "environment variables, installed Python packages, and dependency "
                "health.",
            ),
            plain_text(
                "Pass the ",
                code("--scan-vulnerabilities"),
                " flag to run Grype vulnerability scanning against the image as part "
                "of validation.",
            ),
        ),
        examples=(
            Example(
                description=plain_text("Validate a local Docker image:"),
                command="snow custom-image validate my-image:latest",
            ),
            Example(
                description=plain_text(
                    "Validate a local Docker image and scan it for vulnerabilities:"
                ),
                command=(
                    "snow custom-image validate my-image:latest --scan-vulnerabilities"
                ),
            ),
        ),
    ),
)
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
