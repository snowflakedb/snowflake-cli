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

from pathlib import Path, PurePath
from typing import Dict, List, Optional, Union

from snowflake import snowpark

# Constants for Container Service
DEFAULT_SERVER_PORT = 12020
DEFAULT_EXTENSIONS = ["ms-python.python", "ms-toolsai.jupyter"]
STARTUP_SCRIPT_PATH = PurePath("startup.sh")


class ContainerPayload:
    """
    Represents a Container Service payload that can be uploaded to a Snowflake stage.
    """

    def __init__(
        self,
        extensions: Optional[List[str]] = None,
        port: int = DEFAULT_SERVER_PORT,
    ):
        self.extensions = extensions if extensions else DEFAULT_EXTENSIONS
        self.port = port
        self._get_script_path()

    def _get_script_path(self) -> None:
        """Get the path to the startup script."""
        script_dir = Path(__file__).parent / "scripts"
        self.script_path = script_dir / "startup.sh"

        if not self.script_path.exists():
            raise FileNotFoundError(f"Startup script not found at {self.script_path}")

    def upload(
        self, session: snowpark.Session, stage_path: Union[str, PurePath]
    ) -> Dict[str, Union[PurePath, List[Union[str, PurePath]]]]:
        """Upload the container service payload to a stage."""
        # Convert string path to PurePath if necessary
        stage_path = PurePath(stage_path) if isinstance(stage_path, str) else stage_path

        # Create stage if necessary
        stage_name = stage_path.parts[0].lstrip("@")
        try:
            session.sql(f"describe stage {stage_name}").collect()
        except Exception:
            session.sql(
                f"create stage if not exists {stage_name}"
                " encryption = ( type = 'SNOWFLAKE_SSE' )"
                " comment = 'Created by snowflake.cli for Container Service'"
            ).collect()

        # Upload startup script directly from file
        session.file.put(
            str(self.script_path),
            stage_path.as_posix(),
            auto_compress=False,
            overwrite=True,
        )

        # Return the uploaded payload information
        return {
            "stage_path": stage_path,
            "entrypoint": ["bash", STARTUP_SCRIPT_PATH],
        }


def create_container_payload(
    extensions: Optional[List[str]] = None,
    port: int = DEFAULT_SERVER_PORT,
    startup_commands: Optional[List[str]] = None,
) -> ContainerPayload:
    """
    Create a container service payload.

    Args:
        extensions: List of VS Code extensions to install
        port: Port number for the container service
        startup_commands: Optional additional commands to run at startup (ignored - use environment variables instead)

    Returns:
        A ContainerPayload object
    """
    return ContainerPayload(extensions=extensions, port=port)
