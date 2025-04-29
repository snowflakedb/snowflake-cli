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

from subprocess import run

from click import ClickException


def subprocess_run(command, *args, capture_output=True, text=True, **kwargs) -> str:
    result = run(command, *args, capture_output=capture_output, text=text, **kwargs)
    if result.returncode != 0:
        raise ClickException(
            f"""Command '{command}' finished with non-zero exit code: {result.returncode}
            ----- stdout -----
            {result.stdout}
            ===== stderr =====
            {result.stderr}
            """
        )
    return result.stdout
