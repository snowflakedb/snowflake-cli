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

from snowflake.cli.__about__ import VERSION


def parse_version_for_windows_build() -> list[str]:
    """Convert sematntic version to windows installer acceptable version.

    Windows installer internal version is in the format of 4 integers separated by dots.
    """
    version = VERSION.split(".")
    *msv, last = version

    match last:
        case last if last.isdigit():
            version.append("0")
        case last if "rc" in last:
            version = msv + last.split("rc")
        case last if "dev" in last:
            version = msv + last.split("dev")

    return version


if __name__ == "__main__":
    version = parse_version_for_windows_build()
    print(".".join(version))
