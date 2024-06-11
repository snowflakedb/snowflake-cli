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

import stat
from pathlib import Path


def file_permissions_are_strict(file_path: Path) -> bool:
    accessible_by_others = (
        # https://docs.python.org/3/library/stat.html
        stat.S_IRGRP  # readable by group
        | stat.S_IROTH  # readable by others
        | stat.S_IWGRP  # writeable by group
        | stat.S_IWOTH  # writeable by others
        | stat.S_IXGRP  # executable by group
        | stat.S_IXOTH  # executable by others
    )
    return (file_path.stat().st_mode & accessible_by_others) == 0
