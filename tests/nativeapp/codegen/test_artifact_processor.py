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

import pytest
from snowflake.cli._plugins.nativeapp.codegen.artifact_processor import (
    ProjectFileContextManager,
)

from tests.testing_utils.files_and_dirs import temp_local_dir
from tests_common import IS_WINDOWS

ORIGINAL_CONTENTS = "# This is the original contents"
EDITED_CONTENTS = "# This is the edited contents"


def test_project_file_context_manager():
    dir_contents = {"foo.txt": ORIGINAL_CONTENTS}
    with temp_local_dir(dir_contents) as root:
        foo_path = root / "foo.txt"
        with ProjectFileContextManager(foo_path) as cm:
            assert cm.contents == ORIGINAL_CONTENTS

            cm.edited_contents = EDITED_CONTENTS

        assert foo_path.read_text(encoding="utf-8") == EDITED_CONTENTS


@pytest.mark.skipif(
    IS_WINDOWS, reason="Symlinks on Windows are restricted to Developer mode or admins"
)
def test_project_file_context_manager_unlinks_symlinks():
    dir_contents = {"foo.txt": ORIGINAL_CONTENTS}
    with temp_local_dir(dir_contents) as root:
        foo_path = root / "foo.txt"
        foo_link_path = root / "foo_link.txt"
        foo_link_path.symlink_to(foo_path)

        assert foo_link_path.is_symlink()

        with ProjectFileContextManager(foo_link_path) as cm:
            assert cm.contents == ORIGINAL_CONTENTS

            cm.edited_contents = EDITED_CONTENTS

        assert foo_path.read_text(encoding="utf-8") == ORIGINAL_CONTENTS
        assert foo_link_path.read_text(encoding="utf-8") == EDITED_CONTENTS
        assert not foo_link_path.is_symlink()
