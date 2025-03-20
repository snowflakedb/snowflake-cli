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

import os
from pathlib import Path

import pytest
from snowflake.cli.api.artifacts.common import NotInDeployRootError
from snowflake.cli.api.artifacts.utils import symlink_or_copy

from tests.nativeapp.utils import assert_dir_snapshot, touch
from tests.testing_utils.files_and_dirs import temp_local_dir
from tests_common import IS_WINDOWS, change_directory


@pytest.mark.skipif(
    IS_WINDOWS, reason="Symlinks on Windows are restricted to Developer mode or admins"
)
def test_symlink_or_copy_raises_error(temporary_directory, os_agnostic_snapshot):
    touch("GrandA/ParentA/ChildA")
    with open(Path(temporary_directory, "GrandA/ParentA/ChildA"), "w") as f:
        f.write("Test 1")

    # Create the deploy root
    deploy_root = Path(temporary_directory, "output", "deploy")
    os.makedirs(deploy_root)

    # Incorrect dst path
    with pytest.raises(NotInDeployRootError):
        symlink_or_copy(
            src=Path("GrandA", "ParentA", "ChildA"),
            dst=Path("output", "ParentA", "ChildA"),
            deploy_root=deploy_root,
        )

    file_in_deploy_root = Path("output", "deploy", "ParentA", "ChildA")

    # Correct path and parent directories are automatically created
    symlink_or_copy(
        src=Path("GrandA", "ParentA", "ChildA"),
        dst=file_in_deploy_root,
        deploy_root=deploy_root,
    )

    assert file_in_deploy_root.exists() and file_in_deploy_root.is_symlink()
    assert file_in_deploy_root.read_text(encoding="utf-8") == os_agnostic_snapshot

    # Since file_in_deploy_root is a symlink
    # it resolves to project_dir/GrandA/ParentA/ChildA, which is not in deploy root
    with pytest.raises(NotInDeployRootError):
        symlink_or_copy(
            src=Path("GrandA", "ParentA", "ChildA"),
            dst=file_in_deploy_root,
            deploy_root=deploy_root,
        )

    # Unlink the symlink file and create a file with the same name and path
    # This should pass since src.is_file() always begins by deleting the dst.
    os.unlink(file_in_deploy_root)
    touch(file_in_deploy_root)
    symlink_or_copy(
        src=Path("GrandA", "ParentA", "ChildA"),
        dst=file_in_deploy_root,
        deploy_root=deploy_root,
    )

    # dst is an existing symlink, will resolve to the src during NotInDeployRootError check.
    touch("GrandA/ParentA/ChildB")
    with pytest.raises(NotInDeployRootError):
        symlink_or_copy(
            src=Path("GrandA/ParentA/ChildB"),
            dst=file_in_deploy_root,
            deploy_root=deploy_root,
        )
    assert file_in_deploy_root.exists() and file_in_deploy_root.is_symlink()
    assert file_in_deploy_root.read_text(encoding="utf-8") == os_agnostic_snapshot


@pytest.mark.skipif(
    IS_WINDOWS, reason="Symlinks on Windows are restricted to Developer mode or admins"
)
def test_symlink_or_copy_with_no_symlinks_in_project_root(os_agnostic_snapshot):
    test_dir_structure = {
        "GrandA/ParentA/ChildA/GrandChildA": "Text GrandA/ParentA/ChildA/GrandChildA",
        "GrandA/ParentA/ChildA/GrandChildB.py": "Text GrandA/ParentA/ChildA/GrandChildB.py",
        "GrandA/ParentA/ChildA/GrandChildC": None,  # dir
        "GrandA/ParentA/ChildB.py": "Text GrandA/ParentA/ChildB.py",
        "GrandA/ParentA/ChildC": "Text GrandA/ParentA/ChildC",
        "GrandA/ParentA/ChildD": None,  # dir
        "GrandA/ParentB/ChildA": "Text GrandA/ParentB/ChildA",
        "GrandA/ParentB/ChildB.py": "Text GrandA/ParentB/ChildB.py",
        "GrandA/ParentB/ChildC/GrandChildA": None,  # dir
        "GrandA/ParentC": None,  # dir
        "GrandB/ParentA/ChildA": "Text GrandB/ParentA/ChildA",
        "output/deploy": None,  # dir
    }
    with temp_local_dir(test_dir_structure) as project_root:
        with change_directory(project_root):
            # Sanity Check
            assert_dir_snapshot(Path("."), os_agnostic_snapshot)

            deploy_root = Path(project_root, "output/deploy")

            # "GrandB" dir
            symlink_or_copy(
                src=Path("GrandB/ParentA/ChildA"),
                dst=Path(deploy_root, "Grand1/Parent1/Child1"),
                deploy_root=deploy_root,
            )
            assert not Path(deploy_root, "Grand1").is_symlink()
            assert not Path(deploy_root, "Grand1/Parent1").is_symlink()
            assert Path(deploy_root, "Grand1/Parent1/Child1").is_symlink()

            # "GrandA/ParentC" dir
            symlink_or_copy(
                src=Path("GrandA/ParentC"),
                dst=Path(deploy_root, "Grand2"),
                deploy_root=deploy_root,
            )
            assert not Path(deploy_root, "Grand2").is_symlink()

            # "GrandA/ParentB" dir
            symlink_or_copy(
                src=Path("GrandA/ParentB/ChildA"),
                dst=Path(deploy_root, "Grand3"),
                deploy_root=deploy_root,
            )
            assert Path(deploy_root, "Grand3").is_symlink()
            symlink_or_copy(
                src=Path("GrandA/ParentB/ChildB.py"),
                dst=Path(deploy_root, "Grand4/Parent1.py"),
                deploy_root=deploy_root,
            )
            assert not Path(deploy_root, "Grand4").is_symlink()
            assert Path(deploy_root, "Grand4/Parent1.py").is_symlink()
            symlink_or_copy(
                src=Path("GrandA/ParentB/ChildC"),
                dst=Path(deploy_root, "Grand4/Parent2"),
                deploy_root=deploy_root,
            )
            assert not Path(deploy_root, "Grand4").is_symlink()
            assert not Path(deploy_root, "Grand4/Parent2").is_symlink()
            assert not Path(deploy_root, "Grand4/Parent2/GrandChildA").is_symlink()

            # "GrandA/ParentA" dir (1)
            symlink_or_copy(
                src=Path("GrandA/ParentA"), dst=deploy_root, deploy_root=deploy_root
            )
            assert not deploy_root.is_symlink()
            assert not Path(deploy_root, "ChildA").is_symlink()
            assert Path(deploy_root, "ChildA/GrandChildA").is_symlink()
            assert Path(deploy_root, "ChildA/GrandChildB.py").is_symlink()
            assert not Path(deploy_root, "ChildA/GrandChildC").is_symlink()
            assert Path(deploy_root, "ChildB.py").is_symlink()
            assert Path(deploy_root, "ChildC").is_symlink()
            assert not Path(deploy_root, "ChildD").is_symlink()

            # "GrandA/ParentA" dir (2)
            symlink_or_copy(
                src=Path("GrandA/ParentA"),
                dst=Path(deploy_root, "Grand4/Parent3"),
                deploy_root=deploy_root,
            )
            # Other children of Grand4 will be verified by a full assert_dir_snapshot(project_root) below
            assert not Path(deploy_root, "Grand4/Parent3").is_symlink()
            assert not Path(deploy_root, "Grand4/Parent3/ChildA").is_symlink()
            assert Path(deploy_root, "Grand4/Parent3/ChildA/GrandChildA").is_symlink()
            assert Path(
                deploy_root, "Grand4/Parent3/ChildA/GrandChildB.py"
            ).is_symlink()
            assert not Path(
                deploy_root, "Grand4/Parent3/ChildA/GrandChildC"
            ).is_symlink()
            assert Path(deploy_root, "Grand4/Parent3/ChildB.py").is_symlink()
            assert Path(deploy_root, "Grand4/Parent3/ChildC").is_symlink()
            assert not Path(deploy_root, "Grand4/Parent3/ChildD").is_symlink()

            assert_dir_snapshot(Path("./output/deploy"), os_agnostic_snapshot)

            # This is because the dst can be symlinks, which resolves to project src and hence outside deploy root.
            with pytest.raises(NotInDeployRootError):
                symlink_or_copy(
                    src=Path("GrandA/ParentB"),
                    dst=Path(deploy_root, "Grand4/Parent3"),
                    deploy_root=deploy_root,
                )


@pytest.mark.skipif(
    IS_WINDOWS, reason="Symlinks on Windows are restricted to Developer mode or admins"
)
def test_symlink_or_copy_with_symlinks_in_project_root(os_agnostic_snapshot):
    test_dir_structure = {
        "GrandA/ParentA": "Do not use as src of a symlink",
        "GrandA/ParentB": "Use as src of a symlink: GrandA/ParentB",
        "GrandA/ParentC/ChildA/GrandChildA": "Do not use as src of a symlink",
        "GrandA/ParentC/ChildA/GrandChildB": "Use as src of a symlink: GrandA/ParentC/ChildA/GrandChildB",
        "GrandB/ParentA/ChildA/GrandChildA": "Do not use as src of a symlink",
        "GrandB/ParentA/ChildB/GrandChildA": None,
        "symlinks/Grand1/Parent3/Child1": None,
        "symlinks/Grand2": None,
        "output/deploy": None,  # dir
    }
    with temp_local_dir(test_dir_structure) as project_root:
        with change_directory(project_root):
            # Sanity Check
            assert_dir_snapshot(Path("."), os_agnostic_snapshot)

            os.symlink(
                Path("GrandA/ParentB").resolve(),
                Path(project_root, "symlinks/Grand1/Parent2"),
            )
            os.symlink(
                Path("GrandA/ParentC/ChildA/GrandChildB").resolve(),
                Path(project_root, "symlinks/Grand1/Parent3/Child1/GrandChild2"),
            )
            os.symlink(
                Path("GrandB/ParentA").resolve(),
                Path(project_root, "symlinks/Grand2/Parent1"),
                target_is_directory=True,
            )
            assert Path("symlinks").is_dir() and not Path("symlinks").is_symlink()
            assert (
                Path("GrandA/ParentB").is_file()
                and not Path("GrandA/ParentB").is_symlink()
            )
            assert (
                Path("symlinks/Grand1/Parent2").is_symlink()
                and Path("symlinks/Grand1/Parent2").is_file()
            )
            assert (
                Path("symlinks/Grand1/Parent3/Child1/GrandChild2").is_symlink()
                and Path("symlinks/Grand1/Parent3/Child1/GrandChild2").is_file()
            )
            assert (
                Path("symlinks/Grand2/Parent1").is_symlink()
                and Path("symlinks/Grand2/Parent1").is_dir()
            )

            # Sanity Check
            assert_dir_snapshot(Path("./symlinks"), os_agnostic_snapshot)

            deploy_root = Path(project_root, "output/deploy")

            symlink_or_copy(
                src=Path("GrandA"),
                dst=Path(deploy_root, "TestA"),
                deploy_root=deploy_root,
            )
            assert not Path(deploy_root, "TestA").is_symlink()
            assert Path(deploy_root, "TestA/ParentA").is_symlink()
            assert Path(deploy_root, "TestA/ParentB").is_symlink()
            assert not Path(deploy_root, "TestA/ParentC").is_symlink()
            assert not Path(deploy_root, "TestA/ParentC/ChildA").is_symlink()
            assert Path(deploy_root, "TestA/ParentC/ChildA/GrandChildA").is_symlink()
            assert Path(deploy_root, "TestA/ParentC/ChildA/GrandChildB").is_symlink()

            symlink_or_copy(
                src=Path("GrandB"),
                dst=Path(deploy_root, "TestB"),
                deploy_root=deploy_root,
            )
            assert not Path(deploy_root, "TestB").is_symlink()
            assert not Path(deploy_root, "TestB/ParentA").is_symlink()
            assert not Path(deploy_root, "TestB/ParentA/ChildA").is_symlink()
            assert not Path(deploy_root, "TestB/ParentA/ChildB").is_symlink()
            assert not Path(
                deploy_root, "TestB/ParentA/ChildB/GrandChildA"
            ).is_symlink()
            assert Path(deploy_root, "TestB/ParentA/ChildA/GrandChildA").is_symlink()

            symlink_or_copy(
                src=Path("symlinks"),
                dst=Path(deploy_root, "symlinks"),
                deploy_root=deploy_root,
            )
            assert (
                Path(deploy_root, "symlinks/Grand1").is_dir()
                and not Path(deploy_root, "symlinks/Grand1").is_symlink()
            )
            assert (
                Path(deploy_root, "symlinks/Grand1/Parent2").is_file()
                and Path(deploy_root, "symlinks/Grand1/Parent2").is_symlink()
            )
            assert (
                Path(deploy_root, "symlinks/Grand1/Parent3").is_dir()
                and not Path(deploy_root, "symlinks/Grand1/Parent3").is_symlink()
            )
            assert (
                Path(deploy_root, "symlinks/Grand1/Parent3/Child1").is_dir()
                and not Path(deploy_root, "symlinks/Grand1/Parent3/Child1").is_symlink()
            )
            assert (
                Path(
                    deploy_root, "symlinks/Grand1/Parent3/Child1/GrandChild2"
                ).is_file()
                and Path(
                    deploy_root, "symlinks/Grand1/Parent3/Child1/GrandChild2"
                ).is_symlink()
            )
            assert (
                Path(deploy_root, "symlinks/Grand2").is_dir()
                and not Path(deploy_root, "symlinks/Grand2").is_symlink()
            )
            assert (
                Path(deploy_root, "symlinks/Grand2/Parent1").is_dir()
                and not Path(deploy_root, "symlinks/Grand2/Parent1").is_symlink()
            )
            assert (
                Path(deploy_root, "symlinks/Grand2/Parent1/ChildA").is_dir()
                and not Path(deploy_root, "symlinks/Grand2/Parent1/ChildA").is_symlink()
            )
            assert (
                Path(
                    deploy_root, "symlinks/Grand2/Parent1/ChildA/GrandChildA"
                ).is_file()
                and Path(
                    deploy_root, "symlinks/Grand2/Parent1/ChildA/GrandChildA"
                ).is_symlink()
            )
            assert (
                Path(deploy_root, "symlinks/Grand2/Parent1/ChildB/GrandChildA").is_dir()
                and not Path(
                    deploy_root, "symlinks/Grand2/Parent1/ChildB/GrandChildA"
                ).is_symlink()
            )

            assert_dir_snapshot(Path("./output/deploy"), os_agnostic_snapshot)
