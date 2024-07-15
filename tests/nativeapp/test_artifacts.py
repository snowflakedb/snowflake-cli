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
from typing import Dict, Iterable, List, Optional, Union

import pytest
from snowflake.cli.api.project.definition import load_project
from snowflake.cli.api.project.schemas.native_app.path_mapping import PathMapping
from snowflake.cli.plugins.nativeapp.artifacts import (
    ArtifactError,
    ArtifactPredicate,
    BundleMap,
    DeployRootError,
    NotInDeployRootError,
    SourceNotFoundError,
    TooManyFilesError,
    build_bundle,
    resolve_without_follow,
    symlink_or_copy,
)

from tests.nativeapp.utils import assert_dir_snapshot, touch
from tests.testing_utils.files_and_dirs import pushd, temp_local_dir
from tests_common import IS_WINDOWS


def trimmed_contents(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    with open(path, "r") as handle:
        return handle.read().strip()


def dir_structure(path: Path, prefix="") -> List[str]:
    if not path.is_dir():
        raise ValueError("Path must point to a directory")

    parts: List[str] = []
    for child in sorted(path.iterdir()):
        if child.is_dir():
            parts += dir_structure(child, f"{prefix}{child.name}/")
        else:
            parts.append(f"{prefix}{child.name}")

    return parts


@pytest.fixture
def bundle_map():
    project_files = {
        "snowflake.yml": "# empty",
        "README.md": "# Test Project",
        "app/setup.sql": "-- empty",
        "app/manifest.yml": "# empty",
        "src/snowpark/main.py": "# empty",
        "src/snowpark/a/file1.py": "# empty",
        "src/snowpark/a/file2.py": "# empty",
        "src/snowpark/a/b/file3.py": "# empty",
        "src/snowpark/a/b/file4.py": "# empty",
        "src/snowpark/a/c/file5.py": "# empty",
        "src/streamlit/main_ui.py": "# empty",
        "src/streamlit/helpers/file1.py": "# empty",
        "src/streamlit/helpers/file2.py": "# empty",
    }
    with temp_local_dir(project_files) as project_root:
        deploy_root = project_root / "output" / "deploy"
        yield BundleMap(project_root=project_root, deploy_root=deploy_root)


def ensure_path(path: Union[Path, str]) -> Path:
    if isinstance(path, str):
        return Path(path)
    return path


def verify_mappings(
    bundle_map: BundleMap,
    expected_mappings: Dict[
        Union[str, Path], Optional[Union[str, Path, List[str], List[Path]]]
    ],
    expected_deploy_paths: Dict[
        Union[str, Path], Optional[Union[str, Path, List[str], List[Path]]]
    ]
    | None = None,
    **kwargs,
):
    def normalize_expected_dest(
        dest: Optional[Union[str, Path, List[str], List[Path]]]
    ):
        if dest is None:
            return []
        elif isinstance(dest, str):
            return [ensure_path(dest)]
        elif isinstance(dest, Path):
            return [dest]
        else:
            return sorted([ensure_path(d) for d in dest])

    normalized_expected_mappings = {
        ensure_path(src): normalize_expected_dest(dest)
        for src, dest in expected_mappings.items()
        if dest is not None
    }
    if expected_deploy_paths is not None:
        normalized_expected_deploy_paths = {
            ensure_path(src): normalize_expected_dest(dest)
            for src, dest in expected_deploy_paths.items()
        }
    else:
        normalized_expected_deploy_paths = normalized_expected_mappings

    for src, expected_dests in normalized_expected_deploy_paths.items():
        assert sorted(bundle_map.to_deploy_paths(ensure_path(src))) == expected_dests

    actual_path_mappings: Dict[Path, List[Path]] = {}
    for src, dest in bundle_map.all_mappings(**kwargs):
        mappings = actual_path_mappings.setdefault(src, [])
        mappings.append(dest)
        mappings.sort()

    assert actual_path_mappings == normalized_expected_mappings


def verify_sources(
    bundle_map: BundleMap, expected_sources: Iterable[Union[str, Path]], **kwargs
) -> None:
    actual_sources = sorted(bundle_map.all_sources(**kwargs))
    expected_sources = sorted([ensure_path(src) for src in expected_sources])
    assert actual_sources == expected_sources


def test_empty_bundle_map(bundle_map):
    mappings = list(bundle_map.all_mappings())
    assert mappings == []

    verify_sources(bundle_map, [])

    verify_mappings(
        bundle_map,
        {
            "app/setup.sql": None,
            ".": None,
            "/not/in/project": None,
        },
    )


def test_bundle_map_handles_file_to_file_mappings(bundle_map):
    bundle_map.add(PathMapping(src="README.md", dest="deployed_readme.md"))
    bundle_map.add(PathMapping(src="app/setup.sql", dest="app_setup.sql"))
    bundle_map.add(PathMapping(src="app/manifest.yml", dest="manifest.yml"))

    verify_mappings(
        bundle_map,
        {
            "README.md": "deployed_readme.md",
            "app/setup.sql": "app_setup.sql",
            "app/manifest.yml": "manifest.yml",
        },
    )

    verify_sources(bundle_map, ["README.md", "app/setup.sql", "app/manifest.yml"])


def test_bundle_map_supports_double_star_glob(bundle_map):
    bundle_map.add(PathMapping(src="src/snowpark/**/*.py", dest="deployed/"))

    expected_mappings = {
        "src/snowpark/main.py": "deployed/main.py",
        "src/snowpark/a/file1.py": "deployed/file1.py",
        "src/snowpark/a/file2.py": "deployed/file2.py",
        "src/snowpark/a/b/file3.py": "deployed/file3.py",
        "src/snowpark/a/b/file4.py": "deployed/file4.py",
        "src/snowpark/a/c/file5.py": "deployed/file5.py",
    }

    verify_mappings(bundle_map, expected_mappings)

    verify_sources(bundle_map, expected_mappings.keys())


def test_bundle_map_supports_complex_globbing(bundle_map):
    bundle_map.add(PathMapping(src="src/s*/**/file[3-5].py", dest="deployed/"))

    expected_mappings = {
        "src/snowpark/main.py": None,
        "src/snowpark/a/file1.py": None,
        "src/snowpark/a/file2.py": None,
        "src/snowpark/a/b/file3.py": "deployed/file3.py",
        "src/snowpark/a/b/file4.py": "deployed/file4.py",
        "src/snowpark/a/c/file5.py": "deployed/file5.py",
    }

    verify_mappings(
        bundle_map,
        expected_mappings,
    )

    verify_sources(
        bundle_map,
        [src for src in expected_mappings.keys() if expected_mappings[src] is not None],
    )


def test_bundle_map_handles_mapping_to_deploy_root(bundle_map):
    bundle_map.add(PathMapping(src="app/*", dest="./"))
    bundle_map.add(PathMapping(src="README.md", dest="./"))

    verify_mappings(
        bundle_map,
        {
            "app/setup.sql": "setup.sql",
            "app/manifest.yml": "manifest.yml",
            "README.md": "README.md",
        },
    )


def test_bundle_map_can_rename_directories(bundle_map):
    bundle_map.add(PathMapping(src="app", dest="deployed"))

    verify_mappings(
        bundle_map,
        {
            "app": "deployed",
        },
        expand_directories=False,
    )

    verify_mappings(
        bundle_map,
        {
            "app": "deployed",
            "app/setup.sql": "deployed/setup.sql",
            "app/manifest.yml": "deployed/manifest.yml",
        },
        expand_directories=True,
    )


def test_bundle_map_honours_trailing_slashes(bundle_map):
    bundle_map.add(PathMapping(src="app", dest="deployed/"))
    bundle_map.add(PathMapping(src="README.md", dest="deployed/"))
    bundle_map.add(
        # src trailing slash has no effect
        PathMapping(src="src/snowpark/", dest="deployed/")
    )

    verify_mappings(
        bundle_map,
        {
            "app": "deployed/app",
            "src/snowpark": "deployed/snowpark",
            "README.md": "deployed/README.md",
        },
    )

    verify_mappings(
        bundle_map,
        {
            "app": "deployed/app",
            "app/manifest.yml": "deployed/app/manifest.yml",
            "app/setup.sql": "deployed/app/setup.sql",
            "src/snowpark": "deployed/snowpark",
            "src/snowpark/main.py": "deployed/snowpark/main.py",
            "src/snowpark/a": "deployed/snowpark/a",
            "src/snowpark/a/file1.py": "deployed/snowpark/a/file1.py",
            "src/snowpark/a/file2.py": "deployed/snowpark/a/file2.py",
            "src/snowpark/a/b": "deployed/snowpark/a/b",
            "src/snowpark/a/b/file3.py": "deployed/snowpark/a/b/file3.py",
            "src/snowpark/a/b/file4.py": "deployed/snowpark/a/b/file4.py",
            "src/snowpark/a/c": "deployed/snowpark/a/c",
            "src/snowpark/a/c/file5.py": "deployed/snowpark/a/c/file5.py",
            "README.md": "deployed/README.md",
        },
        expand_directories=True,
    )


def test_bundle_map_disallows_overwriting_deploy_root(bundle_map):
    with pytest.raises(NotInDeployRootError):
        bundle_map.add(PathMapping(src="app/*", dest="."))


def test_bundle_map_disallows_unknown_sources(bundle_map):
    with pytest.raises(SourceNotFoundError):
        bundle_map.add(PathMapping(src="missing/*", dest="deployed/"))

    with pytest.raises(SourceNotFoundError):
        bundle_map.add(PathMapping(src="missing", dest="deployed/"))

    with pytest.raises(SourceNotFoundError):
        bundle_map.add(PathMapping(src="**/*.missing", dest="deployed/"))


def test_bundle_map_disallows_mapping_multiple_to_file(bundle_map):
    with pytest.raises(TooManyFilesError):
        # multiple files named 'file1.py' would collide
        bundle_map.add(PathMapping(src="**/file1.py", dest="deployed/"))

    with pytest.raises(TooManyFilesError):
        bundle_map.add(PathMapping(src="**/file1.py", dest="deployed/"))


def test_bundle_map_allows_mapping_file_to_multiple_destinations(bundle_map):
    bundle_map.add(PathMapping(src="README.md", dest="deployed/README1.md"))
    bundle_map.add(PathMapping(src="README.md", dest="deployed/README2.md"))
    bundle_map.add(PathMapping(src="src/streamlit", dest="deployed/streamlit_orig"))
    bundle_map.add(PathMapping(src="src/streamlit", dest="deployed/streamlit_copy"))
    bundle_map.add(PathMapping(src="src/streamlit/main_ui.py", dest="deployed/"))

    verify_mappings(
        bundle_map,
        expected_mappings={
            "README.md": ["deployed/README1.md", "deployed/README2.md"],
            "src/streamlit": ["deployed/streamlit_orig", "deployed/streamlit_copy"],
            "src/streamlit/main_ui.py": ["deployed/main_ui.py"],
        },
        expected_deploy_paths={
            "README.md": ["deployed/README1.md", "deployed/README2.md"],
            "src/streamlit": ["deployed/streamlit_orig", "deployed/streamlit_copy"],
            "src/streamlit/main_ui.py": [
                "deployed/main_ui.py",
                "deployed/streamlit_orig/main_ui.py",
                "deployed/streamlit_copy/main_ui.py",
            ],
        },
    )

    verify_mappings(
        bundle_map,
        expected_mappings={
            "README.md": ["deployed/README1.md", "deployed/README2.md"],
            "src/streamlit": ["deployed/streamlit_orig", "deployed/streamlit_copy"],
            "src/streamlit/main_ui.py": [
                "deployed/main_ui.py",
                "deployed/streamlit_orig/main_ui.py",
                "deployed/streamlit_copy/main_ui.py",
            ],
            "src/streamlit/helpers": [
                "deployed/streamlit_orig/helpers",
                "deployed/streamlit_copy/helpers",
            ],
            "src/streamlit/helpers/file1.py": [
                "deployed/streamlit_orig/helpers/file1.py",
                "deployed/streamlit_copy/helpers/file1.py",
            ],
            "src/streamlit/helpers/file2.py": [
                "deployed/streamlit_orig/helpers/file2.py",
                "deployed/streamlit_copy/helpers/file2.py",
            ],
        },
        expected_deploy_paths={
            "README.md": ["deployed/README1.md", "deployed/README2.md"],
            "src/streamlit": ["deployed/streamlit_orig", "deployed/streamlit_copy"],
            "src/streamlit/main_ui.py": [
                "deployed/main_ui.py",
                "deployed/streamlit_orig/main_ui.py",
                "deployed/streamlit_copy/main_ui.py",
            ],
            "src/streamlit/helpers": [
                "deployed/streamlit_orig/helpers",
                "deployed/streamlit_copy/helpers",
            ],
            "src/streamlit/helpers/file1.py": [
                "deployed/streamlit_orig/helpers/file1.py",
                "deployed/streamlit_copy/helpers/file1.py",
            ],
            "src/streamlit/helpers/file2.py": [
                "deployed/streamlit_orig/helpers/file2.py",
                "deployed/streamlit_copy/helpers/file2.py",
            ],
        },
        expand_directories=True,
    )


def test_bundle_map_handles_missing_dest(bundle_map):
    bundle_map.add(PathMapping(src="app"))
    bundle_map.add(PathMapping(src="README.md"))
    bundle_map.add(PathMapping(src="src/streamlit/"))

    verify_mappings(
        bundle_map,
        {"app": "app", "README.md": "README.md", "src/streamlit": "src/streamlit"},
    )

    verify_mappings(
        bundle_map,
        {
            "app": "app",
            "app/setup.sql": "app/setup.sql",
            "app/manifest.yml": "app/manifest.yml",
            "README.md": "README.md",
            "src/streamlit": "src/streamlit",
            "src/streamlit/helpers": "src/streamlit/helpers",
            "src/streamlit/main_ui.py": "src/streamlit/main_ui.py",
            "src/streamlit/helpers/file1.py": "src/streamlit/helpers/file1.py",
            "src/streamlit/helpers/file2.py": "src/streamlit/helpers/file2.py",
        },
        expand_directories=True,
    )


def test_bundle_map_disallows_mapping_files_as_directories(bundle_map):
    bundle_map.add(PathMapping(src="app", dest="deployed/"))
    with pytest.raises(ArtifactError):
        bundle_map.add(PathMapping(src="**/main.py", dest="deployed"))


def test_bundle_map_disallows_mapping_directories_as_files(bundle_map):
    bundle_map.add(PathMapping(src="**/main.py", dest="deployed"))
    with pytest.raises(ArtifactError):
        bundle_map.add(PathMapping(src="app", dest="deployed"))


def test_bundle_map_allows_deploying_other_sources_to_renamed_directory(bundle_map):
    bundle_map.add(PathMapping(src="src/snowpark", dest="./snowpark"))
    bundle_map.add(PathMapping(src="README.md", dest="snowpark/"))

    verify_mappings(
        bundle_map,
        {
            "src/snowpark": "snowpark",
            "README.md": "snowpark/README.md",
        },
    )

    verify_mappings(
        bundle_map,
        {
            "README.md": "snowpark/README.md",
            "src/snowpark": "snowpark",
            "src/snowpark/main.py": "snowpark/main.py",
            "src/snowpark/a": "snowpark/a",
            "src/snowpark/a/file1.py": "snowpark/a/file1.py",
            "src/snowpark/a/file2.py": "snowpark/a/file2.py",
            "src/snowpark/a/b": "snowpark/a/b",
            "src/snowpark/a/b/file3.py": "snowpark/a/b/file3.py",
            "src/snowpark/a/b/file4.py": "snowpark/a/b/file4.py",
            "src/snowpark/a/c": "snowpark/a/c",
            "src/snowpark/a/c/file5.py": "snowpark/a/c/file5.py",
        },
        expand_directories=True,
    )


@pytest.mark.skip(reason="Checking deep tree hierarchies is not yet supported")
def test_bundle_map_disallows_collisions_anywhere_in_deployed_hierarchy(bundle_map):
    bundle_map.add(PathMapping(src="src/snowpark", dest="./snowpark"))
    bundle_map.add(PathMapping(src="README.md", dest="snowpark/"))

    # if any of the files collide, however, this is not allowed
    with pytest.raises(TooManyFilesError):
        bundle_map.add(PathMapping(src="app/manifest.yml", dest="snowpark/README.md"))

    with pytest.raises(TooManyFilesError):
        bundle_map.add(PathMapping(src="app/manifest.yml", dest="snowpark/a/file1.py"))


def test_bundle_map_disallows_mapping_outside_deploy_root(bundle_map):
    with pytest.raises(NotInDeployRootError):
        bundle_map.add(PathMapping(src="app", dest="deployed/../../"))

    with pytest.raises(NotInDeployRootError):
        bundle_map.add(PathMapping(src="app", dest=Path().resolve().root))

    with pytest.raises(NotInDeployRootError):
        bundle_map.add(PathMapping(src="app", dest="/////"))


def test_bundle_map_disallows_absolute_src(bundle_map):
    with pytest.raises(ArtifactError):
        absolute_src = bundle_map.project_root() / "app"
        assert absolute_src.is_absolute()
        bundle_map.add(PathMapping(src=str(absolute_src), dest="deployed"))


def test_bundle_map_disallows_absolute_dest(bundle_map):
    with pytest.raises(ArtifactError):
        absolute_dest = bundle_map.deploy_root() / "deployed"
        assert absolute_dest.is_absolute()
        bundle_map.add(PathMapping(src="app", dest=str(absolute_dest)))


def test_bundle_map_disallows_clobbering_parent_directories(bundle_map):
    # one level of nesting
    with pytest.raises(TooManyFilesError):
        bundle_map.add(PathMapping(src="snowflake.yml", dest="./app/"))
        # Adding a new rule to populate ./app/ from an existing directory. This would
        # clobber the output of the previous rule, so it's disallowed
        bundle_map.add(PathMapping(src="./app", dest="./"))

    # same as above but with multiple levels of nesting
    with pytest.raises(TooManyFilesError):
        bundle_map.add(PathMapping(src="snowflake.yml", dest="./src/snowpark/a/"))
        bundle_map.add(PathMapping(src="./src/snowpark", dest="./src/"))


def test_bundle_map_disallows_clobbering_child_directories(bundle_map):
    with pytest.raises(TooManyFilesError):
        bundle_map.add(PathMapping(src="./src/snowpark", dest="./python/"))
        bundle_map.add(PathMapping(src="./app", dest="./python/snowpark/a"))


def test_bundle_map_allows_augmenting_dest_directories(bundle_map):
    # one level of nesting
    # First populate {deploy}/app from an existing directory
    bundle_map.add(PathMapping(src="./app", dest="./"))
    # Then add a new file to that directory
    bundle_map.add(PathMapping(src="snowflake.yml", dest="./app/"))

    # verify that when iterating over mappings, the base directory rule appears first,
    # followed by the file. This is important for correctness, and should be
    # deterministic
    ordered_dests = [
        dest for (_, dest) in bundle_map.all_mappings(expand_directories=True)
    ]
    file_index = ordered_dests.index(Path("app/snowflake.yml"))
    dir_index = ordered_dests.index(Path("app"))
    assert dir_index < file_index


def test_bundle_map_allows_augmenting_dest_directories_nested(bundle_map):
    # same as above but with multiple levels of nesting
    bundle_map.add(PathMapping(src="./src/snowpark", dest="./src/"))
    bundle_map.add(PathMapping(src="snowflake.yml", dest="./src/snowpark/a/"))

    ordered_dests = [
        dest for (_, dest) in bundle_map.all_mappings(expand_directories=True)
    ]
    file_index = ordered_dests.index(Path("src/snowpark/a/snowflake.yml"))
    dir_index = ordered_dests.index(Path("src/snowpark"))
    assert dir_index < file_index


def test_bundle_map_returns_mappings_in_insertion_order(bundle_map):
    # this behaviour is important to make sure the deploy root is populated in a
    # deterministic manner, so verify it here
    bundle_map.add(PathMapping(src="./app", dest="./"))
    bundle_map.add(PathMapping(src="snowflake.yml", dest="./app/"))
    bundle_map.add(PathMapping(src="./src/snowpark", dest="./src/"))
    bundle_map.add(PathMapping(src="snowflake.yml", dest="./src/snowpark/a/"))

    ordered_dests = [
        dest for (_, dest) in bundle_map.all_mappings(expand_directories=False)
    ]
    assert ordered_dests == [
        Path("app"),
        Path("app/snowflake.yml"),
        Path("src/snowpark"),
        Path("src/snowpark/a/snowflake.yml"),
    ]


def test_bundle_map_all_mappings_generates_absolute_directories_when_requested(
    bundle_map,
):
    project_root = bundle_map.project_root()
    assert project_root.is_absolute()
    deploy_root = bundle_map.deploy_root()
    assert deploy_root.is_absolute()

    bundle_map.add(PathMapping(src="app", dest="deployed_app"))
    bundle_map.add(PathMapping(src="README.md", dest="deployed_README.md"))
    bundle_map.add(PathMapping(src="src/streamlit", dest="deployed_streamlit"))

    verify_mappings(
        bundle_map,
        {
            "app": "deployed_app",
            "README.md": "deployed_README.md",
            "src/streamlit": "deployed_streamlit",
        },
    )

    verify_mappings(
        bundle_map,
        {
            project_root / "app": deploy_root / "deployed_app",
            project_root / "README.md": deploy_root / "deployed_README.md",
            project_root / "src/streamlit": deploy_root / "deployed_streamlit",
        },
        absolute=True,
        expand_directories=False,
    )

    verify_mappings(
        bundle_map,
        {
            project_root / "app": deploy_root / "deployed_app",
            project_root / "app/setup.sql": deploy_root / "deployed_app/setup.sql",
            project_root
            / "app/manifest.yml": deploy_root
            / "deployed_app/manifest.yml",
            project_root / "README.md": deploy_root / "deployed_README.md",
            project_root / "src/streamlit": deploy_root / "deployed_streamlit",
            project_root
            / "src/streamlit/helpers": deploy_root
            / "deployed_streamlit/helpers",
            project_root
            / "src/streamlit/main_ui.py": deploy_root
            / "deployed_streamlit/main_ui.py",
            project_root
            / "src/streamlit/helpers/file1.py": deploy_root
            / "deployed_streamlit/helpers/file1.py",
            project_root
            / "src/streamlit/helpers/file2.py": deploy_root
            / "deployed_streamlit/helpers/file2.py",
        },
        absolute=True,
        expand_directories=True,
    )


def test_bundle_map_all_sources_generates_absolute_directories_when_requested(
    bundle_map,
):
    project_root = bundle_map.project_root()
    assert project_root.is_absolute()

    bundle_map.add(PathMapping(src="app", dest="deployed_app"))
    bundle_map.add(PathMapping(src="README.md", dest="deployed_README.md"))
    bundle_map.add(PathMapping(src="src/streamlit", dest="deployed_streamlit"))

    verify_sources(bundle_map, ["app", "README.md", "src/streamlit"])

    verify_sources(
        bundle_map,
        [
            project_root / "app",
            project_root / "README.md",
            project_root / "src/streamlit",
        ],
        absolute=True,
    )


def test_bundle_map_all_mappings_accepts_predicates(bundle_map):
    project_root = bundle_map.project_root()
    assert project_root.is_absolute()
    deploy_root = bundle_map.deploy_root()
    assert deploy_root.is_absolute()

    bundle_map.add(PathMapping(src="app", dest="deployed_app"))
    bundle_map.add(PathMapping(src="README.md", dest="deployed_README.md"))
    bundle_map.add(PathMapping(src="src/streamlit", dest="deployed_streamlit"))

    collected: Dict[Path, Path] = {}

    def collecting_predicate(predicate: ArtifactPredicate) -> ArtifactPredicate:
        def _predicate(src: Path, dest: Path) -> bool:
            collected[src] = dest
            return predicate(src, dest)

        return _predicate

    verify_mappings(
        bundle_map,
        {
            project_root
            / "src/streamlit/main_ui.py": deploy_root
            / "deployed_streamlit/main_ui.py",
            project_root
            / "src/streamlit/helpers/file1.py": deploy_root
            / "deployed_streamlit/helpers/file1.py",
            project_root
            / "src/streamlit/helpers/file2.py": deploy_root
            / "deployed_streamlit/helpers/file2.py",
        },
        absolute=True,
        expand_directories=True,
        predicate=collecting_predicate(
            lambda src, dest: src.is_file() and src.suffix == ".py"
        ),
    )

    assert collected == {
        project_root / "app": deploy_root / "deployed_app",
        project_root / "app/setup.sql": deploy_root / "deployed_app/setup.sql",
        project_root / "app/manifest.yml": deploy_root / "deployed_app/manifest.yml",
        project_root / "README.md": deploy_root / "deployed_README.md",
        project_root / "src/streamlit": deploy_root / "deployed_streamlit",
        project_root
        / "src/streamlit/helpers": deploy_root
        / "deployed_streamlit/helpers",
        project_root
        / "src/streamlit/main_ui.py": deploy_root
        / "deployed_streamlit/main_ui.py",
        project_root
        / "src/streamlit/helpers/file1.py": deploy_root
        / "deployed_streamlit/helpers/file1.py",
        project_root
        / "src/streamlit/helpers/file2.py": deploy_root
        / "deployed_streamlit/helpers/file2.py",
    }

    collected = {}

    verify_mappings(
        bundle_map,
        {
            "src/streamlit/main_ui.py": "deployed_streamlit/main_ui.py",
            "src/streamlit/helpers/file1.py": "deployed_streamlit/helpers/file1.py",
            "src/streamlit/helpers/file2.py": "deployed_streamlit/helpers/file2.py",
        },
        absolute=False,
        expand_directories=True,
        predicate=collecting_predicate(lambda src, dest: src.suffix == ".py"),
    )

    assert collected == {
        Path("app"): Path("deployed_app"),
        Path("app/setup.sql"): Path("deployed_app/setup.sql"),
        Path("app/manifest.yml"): Path("deployed_app/manifest.yml"),
        Path("README.md"): Path("deployed_README.md"),
        Path("src/streamlit"): Path("deployed_streamlit"),
        Path("src/streamlit/main_ui.py"): Path("deployed_streamlit/main_ui.py"),
        Path("src/streamlit/helpers"): Path("deployed_streamlit/helpers"),
        Path("src/streamlit/helpers/file1.py"): Path(
            "deployed_streamlit/helpers/file1.py"
        ),
        Path("src/streamlit/helpers/file2.py"): Path(
            "deployed_streamlit/helpers/file2.py"
        ),
    }


def test_bundle_map_to_deploy_path(bundle_map):
    bundle_map.add(PathMapping(src="app", dest="deployed_app"))
    bundle_map.add(PathMapping(src="README.md", dest="deployed_README.md"))
    bundle_map.add(PathMapping(src="src/streamlit", dest="deployed_streamlit"))

    # to_deploy_path returns relative paths when relative paths are given as input
    assert bundle_map.to_deploy_paths(Path("app")) == [Path("deployed_app")]
    assert bundle_map.to_deploy_paths(Path("README.md")) == [Path("deployed_README.md")]
    assert bundle_map.to_deploy_paths(Path("src/streamlit")) == [
        Path("deployed_streamlit")
    ]
    assert bundle_map.to_deploy_paths(Path("src/streamlit/main_ui.py")) == [
        Path("deployed_streamlit/main_ui.py")
    ]
    assert bundle_map.to_deploy_paths(Path("src/streamlit/helpers")) == [
        Path("deployed_streamlit/helpers")
    ]
    assert bundle_map.to_deploy_paths(Path("src/streamlit/helpers/file1.py")) == [
        Path("deployed_streamlit/helpers/file1.py")
    ]
    assert bundle_map.to_deploy_paths(Path("src/streamlit/missing.py")) == []
    assert bundle_map.to_deploy_paths(Path("missing")) == []
    assert bundle_map.to_deploy_paths(Path("src/missing/")) == []
    assert bundle_map.to_deploy_paths(bundle_map.project_root().parent) == []

    # to_deploy_path returns absolute paths when absolute paths are given as input
    project_root = bundle_map.project_root()
    deploy_root = bundle_map.deploy_root()
    assert bundle_map.to_deploy_paths(project_root / "app") == [
        deploy_root / "deployed_app"
    ]
    assert bundle_map.to_deploy_paths(project_root / "README.md") == [
        deploy_root / "deployed_README.md"
    ]
    assert bundle_map.to_deploy_paths(project_root / "src/streamlit") == [
        deploy_root / "deployed_streamlit"
    ]
    assert bundle_map.to_deploy_paths(project_root / "src/streamlit/main_ui.py") == [
        deploy_root / "deployed_streamlit/main_ui.py"
    ]
    assert bundle_map.to_deploy_paths(project_root / "src/streamlit/helpers") == [
        deploy_root / "deployed_streamlit/helpers"
    ]
    assert bundle_map.to_deploy_paths(
        project_root / "src/streamlit/helpers/file1.py"
    ) == [deploy_root / "deployed_streamlit/helpers/file1.py"]
    assert bundle_map.to_deploy_paths(project_root / "src/streamlit/missing.py") == []


def test_bundle_map_to_deploy_path_returns_multiple_matches(bundle_map):
    bundle_map.add(PathMapping(src="src/snowpark", dest="d1"))
    bundle_map.add(PathMapping(src="src/snowpark", dest="d2"))

    assert sorted(bundle_map.to_deploy_paths(Path("src/snowpark"))) == [
        Path("d1"),
        Path("d2"),
    ]

    assert sorted(bundle_map.to_deploy_paths(Path("src/snowpark/main.py"))) == [
        Path("d1/main.py"),
        Path("d2/main.py"),
    ]

    assert sorted(bundle_map.to_deploy_paths(Path("src/snowpark/a/b"))) == [
        Path("d1/a/b"),
        Path("d2/a/b"),
    ]

    bundle_map.add(PathMapping(src="src/snowpark/a", dest="d3"))

    assert sorted(bundle_map.to_deploy_paths(Path("src/snowpark/a/b/file3.py"))) == [
        Path("d1/a/b/file3.py"),
        Path("d2/a/b/file3.py"),
        Path("d3/b/file3.py"),
    ]


@pytest.mark.parametrize(
    "dest, src",
    [
        ["manifest.yml", "app/manifest.yml"],
        [".", None],
        ["python/snowpark/main.py", "src/snowpark/main.py"],
        ["python/snowpark", "src/snowpark"],
        ["python/snowpark/a/b", "src/snowpark/a/b"],
        ["python/snowpark/a/b/fake.py", None],
        [
            # even though a rule creates this directory, it has no equivalent source folder
            "python",
            None,
        ],
        ["/fake/foo.py", None],
    ],
)
def test_to_project_path(bundle_map, dest, src):
    bundle_map.add(PathMapping(src="app/*", dest="./"))
    bundle_map.add(PathMapping(src="src/snowpark", dest="./python/snowpark"))

    # relative paths
    if src is None:
        assert bundle_map.to_project_path(Path(dest)) is None
        assert bundle_map.to_project_path(Path(bundle_map.deploy_root() / dest)) is None
    else:
        assert bundle_map.to_project_path(Path(dest)) == Path(src)
        assert (
            bundle_map.to_project_path(Path(bundle_map.deploy_root() / dest))
            == bundle_map.project_root() / src
        )


def test_bundle_map_ignores_sources_in_deploy_root(bundle_map):
    bundle_map.deploy_root().mkdir(parents=True, exist_ok=True)
    deploy_root_source = bundle_map.deploy_root() / "should_not_match.yml"
    touch(str(deploy_root_source))

    bundle_map.add(PathMapping(src="**/*.yml", dest="deployed/"))

    verify_mappings(
        bundle_map,
        {
            "app/manifest.yml": "deployed/manifest.yml",
            "snowflake.yml": "deployed/snowflake.yml",
        },
    )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_napp_project_1_artifacts(project_definition_files, os_agnostic_snapshot):
    project_root = project_definition_files[0].parent
    native_app = load_project(project_definition_files).project_definition.native_app

    with pushd(project_root) as local_path:
        deploy_root = Path(local_path, native_app.deploy_root)
        build_bundle(local_path, deploy_root, native_app.artifacts)

        assert_dir_snapshot(deploy_root.relative_to(local_path), os_agnostic_snapshot)

        # we should be able to re-bundle without any errors happening
        build_bundle(local_path, deploy_root, native_app.artifacts)

        # any additional files created in the deploy root will be obliterated by re-bundle
        with open(deploy_root / "unknown_file.txt", "w") as handle:
            handle.write("I am an unknown file!")
        assert_dir_snapshot(deploy_root.relative_to(local_path), os_agnostic_snapshot)

        build_bundle(local_path, deploy_root, native_app.artifacts)
        assert_dir_snapshot(deploy_root.relative_to(local_path), os_agnostic_snapshot)


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_source_not_found(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(SourceNotFoundError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[PathMapping(src="NOTFOUND.md", dest="NOTFOUND.md")],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_glob_matched_nothing(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(SourceNotFoundError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[PathMapping(src="**/*.jar", dest=".")],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_outside_deploy_root_three_ways(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(NotInDeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[PathMapping(src="setup.sql", dest="..")],
        )

    with pytest.raises(NotInDeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[PathMapping(src="setup.sql", dest="/")],
        )

    with pytest.raises(NotInDeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[PathMapping(src="app", dest=".")],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_bad_deploy_root(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(DeployRootError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "..", "deploy"),
            artifacts=[],
        )

    with pytest.raises(DeployRootError):
        with open(project_root / "deploy", "w") as handle:
            handle.write("Deploy root should not be a file...")

        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[],
        )


@pytest.mark.parametrize("project_definition_files", ["napp_project_1"], indirect=True)
def test_too_many_files(project_definition_files):
    project_root = project_definition_files[0].parent
    with pytest.raises(TooManyFilesError):
        build_bundle(
            project_root,
            deploy_root=Path(project_root, "deploy"),
            artifacts=[
                PathMapping(
                    src="app/streamlit/*.py", dest="somehow_combined_streamlits.py"
                )
            ],
        )


@pytest.mark.skipif(
    IS_WINDOWS, reason="Symlinks on Windows are restricted to Developer mode or admins"
)
@pytest.mark.parametrize(
    "project_path,expected_path",
    [
        [
            "srcfile",
            "deploy/file",
        ],
        [
            "srcdir",
            "deploy/dir",
        ],
        [
            "srcdir/nested_file1",
            "deploy/dir/nested_file1",
        ],
        [
            "srcdir/nested_dir/nested_file2",
            "deploy/dir/nested_dir/nested_file2",
        ],
        [
            "srcdir/nested_dir",
            "deploy/dir/nested_dir",
        ],
        [
            "not-in-deploy",
            None,
        ],
    ],
)
def test_source_path_to_deploy_path(
    temp_dir,
    project_path,
    expected_path,
):
    # Source files
    touch("srcfile")
    touch("srcdir/nested_file1")
    touch("srcdir/nested_dir/nested_file2")
    touch("not-in-deploy")
    # Build
    os.mkdir("deploy")
    os.symlink("srcfile", "deploy/file")
    os.symlink(Path("srcdir").resolve(), Path("deploy/dir"))

    bundle_map = BundleMap(
        project_root=Path().resolve(), deploy_root=Path("deploy").resolve()
    )
    bundle_map.add(PathMapping(src="srcdir", dest="./dir"))
    bundle_map.add(PathMapping(src="srcfile", dest="./file"))

    result = bundle_map.to_deploy_paths(resolve_without_follow(Path(project_path)))
    if expected_path:
        assert result == [resolve_without_follow(Path(expected_path))]
    else:
        assert result == []


@pytest.mark.skipif(
    IS_WINDOWS, reason="Symlinks on Windows are restricted to Developer mode or admins"
)
def test_symlink_or_copy_raises_error(temp_dir, os_agnostic_snapshot):
    touch("GrandA/ParentA/ChildA")
    with open(Path(temp_dir, "GrandA/ParentA/ChildA"), "w") as f:
        f.write("Test 1")

    # Create the deploy root
    deploy_root = Path(temp_dir, "output", "deploy")
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
        with pushd(project_root):
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
        with pushd(project_root):
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
