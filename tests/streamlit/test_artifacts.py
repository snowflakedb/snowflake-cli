import os
from pathlib import Path

import pytest

from tests.streamlit.streamlit_test_class import StreamlitTestClass

BUNDLE_ROOT = Path("output") / "bundle" / "streamlit"
STREAMLIT_NAME = "test_streamlit"

ALL_PATHS = [
    {"local": Path("src") / "app.py", "stage": "/src"},
    {
        "local": Path("src") / "dir" / "dir_app.py",
        "stage": "/src/dir",
    },
]


class TestArtifacts(StreamlitTestClass):
    @pytest.mark.parametrize(
        "artifacts, paths",
        [
            ("src", ALL_PATHS),
            ("src/", ALL_PATHS),
            ("src/*", ALL_PATHS),
            ("src/*.py", [{"local": Path("src") / "app.py", "stage": "/src"}]),
            (
                "src/dir/dir_app.py",
                [
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/src/dir",
                    }
                ],
            ),
            (
                {"src": "src/**/*", "dest": "source/"},
                [
                    {"local": Path("src") / "app.py", "stage": "/source"},
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source",
                    },
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source/dir",
                    },
                ],
            ),
            (
                {"src": "src", "dest": "source/"},
                [
                    {
                        "local": Path("src") / "app.py",
                        "stage": "/source/src",
                    },
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source/src/dir",
                    },
                ],
            ),
            (
                {"src": "src/", "dest": "source/"},
                [
                    {
                        "local": Path("src") / "app.py",
                        "stage": "/source/src",
                    },
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source/src/dir",
                    },
                ],
            ),
            (
                {"src": "src/*", "dest": "source/"},
                [
                    {"local": Path("src") / "app.py", "stage": "/source"},
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source/dir",
                    },
                ],
            ),
            (
                {"src": "src/dir/dir_app.py", "dest": "source/dir/apps/"},
                [
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source/dir/apps",
                    }
                ],
            ),
        ],
    )
    def test_deploy_with_artifacts(self, artifacts, paths, setup):
        streamlit_files = [
            "streamlit_app.py",
            "pages/my_page.py",
            "environment.yml",
        ]

        with self.project_directory("glob_patterns") as tmp:
            self.alter_snowflake_yml(
                tmp / "snowflake.yml",
                "entities.my_streamlit.artifacts",
                streamlit_files + [artifacts],
            )

            result = self.runner.invoke(
                [
                    "streamlit",
                    "deploy",
                    "--replace",
                ]
            )
            assert result.exit_code == 0, result.output

            self._assert_that_exactly_those_files_were_put_to_stage(
                streamlit_files + paths
            )

    @pytest.mark.parametrize(
        "artifacts, paths",
        [
            ("src", ALL_PATHS),
            ("src/", ALL_PATHS),
            ("src/*", ALL_PATHS),
            ("src/*.py", [{"local": Path("src") / "app.py", "stage": "/src"}]),
            (
                {"src": "src/**/*", "dest": "source/"},
                [
                    {"local": Path("src") / "app.py", "stage": "/source"},
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source",
                    },
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source/dir",
                    },
                ],
            ),
            (
                {"src": "src/", "dest": "source/"},
                [
                    {"local": Path("src") / "app.py", "stage": "/source/src"},
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source/src/dir",
                    },
                ],
            ),
            (
                {"src": "src/dir/dir_app.py", "dest": "source/dir/apps/"},
                [
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source/dir/apps",
                    }
                ],
            ),
        ],
    )
    def test_deploy_with_artifacts_from_other_directory(self, artifacts, paths, setup):
        streamlit_files = [
            "streamlit_app.py",
            "pages/my_page.py",
            "environment.yml",
        ]

        with self.project_directory("glob_patterns") as tmp:
            os.chdir(Path(os.getcwd()).parent)
            self.alter_snowflake_yml(
                tmp / "snowflake.yml",
                "entities.my_streamlit.artifacts",
                streamlit_files + [artifacts],
            )

            result = self.runner.invoke(["streamlit", "deploy", "-p", tmp, "--replace"])
            assert result.exit_code == 0, result.output

            self._assert_that_exactly_those_files_were_put_to_stage(
                put_files=streamlit_files + paths, project_root=tmp
            )
