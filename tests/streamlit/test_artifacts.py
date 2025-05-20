import os
from pathlib import Path

import pytest

from tests.streamlit.streamlit_test_class import StreamlitTestClass

STREAMLIT_FILES = [
    "streamlit_app.py",
    "pages/my_page.py",
    "environment.yml",
]

ALL_PATHS = [
    {"local": Path("src") / "app.py", "stage": "/src"},
    {
        "local": Path("src") / "dir" / "dir_app.py",
        "stage": "/src/dir",
    },
]

ALL_BUNDLED_PATHS = [
    "src/dir/dir_app.py",
    "src/app.py",
]


class TestArtifacts(StreamlitTestClass):
    @pytest.mark.parametrize(
        "artifacts, paths, bundled_paths",
        [
            ("src", ALL_PATHS, ALL_BUNDLED_PATHS),
            ("src/", ALL_PATHS, ALL_BUNDLED_PATHS),
            ("src/*", ALL_PATHS, ALL_BUNDLED_PATHS),
            (
                "src/*.py",
                [{"local": Path("src") / "app.py", "stage": "/src"}],
                ["src/app.py"],
            ),
            (
                "src/dir/dir_app.py",
                [
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/src/dir",
                    }
                ],
                ["src/dir/dir_app.py"],
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
                [
                    "source/dir_app.py",
                    "source/app.py",
                    "source/dir/dir_app.py",
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
                [
                    "source/src/app.py",
                    "source/src/dir/dir_app.py",
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
                [
                    "source/src/app.py",
                    "source/src/dir/dir_app.py",
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
                [
                    "source/src/app.py",
                    "source/src/dir/dir_app.py",
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
                [
                    "source/src/app.py",
                    "source/src/dir/dir_app.py",
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
                ["source/dir/dir_app.py", "source/app.py"],
            ),
            (
                {"src": "src/dir/dir_app.py", "dest": "source/dir/apps/"},
                [
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source/dir/apps",
                    }
                ],
                ["source/dir/apps/dir_app.py"],
            ),
        ],
    )
    def test_deploy_with_artifacts(
        self,
        artifacts,
        paths,
        bundled_paths,
        project_directory,
        alter_snowflake_yml,
        runner,
        mock_streamlit_ctx,
    ):

        self.mock_connector.return_value = mock_streamlit_ctx
        self.mock_conn.execute_string.return_value = mock_streamlit_ctx

        with project_directory("glob_patterns") as tmp:
            alter_snowflake_yml(
                tmp / "snowflake.yml",
                "entities.my_streamlit.artifacts",
                STREAMLIT_FILES + [artifacts],
            )

            result = runner.invoke(
                [
                    "streamlit",
                    "deploy",
                    "--replace",
                ]
            )
            assert result.exit_code == 0, result.output

            self._assert_that_exactly_those_files_were_put_to_stage(
                STREAMLIT_FILES + bundled_paths, streamlit_name="my_streamlit"
            )

    @pytest.mark.parametrize(
        "artifacts, paths, bundle_paths",
        [
            ("src", ALL_PATHS, ALL_BUNDLED_PATHS),
            ("src/", ALL_PATHS, ALL_BUNDLED_PATHS),
            ("src/*", ALL_PATHS, ALL_BUNDLED_PATHS),
            (
                "src/*.py",
                [{"local": Path("src") / "app.py", "stage": "/src"}],
                ["src/app.py"],
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
                ["source/dir/dir_app.py", "source/app.py", "source/dir_app.py"],
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
                ["source/src/app.py", "source/src/dir/dir_app.py"],
            ),
            (
                {"src": "src/dir/dir_app.py", "dest": "source/dir/apps/"},
                [
                    {
                        "local": Path("src") / "dir" / "dir_app.py",
                        "stage": "/source/dir/apps",
                    }
                ],
                ["source/dir/apps/dir_app.py"],
            ),
        ],
    )
    def test_deploy_with_artifacts_from_other_directory(
        self,
        artifacts,
        paths,
        bundle_paths,
        runner,
        project_directory,
        alter_snowflake_yml,
        mock_streamlit_ctx,
    ):
        self.mock_connector.return_value = mock_streamlit_ctx
        self.mock_conn.execute_string.return_value = mock_streamlit_ctx

        with project_directory("glob_patterns") as tmp:
            os.chdir(Path(os.getcwd()).parent)
            alter_snowflake_yml(
                tmp / "snowflake.yml",
                "entities.my_streamlit.artifacts",
                STREAMLIT_FILES + [artifacts],
            )

            result = runner.invoke(["streamlit", "deploy", "-p", tmp, "--replace"])
            assert result.exit_code == 0, result.output

            self._assert_that_exactly_those_files_were_put_to_stage(
                put_files=STREAMLIT_FILES + bundle_paths,
                streamlit_name="my_streamlit",
            )
