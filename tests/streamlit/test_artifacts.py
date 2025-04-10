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
    def test_deploy_with_artifacts(
        self,
        artifacts,
        paths,
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
                STREAMLIT_FILES + paths, streamlit_name="test_streamlit_deploy_snowcli"
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
    def test_deploy_with_artifacts_from_other_directory(
        self,
        artifacts,
        paths,
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
                put_files=STREAMLIT_FILES + paths,
                streamlit_name="test_streamlit_deploy_snowcli",
            )
