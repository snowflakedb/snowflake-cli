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

from textwrap import dedent

import pytest
from snowflake.cli._plugins.nativeapp.utils import (
    get_first_paragraph_from_markdown_file,
)
from snowflake.cli.api.secure_path import SecurePath


@pytest.mark.parametrize(
    "file_content, expected_paragraph",
    [
        (
            dedent(
                """
            ## Introduction

            This is an example template for a Snowflake Native App project which demonstrates the use of Python extension code and adding Streamlit code. This template is meant to guide developers towards a possible project structure on the basis of functionality, as well as to indicate the contents of some common and useful files. 

            Since this template contains Python files only, you do not need to perform any additional steps to build the source code. You can directly go to the next section. However, if there were any source code that needed to be built, you must manually perform the build steps here before proceeding to the next section. 

            Similarly, you can also use your own build steps for any other languages supported by Snowflake that you wish to write your code in. For more information on supported languages, visit [docs](https://docs.snowflake.com/en/developer-guide/stored-procedures-vs-udfs#label-sp-udf-languages).
            """
            ),
            "This is an example template for a Snowflake Native App project which demonstrates the use of Python extension code and adding Streamlit code. This template is meant to guide developers towards a possible project structure on the basis of functionality, as well as to indicate the contents of some common and useful files.",
        ),
        (
            "Similarly, you can also use your own build steps for any other languages supported by Snowflake that you wish to write your code in. For more information on supported languages, visit [docs](https://docs.snowflake.com/en/developer-guide/stored-procedures-vs-udfs#label-sp-udf-languages).",
            "Similarly, you can also use your own build steps for any other languages supported by Snowflake that you wish to write your code in. For more information on supported languages, visit [docs](https://docs.snowflake.com/en/developer-guide/stored-procedures-vs-udfs#label-sp-udf-languages).",
        ),
    ],
)
def test_get_first_paragraph_from_markdown_file_with_valid_path_and_paragraph_content(
    file_content, expected_paragraph
):
    with SecurePath.temporary_directory() as temp_path:
        temp_readme_path = (temp_path / "README.md").path

        with open(temp_readme_path, "w+") as temp_readme_file:
            temp_readme_file.write(file_content)

        actual_paragraph = get_first_paragraph_from_markdown_file(temp_readme_path)

        assert actual_paragraph == expected_paragraph


@pytest.mark.parametrize(
    "file_content",
    [
        dedent(
            """
            # Just some Headings

            ## And some whitespace.
            """
        ),
        "",
    ],
)
def test_get_first_paragraph_from_markdown_file_with_valid_path_and_no_paragraph_content(
    file_content,
):
    with SecurePath.temporary_directory() as temp_path:
        temp_readme_path = (temp_path / "README.md").path

        with open(temp_readme_path, "w+") as temp_readme_file:
            temp_readme_file.write(file_content)

        result = get_first_paragraph_from_markdown_file(temp_readme_path)

        assert result is None


def test_get_first_paragraph_from_markdown_file_with_invalid_path():
    with SecurePath.temporary_directory() as temp_path:
        temp_readme_path = (temp_path / "README.md").path

        with pytest.raises(FileNotFoundError):
            get_first_paragraph_from_markdown_file(temp_readme_path)
