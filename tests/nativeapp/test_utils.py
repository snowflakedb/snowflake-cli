import pytest
from snowflake.cli.api.secure_path import SecurePath
from snowflake.cli.plugins.nativeapp.utils import get_first_paragraph_from_markdown_file


@pytest.mark.parametrize(
    "file_content, expected_paragraph",
    [
        (
            "\n"
            "## Introduction"
            "\n"
            "This is an example template for a Snowflake Native App project which demonstrates the use of Python "
            "extension code and adding Streamlit code. This template is meant to guide developers towards a "
            "possible project structure on the basis of functionality, as well as to indicate the contents of "
            "some common and useful files.\n"
            "\n"
            "Since this template contains Python files only, you do not need to perform any additional steps to "
            "build the source code. You can directly go to the next section. However, if there were any source "
            "code that needed to be built, you must manually perform the build steps here before proceeding to "
            "the next section.",
            "This is an example template for a Snowflake Native App project which demonstrates the use of "
            "Python extension code and adding Streamlit code. This template is meant to guide developers towards a "
            "possible project structure on the basis of functionality, as well as to indicate the contents of some "
            "common and useful files.",
        ),
        ("# Just a Heading\n" "\n" "## And some whitespace.", None),
    ],
)
def test_get_first_paragraph_from_markdown_file_with_valid_path(
    file_content, expected_paragraph
):
    with SecurePath.temporary_directory() as temp_path:
        temp_readme_path = (temp_path / "README.md").path

        with open(temp_readme_path, "w+") as temp_readme_file:
            temp_readme_file.write(file_content)

        actual_paragraph = get_first_paragraph_from_markdown_file(temp_readme_path)

        assert actual_paragraph == expected_paragraph


def test_get_first_paragraph_from_markdown_file_with_invalid_path():
    with SecurePath.temporary_directory() as temp_path:
        temp_readme_path = (temp_path / "README.md").path

        actual_paragraph = get_first_paragraph_from_markdown_file(temp_readme_path)

        assert actual_paragraph is None
