from unittest import mock

import pytest

from tests.testing_utils.fixtures import *

PROJECT_PATH = "demo_na_project"


@pytest.mark.parametrize(
    "id,init_args",
    [
        # with implicit name
        ["all_upper", ["DEMO_NA_PROJECT"]],
        ["snake_case", ["demo_na_project"]],
        ["camel_case", ["DemoNAProject"]],
        ["with_dashes", ["demo-na-project"]],
        ["with_spaces", ["demo na project"]],
        ["with_dots", ["demo.na.project"]],
        ["with_dollar_sign", ["demo$na$project"]],
        ["with_ampersand", ["demo@na@project"]],
        ["with_double_dash", ["demo--na--project"]],
        ["with_double_dot", ["demo..na..project"]],
        ["with_double_underscore", ["demo..na..project"]],
        ["with_mixed_replacement", ["demo.-na .project"]],
        ["leading_underscore", ["_demo_na_project"]],
        ["leading_underscore", ["_demo_na_project"]],
        ["leading_digit", ["1_demo_na_project"]],
        ["middle_single_quote", ["demo_na'project"]],
        ["middle_double_quote", ['demo_na"project']],
        ["with_backslash", ["demo\\na\\project"]],
        ["with_slash", ["demo/na/project"]],
        ["relative_to_cwd", ["./demo/na/project"]],
        ["relative_to_parent", ["../demo/na/project"]],
        ["absolute", ["/demo/na/project"]],
        ["double_quoted", ['"demo_na_project"']],
        ["with_inner_quotes", ['"demo""na""_""""project"']],
        # with explicit --name
        ["name_all_upper", [PROJECT_PATH, "--name", "DEMO_NA_PROJECT"]],
        ["name_snake_case", [PROJECT_PATH, "--name", "demo_na_project"]],
        ["name_camel_case", [PROJECT_PATH, "--name", "DemoNAProject"]],
        ["name_with_dashes", [PROJECT_PATH, "--name", "demo-na-project"]],
        ["name_with_spaces", [PROJECT_PATH, "--name", "demo na project"]],
        ["name_with_dots", [PROJECT_PATH, "--name", "demo.na.project"]],
        ["name_with_dollar_sign", [PROJECT_PATH, "--name", "demo$na$project"]],
        ["name_with_ampersand", [PROJECT_PATH, "--name", "demo@na@project"]],
        ["name_with_double_dash", [PROJECT_PATH, "--name", "demo--na--project"]],
        ["name_with_double_dot", [PROJECT_PATH, "--name", "demo..na..project"]],
        ["name_with_double_underscore", [PROJECT_PATH, "--name", "demo..na..project"]],
        ["name_with_mixed_replacement", [PROJECT_PATH, "--name", "demo.-na .project"]],
        ["name_leading_underscore", [PROJECT_PATH, "--name", "_demo_na_project"]],
        ["name_leading_underscore", [PROJECT_PATH, "--name", "_demo_na_project"]],
        ["name_leading_digit", [PROJECT_PATH, "--name", "1_demo_na_project"]],
        ["name_middle_single_quote", [PROJECT_PATH, "--name", "demo_na'project"]],
        ["name_middle_double_quote", [PROJECT_PATH, "--name", 'demo_na"project']],
        ["name_with_backslash", [PROJECT_PATH, "--name", "demo\\na\\project"]],
        ["name_with_slash", [PROJECT_PATH, "--name", "demo/na/project"]],
        ["name_double_quoted", [PROJECT_PATH, "--name", '"demo_na_project"']],
        [
            "name_with_inner_quotes",
            [PROJECT_PATH, "--name", '"demo""na""_""""project"'],
        ],
    ],
)
@mock.patch(
    "snowflake.cli.plugins.nativeapp.init._init_from_template", return_value=None
)
def test_init_no_template_success(
    mock_init_from_template, runner, temp_dir, snapshot, id, init_args
):
    args = ["app", "init"]
    args.extend(init_args)
    result = runner.invoke(args)

    assert result.exit_code == 0
    assert result.output == snapshot


@pytest.mark.parametrize(
    "id,init_args",
    [
        # with implicit name
        ["with_unterminated_id", ['"demo_na_project']],
        ["with_trailing_double_quote", ['demo_na_project"']],
        ["with_invalid_id", ['"demo"na_project']],
        ["with_unquoted_inner_quote", ['"demo"na_project"']],
        ["empty_path", [""]],
        # with explicit --name
        ["name_with_unterminated_id", [PROJECT_PATH, "--name", '"demo_na_project']],
        [
            "name_with_trailing_double_quote",
            [PROJECT_PATH, "--name", 'demo_na_project"'],
        ],
        ["name_with_invalid_id", [PROJECT_PATH, "--name", '"demo"na_project']],
        [
            "name_with_unquoted_inner_quote",
            [PROJECT_PATH, "--name", '"demo"na_project"'],
        ],
        ["empty_name", [PROJECT_PATH, "--name", ""]],
    ],
)
@mock.patch(
    "snowflake.cli.plugins.nativeapp.init._init_from_template", return_value=None
)
def test_init_no_template_failure(
    mock_init_from_template, runner, temp_dir, snapshot, id, init_args
):
    args = ["app", "init"]
    args.extend(init_args)
    result = runner.invoke(args)

    assert result.exit_code == 1
    assert result.output == snapshot
