import pytest


@pytest.mark.parametrize(
    "flags",
    [
        ["--pypi-download", "yes"],
        ["--pypi-download", "no"],
        ["--pypi-download", "ask"],
        ["--check-anaconda-for-pypi-deps"],
        ["-a"],
        ["--no-check-anaconda-for-pypi-deps"],
        ["--package-native-libraries", "yes"],
        ["--package-native-libraries", "no"],
        ["--package-native-libraries", "ask"],
    ],
)
def test_snowpark_build_deprecated_flags_warning(flags, runner, project_directory):
    with project_directory("snowpark_functions"):
        result = runner.invoke(["snowpark", "build", *flags])
        assert result.exit_code == 0, result.output
        assert "flag is deprecated" in result.output


def test_snowpark_build_no_deprecated_warnings_by_default(runner, project_directory):
    with project_directory("snowpark_functions"):
        result = runner.invoke(["snowpark", "build"])
        assert result.exit_code == 0, result.output
        assert "flag is deprecated" not in result.output
