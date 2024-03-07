import pytest
from snowflake.cli.plugins.snowpark.models import Requirement, get_package_name


@pytest.mark.parametrize(
    "line,name,extras",
    [
        ("ipython ; extra == 'docs'", "ipython", ["docs"]),
        ("foo", "foo", []),
        ("pytest ; extra == 'tests'", "pytest", ["tests"]),
    ],
)
def test_requirement_is_parsed_correctly(line, name, extras):
    result = Requirement.parse_line(line)

    assert result.name == name
    assert result.extras == extras


@pytest.mark.parametrize(
    "line,name",
    [
        ("pydantic", "pydantic"),
        (
            "git+https://github.com/sfc-gh-turbaszek/dummy-pkg-for-tests",
            "dummy-pkg-for-tests",
        ),
        (
            "git+https://github.com/sfc-gh-turbaszek/dummy-pkg-for-tests@foo",
            "dummy-pkg-for-tests",
        ),
        (
            "git+https://github.com/sfc-gh-turbaszek/dummy-pkg-for-tests@0123456789abcdef0123456789abcdef01234567",
            "dummy-pkg-for-tests",
        ),
        ("foo.zip", "foo"),
    ],
)
def test_get_package_name(line, name):
    assert get_package_name(line) == name
