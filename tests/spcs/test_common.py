from snowflake.cli.plugins.spcs.common import validate_and_set_instances
from tests.testing_utils.fixtures import *
from click import ClickException


@pytest.mark.parametrize(
    "min_instances, max_instances, expected_max",
    [
        (2, None, 2),  # max_instances is None, set max_instances to min_instances
        (
            5,
            10,
            10,
        ),  # max_instances is valid non-None value, return max_instances unchanged
    ],
)
def test_validate_and_set_instances(min_instances, max_instances, expected_max):
    assert expected_max == validate_and_set_instances(
        min_instances, max_instances, "name"
    )


@pytest.mark.parametrize(
    "min_instances, max_instances, expected_msg",
    [
        (0, 1, "min_name must be positive"),  # non-positive min_instances
        (-1, 1, "min_name must be positive"),  # negative min_instances
        (
            2,
            1,
            "max_name must be greater or equal to min_name",
        ),  # min_instances > max_instances
    ],
)
def test_validate_and_set_instances_invalid(min_instances, max_instances, expected_msg):
    with pytest.raises(ClickException) as exc:
        validate_and_set_instances(min_instances, max_instances, "name")
    assert expected_msg in exc.value.message
