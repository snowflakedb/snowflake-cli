import pytest

from snowcli.cli.stage.manager import StageManager


@pytest.mark.parametrize(
    "stage,expected",
    [
        ("stageName", "@stageName"),
        ("@stageName", "@stageName"),
        ("@my_db.my$Schema.my_Stage", "@my_db.my$Schema.my_Stage"),
        ("@stage/my/path", "@stage/my/path"),
        ("'@db.schema.stage/my/path'", "'@db.schema.stage/my/path'"),
        ("@db.my'schema'.stage/my/path", "'@db.my''schema''.stage/my/path'"),
        ("'@db.my''schema''.stage/my/path'", "'@db.my''schema''.stage/my/path'"),
    ],
)
def test_quote_stage_name(stage, expected):
    assert StageManager.quote_stage_name(stage) == expected
