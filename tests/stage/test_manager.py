from snowcli.cli.stage.manager import StageManager


def test_quote_stage_name():
    assert StageManager.quote_stage_name("stageName") == "@stageName"
    assert StageManager.quote_stage_name("@stageName") == "@stageName"
    assert (
        StageManager.quote_stage_name("@my_db.my$Schema.my_Stage")
        == "@my_db.my$Schema.my_Stage"
    )
    assert StageManager.quote_stage_name("@stage/my/path") == "@stage/my/path"
    assert (
        StageManager.quote_stage_name("'@db.schema.stage/my/path'")
        == "'@db.schema.stage/my/path'"
    )
    assert (
        StageManager.quote_stage_name("@db.my'schema'.stage/my/path")
        == "'@db.my''schema''.stage/my/path'"
    )
    assert (
        StageManager.quote_stage_name("'@db.my''schema''.stage/my/path'")
        == "'@db.my''schema''.stage/my/path'"
    )
