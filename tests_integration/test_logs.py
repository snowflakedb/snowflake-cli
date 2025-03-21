import pytest


@pytest.mark.integration
def test_logs(runner):
    result = runner.invoke_with_connection_json(["logs", "compute_pool", "SNOWCLI_COMPUTE_POOL","--from","2025-03-21T11:00:00+01:00", "--refresh","20"])
    print(result)
    assert True
