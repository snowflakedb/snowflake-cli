import pytest

SUCCESS = False


@pytest.mark.integration
def test_flaky():
    global SUCCESS
    if SUCCESS:
        pass
    else:
        SUCCESS = True
        raise AssertionError("This test is flaky and should be retried.")
