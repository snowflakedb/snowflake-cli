import subprocess

import pytest


@pytest.mark.e2e
def test_config_file_creation(snowcli, test_root_path, snowflake_home, snapshot):
    output1 = subprocess.check_output([snowcli, "connection", "list"], encoding="utf-8")
    snapshot.assert_match(output1)

    output2 = subprocess.check_output([snowcli, "connection", "list"], encoding="utf-8")
    snapshot.assert_match(output2)
