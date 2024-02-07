# import platform
import warnings

import pytest


#
# @pytest.mark.skipif(
#     platform.system() == "Windows", reason="Permission setting does not work on Windows"
# )
@pytest.mark.integration
def test_created_config_file_does_not_trigger_permission_warning(
    runner, snowflake_home
):
    config_file = snowflake_home / "config.toml"
    assert not config_file.exists()
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = runner.invoke("--help")
        assert result.exit_code == 0
        assert config_file.exists()
