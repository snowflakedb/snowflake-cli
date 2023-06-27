from pathlib import Path
from tempfile import TemporaryDirectory

from snowcli.config import CliConfigManager


def test_empty_config_file_is_created_if_not_present():
    with TemporaryDirectory() as tmp_dir:
        config_file = Path(tmp_dir) / "sub" / "config.toml"
        assert config_file.exists() is False

        cm = CliConfigManager(file_path=config_file)
        cm.from_context(config_path_override=None)
        assert config_file.exists() is True
        assert config_file.read_text() == """[connections]\n"""
