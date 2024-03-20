import pytest


@pytest.fixture(scope="module")
def install_plugin(test_root_path):
    def _install(name: str):
        import subprocess

        path = test_root_path / ".." / "test_external_plugins" / name
        subprocess.check_call(["pip", "install", path, "--no-build-isolation"])

    return _install
