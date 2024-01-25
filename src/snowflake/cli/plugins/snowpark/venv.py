import logging
import subprocess
import sys
import venv
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Tuple

if (is_windows:=sys.platform == "win32"):
    import _winapi  # noqa type: ignore

log = logging.getLogger(__name__)


class Venv(object):
    ERROR_MESSAGE = "Running command {0} caused error {1}"

    def __init__(self, directory: str = "", with_pip: bool = True):
        self.directory = TemporaryDirectory(directory)
        self.with_pip = with_pip

    def __enter__(self):
        self._create_venv()
        self.python_path = Path(self.directory.name) / "bin" / "python"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.directory.cleanup()

    def _create_venv(self):
        venv.create(self.directory.name, self.with_pip)

    def run_python(self, args):

        try:
            process = subprocess.run(
                [self.python_path, *args],
                capture_output=(not is_windows),
                text=True,
                shell=is_windows,
            )
        except subprocess.CalledProcessError as e:
            log.error(self.ERROR_MESSAGE, "python" + " ".join(args), e.stderr)
            raise SystemExit

        return process

    def pip_install(self, name: str, req_type: str, directory: str = ".packages"):
        arguments = ["-m", "pip", "install", "-t", directory]
        arguments += ["-r", name] if req_type == "file" else [name]
        process = self.run_python(arguments)

        print(Path(directory).absolute())

        return process.returncode


