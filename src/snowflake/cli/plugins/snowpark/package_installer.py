import logging
import subprocess
import venv
from pathlib import Path
from tempfile import TemporaryDirectory

log = logging.getLogger(__name__)


class PackageInstaller:
    PIP_ERROR_MESSAGE = "Running pip install for {0} {1} returned code {2}"
    PIP_FULL_ERROR = "Full error response: {0}"
    VENV_ERROR_MESSAGE = "Error creating venv. Full error trace: {0}"

    def __init__(self, destination: Path = Path.cwd()):
        self.temp_dir = TemporaryDirectory()
        self.destination = destination
        self.venv_dir = Path(self.temp_dir.name) / "venv"
        self.python_path = (self.venv_dir / "bin" / "python").name

        self._create_venv()

    def _create_venv(self):
        builder = venv.EnvBuilder(
            with_pip=True, system_site_packages=False, clear=True, symlinks=True
        )

        try:
            builder.create(self.venv_dir)
        except subprocess.CalledProcessError as e:
            log.error(self.VENV_ERROR_MESSAGE.format(e.output))
            raise SystemExit

    def run_pip_install(self, name: str, req_type: str):

        arguments = ["-r", name] if req_type == "file" else [name]
        with self.temp_dir:
            try:
                process = subprocess.run(
                    [self.python_path, "-m", "pip", "install", "-t", ".packages/"]
                    + arguments,
                    capture_output=True,
                    text=True,
                )
                log.debug(process.stdout)
            except subprocess.CalledProcessError as e:
                log.error(self.PIP_ERROR_MESSAGE.format(req_type, name, e.returncode))
                log.error(self.PIP_FULL_ERROR.format(e.stderr))

        return process.returncode

    def cleanup(self):
        self.temp_dir.cleanup()
