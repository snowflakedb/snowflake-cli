import json
from pathlib import Path

from snowflake.cli.api.secure_path import SecurePath

CONFIG_FILE = "config.json"
OUTPUT = "output.txt"
LIMIT = 5


class DataProcessor:
    def __init__(self, base_dir):
        self.base_dir = base_dir

    def load_config(self):
        p = Path(self.base_dir) / CONFIG_FILE
        data = SecurePath(p).read_text(file_size_limit_mb=LIMIT)
        return json.loads(data)

    def save_results(self, results):
        out = Path(self.base_dir) / "results" / OUTPUT
        SecurePath(out).write_text(json.dumps(results))


class FileManager:
    def read_template(self, path):
        p = Path(path)
        if p.suffix == ".tmpl":
            return SecurePath(p).read_text(file_size_limit_mb=2)
        with SecurePath(path).open("rb", read_file_limit_mb=2) as f:
            return f.read()

    def write_output(self, dest, content):
        SecurePath(dest).write_text(content)


def path_manipulation_only(base, name):
    p = Path(base) / name
    stem = p.stem
    parent = p.parent
    suffix = p.suffix
    full_name = p.name
    _ = (stem, parent, suffix, full_name)
    return p


def stat_checks_only(path):
    p = Path(path)
    if not p.exists():
        return None
    if p.is_dir():
        return list(p.iterdir())
    if p.is_file():
        return p
    return None
