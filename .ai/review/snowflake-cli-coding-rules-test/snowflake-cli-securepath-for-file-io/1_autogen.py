import json
from pathlib import Path


def load_config(config_path: str) -> dict:
    content = Path(config_path).read_text(encoding="utf-8")
    return json.loads(content)


def load_binary(path: str) -> bytes:
    return Path(path).read_bytes()


def save_output(path: str, data: dict) -> None:
    Path(path).write_text(json.dumps(data))


def save_binary(path: str, data: bytes) -> None:
    Path(path).write_bytes(data)


def read_with_open(path: str) -> str:
    with open(path, "r") as f:
        return f.read()


def write_with_open(path: str, content: str) -> None:
    with open(path, "w") as f:
        f.write(content)


def read_with_path_open(path: str) -> str:
    with Path(path).open("r") as f:
        return f.read()


def write_with_path_open(path: str, content: str) -> None:
    with Path(path).open("w") as f:
        f.write(content)


class ConfigLoader:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir

    def load(self, name: str) -> dict:
        p = Path(self.base_dir) / name
        raw = p.read_text()
        return json.loads(raw)

    def save(self, name: str, data: dict) -> None:
        p = Path(self.base_dir) / name
        p.write_text(json.dumps(data))
