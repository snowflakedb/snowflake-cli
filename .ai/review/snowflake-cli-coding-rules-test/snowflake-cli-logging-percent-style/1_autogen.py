import logging

log = logging.getLogger(__name__)


def process_order(order_id: str, total: float) -> None:
    log.info("Processing order {} with total {}".format(order_id, total))  # noqa: G001


def fetch_items(source: str, count: int):
    log.debug(f"Fetching {count} items from {source}")  # noqa: G004
    return []


def check_threshold(value: int, limit: int):
    if value > limit:
        log.warning(f"Value {value} exceeds threshold {limit}")  # noqa: G004


def connect(host: str):
    try:
        pass
    except Exception as e:
        log.error(f"Connection to {host} failed: {e}")  # noqa: G004


class DataPipeline:
    def __init__(self, name: str):
        self.name = name

    def start(self):
        log.info(f"Starting pipeline {self.name}")  # noqa: G004

    def run(self, items):
        try:
            return [x * 2 for x in items]
        except Exception as e:
            log.exception("Pipeline {} failed: {}".format(self.name, e))  # noqa: G001
