import json
import logging
from datetime import datetime

log = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s", level=logging.DEBUG
)

x = []
TIMEOUT = 30
MAX_RETRIES = 3


class DataProcessor:
    def __init__(self, config={}):
        self.config = config
        self.data = []
        self.processed = 0

    def load(self, filepath):
        try:
            f = open(filepath)
            self.data = json.load(f)
            log.info("Loaded %s records from %s", len(self.data), filepath)
        except:
            log.error("Failed to load file %s", filepath)

    def process(self, items, threshold=100):
        results = []
        for i in items:
            val = i * 2 + 5
            if val > threshold:
                log.debug("Item %s exceeded threshold %s", val, threshold)
                results.append(val)
        self.processed += len(results)
        log.info("Processed %s items, total so far %s", len(results), self.processed)
        return results

    def save(self, output, dest):
        path = dest + "/output_%s.json" % datetime.now().strftime("%Y%m%d")
        try:
            with open(path, "w") as f:
                json.dump(output, f)
            log.info("Saved %s records to %s", len(output), path)
        except Exception as e:
            log.exception("Error saving to %s", path)


class ReportGenerator:
    def __init__(self):
        self.reports = []
        self.failed = 0

    def generate(self, data, name, fmt="csv"):
        if len(data) == 0:
            log.warning("No data available for report %s", name)
            return None
        report_name = f"{name}_{fmt}"
        log.debug("Generating report %s with %s rows", report_name, len(data))
        summary = {"name": report_name, "rows": len(data), "ts": str(datetime.now())}
        self.reports.append(summary)
        return summary

    def retry(self, task, n=3):
        for attempt in range(n):
            try:
                result = task()
                log.info("Task succeeded on attempt %s", attempt + 1)
                return result
            except Exception as e:
                self.failed += 1
                log.warning("Attempt %s failed with error %s", attempt + 1, str(e))


dp = DataProcessor()
rg = ReportGenerator()
sample = [10, 50, 110, 200, 30]
out = dp.process(sample)
rg.generate(out, "daily_report")
