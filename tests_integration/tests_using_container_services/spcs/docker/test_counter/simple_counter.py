#!/opt/conda/bin/python3

# Copyright (c) 2024 Snowflake Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import time
import sys

logger = logging.getLogger("tests")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
formatter = logging.Formatter("%(message)s")
ch.setFormatter(formatter)

logger.addHandler(ch)


def main():
    args = sys.argv
    end_wait_time = 0  # sec
    if len(args) > 1:
        end_wait_time = int(args[1])
    for i in range(0, 10):
        logger.info(f"processing {i}")
        time.sleep(0.1)
    logger.info(f"waiting: {end_wait_time} sec")
    time.sleep(end_wait_time)


if __name__ == "__main__":
    main()
