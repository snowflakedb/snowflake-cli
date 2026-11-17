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

PRIVACY_NOTICE = (
    "This command sends your message to Snowflake to improve Snowflake CLI.\n"
    "Do not include passwords, access tokens, private keys, account locators,\n"
    "data from Snowflake accounts, SQL that contains customer data, or other\n"
    "personal or confidential information."
)

ISSUES_URL = "https://github.com/snowflakedb/snowflake-cli/issues"

STATUS_SUBMITTED = "submitted"
STATUS_CONFIRMATION_REQUIRED = "confirmation_required"
STATUS_RATE_LIMITED = "rate_limited"
STATUS_NO_CONNECTION = "no_connection"
STATUS_SEND_FAILED = "send_failed"
STATUS_REJECTED = "rejected"

MAX_MESSAGE_BYTES = 4000
RATE_LIMIT_MAX = 5
RATE_LIMIT_WINDOW_SECONDS = 3600
SECRET_MASK = "********"
