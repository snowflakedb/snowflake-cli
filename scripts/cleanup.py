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
from __future__ import annotations

import os
import typing as t
from datetime import datetime, timedelta

from snowflake.cli._app.snow_connector import update_connection_details_with_private_key
from snowflake.snowpark.session import Session


def is_stale(item):
    return datetime.utcnow() - item.created_on.replace(tzinfo=None) >= timedelta(
        hours=2
    )


def remove_resource(resource_type: str, item, role: str):
    msg = f"Deleting {resource_type} {item.name}"
    try:
        if is_stale(item):
            session.sql(
                f"GRANT OWNERSHIP ON {resource_type} {item.name} TO ROLE {role}"
            )
            session.sql(f"drop {resource_type} {item.name}").collect()
            print("SUCCESS", msg, f"created at {item.created_on}")
    except Exception as err:
        print("ERROR", msg)
        print(str(err)[:200])


def remove_resources(single: str, plural: str, known_instances: t.List[str], role: str):
    items = session.sql(f"show {plural}").collect()

    for item in items:
        if item.name not in known_instances:
            remove_resource(resource_type=single, item=item, role=role)


if __name__ == "__main__":
    role = os.getenv("SNOWFLAKE_CONNECTIONS_INTEGRATION_ROLE", "INTEGRATION_TESTS")
    config = {
        "authenticator": "SNOWFLAKE_JWT",
        "account": os.getenv("SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_CONNECTIONS_INTEGRATION_USER"),
        "private_key_file": os.getenv(
            "SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_FILE",
            os.getenv("SNOWFLAKE_CONNECTIONS_INTEGRATION_PRIVATE_KEY_PATH"),
        ),
        "database": "SNOWCLI_DB",
        "role": role,
    }
    update_connection_details_with_private_key(config)
    session = Session.builder.configs(config).create()

    session.use_role(role)

    known_objects: t.Dict[t.Tuple[str, str], t.List[str]] = {
        ("database", "databases"): [
            "CLEANUP_DB",
            "DOCS_TESTING",
            "EXTERNAL_ACCESS_DB",
            "SNOWCLI_DB",
        ],
        ("compute pool", "compute pools"): [],
        ("service", "services"): [],
        ("application", "applications"): [],
        ("warehouse", "warehouses"): ["XSMALL"],
    }

    for (single, plural), known in known_objects.items():
        remove_resources(single, plural, known, role=role)
