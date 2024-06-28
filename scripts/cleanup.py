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

from snowflake.core import Root
from snowflake.core.compute_pool import ComputePoolResource
from snowflake.core.database import DatabaseResource
from snowflake.core.exceptions import APIError, UnauthorizedError
from snowflake.core.service import ServiceResource
from snowflake.snowpark.session import Session


def resource_delete(resource_type, name: str, collection):
    msg = f"Deleting {resource_type.__name__} {name}"
    try:
        resource_type(name=name, collection=collection).delete()
        print("SUCCESS", msg)
    except UnauthorizedError:
        print(f"Insufficient privileges to operate on {name}")
    except APIError as err:
        print("ERROR", msg)
        print(err.body)


if __name__ == "__main__":
    session = Session.builder.configs(
        {
            "account": os.getenv("SNOWFLAKE_CONNECTIONS_INTEGRATION_ACCOUNT"),
            "user": os.getenv("SNOWFLAKE_CONNECTIONS_INTEGRATION_USER"),
            "password": os.getenv("SNOWFLAKE_CONNECTIONS_INTEGRATION_PASSWORD"),
            "role": "INTEGRATION_TESTS",
        }
    ).create()

    session.use_role("INTEGRATION_TESTS")

    root = Root(session)
    known_objects: t.Dict[t.Any, t.List[str]] = {
        (root.compute_pools, ComputePoolResource): [],
        (root.databases, DatabaseResource): [
            "cleanup_db",
            "docs_testing",
            "external_access_db",
            "snowcli_db",
            "notebooks",
        ],
        (root.databases["SNOWCLI_DB"].schemas["public"].services, ServiceResource): [],
    }
    for (collection, resource_type), known in known_objects.items():
        for item in collection.iter():
            if item.name not in known:
                resource_delete(resource_type, item.name, collection)

    # Drop apps
    apps = session.sql("show applications").collect()
    for app in apps:
        try:
            session.sql("drop application ?", params=app.name)
            print(f"SUCCESS deleting {app.name}")
        except:
            print(f"ERROR deleting {app.name}")
