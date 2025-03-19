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

import unittest.mock as mock
from typing import Any, Dict

import factory
import pytest
from snowflake.cli._plugins.nativeapp.codegen.snowpark.models import (
    NativeAppExtensionFunction,
)
from snowflake.cli._plugins.nativeapp.entities.application import (
    ApplicationEntity,
    ApplicationEntityModel,
)
from snowflake.cli._plugins.nativeapp.entities.application_package import (
    ApplicationPackageEntity,
    ApplicationPackageEntityModel,
)
from snowflake.cli.api.artifacts.bundle_map import BundleMap

from tests.nativeapp.factories import (
    ApplicationEntityModelFactory,
    ApplicationPackageEntityModelFactory,
)


@pytest.fixture()
def mock_bundle_map():
    yield mock.Mock(spec=BundleMap)


@pytest.fixture()
def application_package_entity(workspace_context, request) -> ApplicationPackageEntity:
    pkg_params = getattr(request, "param", {})
    data = ApplicationPackageEntityModelFactory(
        identifier=factory.Faker("word"), **pkg_params
    )
    model = ApplicationPackageEntityModel(**data)
    return ApplicationPackageEntity(model, workspace_context)


@pytest.fixture()
def application_entity(workspace_context, request):
    app_params = getattr(request, "param", {})
    data = ApplicationEntityModelFactory(identifier=factory.Faker("word"), **app_params)
    model = ApplicationEntityModel(**data)
    return ApplicationEntity(model, workspace_context)


@pytest.fixture
def native_app_extension_function_raw_data() -> Dict[str, Any]:
    return {
        "type": "procedure",
        "lineno": 42,
        "name": "my_function",
        "signature": [{"name": "first", "type": "int", "default": "42"}],
        "returns": "int",
        "runtime": "3.11",
        "handler": "my_function_handler",
        "external_access_integrations": ["integration_one", "integration_two"],
        "secrets": {"key1": "secret_one", "key2": "integration_two"},
        "packages": ["package_one==1.0.2", "package_two"],
        "imports": ["/path/to/import1.py", "/path/to/import2.zip"],
        "execute_as_caller": False,
        "schema": "DATA",
        "application_roles": ["APP_ADMIN", "APP_VIEWER"],
    }


@pytest.fixture
def native_app_extension_function(
    native_app_extension_function_raw_data,
) -> NativeAppExtensionFunction:
    return NativeAppExtensionFunction(**native_app_extension_function_raw_data)
