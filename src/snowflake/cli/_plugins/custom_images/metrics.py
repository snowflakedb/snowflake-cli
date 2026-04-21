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

_CIV = "features.global.custom_image_validate"


class CustomImageCounterField:
    CUSTOM_IMAGE_VALIDATE = _CIV
    CUSTOM_IMAGE_VALIDATE_FAILED = f"{_CIV}_failed"
    CUSTOM_IMAGE_VALIDATE_FAIL_IMAGE_NOT_FOUND = f"{_CIV}_fail_image_not_found"
    CUSTOM_IMAGE_VALIDATE_FAIL_ENTRYPOINT = f"{_CIV}_fail_entrypoint"
    CUSTOM_IMAGE_VALIDATE_FAIL_ENV_VARS = f"{_CIV}_fail_env_vars"
    CUSTOM_IMAGE_VALIDATE_FAIL_PYTHON_PACKAGES = f"{_CIV}_fail_python_packages"
    CUSTOM_IMAGE_VALIDATE_FAIL_DEPENDENCY_HEALTH = f"{_CIV}_fail_dependency_health"
    CUSTOM_IMAGE_VALIDATE_FAIL_REQUIRED_SCRIPTS = f"{_CIV}_fail_required_scripts"
