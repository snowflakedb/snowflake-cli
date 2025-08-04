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

from typing import Literal

PARAM_APPLICATION_NAME: Literal["snowcli"] = "snowcli"

# This is also defined on server side. Changing this parameter would require
# a change in https://github.com/snowflakedb/snowflake
INTERNAL_APPLICATION_NAME: Literal["SNOWFLAKE_CLI"] = "SNOWFLAKE_CLI"

# Authenticator types
AUTHENTICATOR_WORKLOAD_IDENTITY: Literal["WORKLOAD_IDENTITY"] = "WORKLOAD_IDENTITY"
AUTHENTICATOR_SNOWFLAKE_JWT: Literal["SNOWFLAKE_JWT"] = "SNOWFLAKE_JWT"
AUTHENTICATOR_USERNAME_PASSWORD_MFA: Literal[
    "username_password_mfa"
] = "username_password_mfa"
AUTHENTICATOR_OAUTH_AUTHORIZATION_CODE: Literal[
    "OAUTH_AUTHORIZATION_CODE"
] = "OAUTH_AUTHORIZATION_CODE"
