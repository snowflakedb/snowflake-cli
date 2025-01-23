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


from snowflake.cli.api.project.util import to_identifier


def get_org_and_account_name(runner):
    """
    Get the current account and organization name.
    Returns:
        str: The current account and organization name in the format of
            <ORG_NAME>.<ACCOUNT_NAME>
    If the account name or organization name contains non SQL-safe characters, they will be quoted.
    """
    result = runner.invoke_with_connection_json(
        ["sql", "-q", "select current_account_name(), current_organization_name()"]
    )
    assert result.exit_code == 0
    account_name = result.json[0]["CURRENT_ACCOUNT_NAME()"]
    org_name = result.json[0]["CURRENT_ORGANIZATION_NAME()"]

    return f"{to_identifier(org_name)}.{to_identifier(account_name)}"
