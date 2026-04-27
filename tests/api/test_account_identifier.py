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

import pytest
from snowflake.cli.api.identifiers import AccountIdentifier


class TestAccountIdentifier:
    def test_constructor_uppercases_fields(self):
        identifier = AccountIdentifier(
            organization_name="my_org", account_name="my_account"
        )
        assert identifier.organization_name == "MY_ORG"
        assert identifier.account_name == "MY_ACCOUNT"

    def test_str_returns_hyphen_form(self):
        identifier = AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        )
        assert str(identifier) == "MY_ORG-MY_ACCOUNT"

    def test_equality_of_same_components(self):
        assert AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        ) == AccountIdentifier(organization_name="MY_ORG", account_name="MY_ACCOUNT")

    def test_inequality_of_different_components(self):
        assert AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        ) != AccountIdentifier(organization_name="OTHER_ORG", account_name="MY_ACCOUNT")

    @pytest.mark.parametrize(
        "identifier_string,expected",
        [
            ("MY_ORG-MY_ACCOUNT", AccountIdentifier("MY_ORG", "MY_ACCOUNT")),
            ("MY_ORG.MY_ACCOUNT", AccountIdentifier("MY_ORG", "MY_ACCOUNT")),
            ("my_org-my_account", AccountIdentifier("MY_ORG", "MY_ACCOUNT")),
            ("", AccountIdentifier("", "")),
            ("NOSEPARATOR", AccountIdentifier("NOSEPARATOR", "")),
            ("ORG-ACCT-EXTRA", AccountIdentifier("ORG", "ACCT-EXTRA")),
            ("ORG.ACCT.EXTRA", AccountIdentifier("ORG", "ACCT.EXTRA")),
            ("-", AccountIdentifier("", "")),
            (".", AccountIdentifier("", "")),
            ("   ", AccountIdentifier("   ", "")),
        ],
        ids=[
            "hyphen",
            "dot",
            "case_insensitive",
            "empty",
            "no_separator",
            "multi_hyphen",
            "multi_dot",
            "just_hyphen",
            "just_dot",
            "whitespace",
        ],
    )
    def test_from_string(self, identifier_string, expected):
        assert AccountIdentifier.from_string(identifier_string) == expected
