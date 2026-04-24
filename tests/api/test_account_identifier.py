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

from snowflake.cli.api.identifiers import AccountIdentifier


class TestAccountIdentifier:
    def test_constructor_uppercases_fields(self):
        identifier = AccountIdentifier(
            organization_name="my_org", account_name="my_account"
        )
        assert identifier.organization_name == "MY_ORG"
        assert identifier.account_name == "MY_ACCOUNT"

    def test_as_hyphen_form(self):
        identifier = AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        )
        assert identifier.as_hyphen_form() == "MY_ORG-MY_ACCOUNT"

    def test_as_dot_form(self):
        identifier = AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        )
        assert identifier.as_dot_form() == "MY_ORG.MY_ACCOUNT"

    def test_str_returns_hyphen_form(self):
        identifier = AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        )
        assert str(identifier) == "MY_ORG-MY_ACCOUNT"

    def test_matches_hyphen_form(self):
        identifier = AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        )
        assert identifier.matches("MY_ORG-MY_ACCOUNT")

    def test_matches_dot_form(self):
        identifier = AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        )
        assert identifier.matches("MY_ORG.MY_ACCOUNT")

    def test_matches_is_case_insensitive(self):
        identifier = AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        )
        assert identifier.matches("my_org-my_account")
        assert identifier.matches("my_org.my_account")

    def test_matches_returns_false_for_different_account(self):
        identifier = AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        )
        assert not identifier.matches("OTHER_ORG-OTHER_ACCOUNT")
        assert not identifier.matches("OTHER_ORG.OTHER_ACCOUNT")

    def test_matches_returns_false_for_missing_component(self):
        identifier = AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        )
        assert not identifier.matches("MY_ORG")
        assert not identifier.matches("MY_ACCOUNT")

    def test_matches_returns_false_for_string_without_separator(self):
        identifier = AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        )
        assert not identifier.matches("MY_ORG_MY_ACCOUNT")

    def test_equality_of_same_components(self):
        assert AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        ) == AccountIdentifier(organization_name="MY_ORG", account_name="MY_ACCOUNT")

    def test_inequality_of_different_components(self):
        assert AccountIdentifier(
            organization_name="MY_ORG", account_name="MY_ACCOUNT"
        ) != AccountIdentifier(organization_name="OTHER_ORG", account_name="MY_ACCOUNT")
