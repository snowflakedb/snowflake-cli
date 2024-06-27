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

SPECIAL_COMMENT_OLD = "GENERATED_BY_SNOWCLI"
SPECIAL_COMMENT = "GENERATED_BY_SNOWFLAKECLI"
ALLOWED_SPECIAL_COMMENTS = {SPECIAL_COMMENT, SPECIAL_COMMENT_OLD}
LOOSE_FILES_MAGIC_VERSION = "UNVERSIONED"

NAME_COL = "name"
COMMENT_COL = "comment"
OWNER_COL = "owner"
VERSION_COL = "version"
PATCH_COL = "patch"

INTERNAL_DISTRIBUTION = "internal"
EXTERNAL_DISTRIBUTION = "external"

ERROR_MESSAGE_2003 = "does not exist or not authorized"
ERROR_MESSAGE_2043 = "Object does not exist, or operation cannot be performed."
ERROR_MESSAGE_606 = "No active warehouse selected in the current session."
ERROR_MESSAGE_093079 = "Application is no longer available for use"
ERROR_MESSAGE_093128 = "The application owns one or more objects within the account"
