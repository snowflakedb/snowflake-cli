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
CHANNEL_COL = "release_channel_name"
AUTHORIZE_TELEMETRY_COL = "authorize_telemetry_event_sharing"

INTERNAL_DISTRIBUTION = "internal"
EXTERNAL_DISTRIBUTION = "external"

DEFAULT_CHANNEL = "DEFAULT"
DEFAULT_DIRECTIVE = "DEFAULT"
MAX_VERSIONS_IN_RELEASE_CHANNEL = 2
