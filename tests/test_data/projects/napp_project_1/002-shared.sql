/*
 Copyright (c) 2024 Snowflake Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
*/

-- package script (2/2)

create or replace table {{ package_name }}.my_shared_content.shared_table (
  col1 number,
  col2 varchar
);
grant select on table {{ package_name }}.my_shared_content.shared_table
  to share in application package {{ package_name }};
