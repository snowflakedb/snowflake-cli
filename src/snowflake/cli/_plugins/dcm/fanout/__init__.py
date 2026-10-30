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
"""Paced fan-out of `snow dcm plan` / `snow dcm deploy` across many targets.

Multi-tenant projects provision one DCM project per tenant from a shared
template, so a run may need to issue hundreds or thousands of independent
invocations. Snowflake enforces an account-level cap on concurrent Jinja2
templating operations, and that cap is triggered by invocation *starts* landing
together rather than by how many invocations are in flight. Spacing the starts
avoids it, which is what this package implements.
"""
