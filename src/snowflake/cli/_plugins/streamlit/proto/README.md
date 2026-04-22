<!--
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
 -->

## Streamlit developer log streaming protobuf definitions

This directory contains the protobuf definitions used by the
`snow streamlit logs` command to talk to the Streamlit container runtime's
developer log service.

### Files

- `developer/v1/logs_service.proto` — source proto definition (authoritative).
- `generated/developer/v1/logs_service_pb2.py` — generated Python bindings.
  Do not edit by hand.

### When to regenerate

Regenerate `logs_service_pb2.py` any time `logs_service.proto` is modified
(new fields, enum values, messages, etc.) or when bumping the protobuf
toolchain. The generated file must be committed alongside the proto change
in the same PR.

### How to regenerate

From the repository root:

```bash
pip install 'grpcio-tools>=1.60'

python -m grpc_tools.protoc \
  --proto_path=src/snowflake/cli/_plugins/streamlit/proto \
  --python_out=src/snowflake/cli/_plugins/streamlit/proto/generated \
  developer/v1/logs_service.proto
```

### Post-generation patch (required)

The default code emitted by `protoc` calls
`google.protobuf.runtime_version.ValidateProtobufRuntimeVersion(...)` at
import time. That symbol only exists in `protobuf>=5.26`. Because
`snowflake-connector-python` may pin `protobuf<5`, we must keep the runtime
check tolerant to older versions. After regenerating, re-apply the
`try / except Exception: pass` wrapper around the
`ValidateProtobufRuntimeVersion` call at the top of the generated file
(see the existing `logs_service_pb2.py` for the exact shape).

### Source of truth

The proto definition is mirrored from
`github.com/snowflakedb/streamlit-container-runtime/gen/developer/v1` — if
the server-side definition changes, update `logs_service.proto` here to
match before regenerating.
