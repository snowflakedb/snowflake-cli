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

# JVM code bundle test artifacts

Prebuilt jars used by the `language: java` / `language: scala` code bundle
integration tests in `tests_integration/test_bundle.py`. A JVM code bundle is
executed with `--entrypoint <fully.qualified.MainClass>` rather than a file
path, so the tests need a jar that really loads.

| File | Main class | What it does |
|------|-----------|--------------|
| `scos-jvm-hello_2.12-1.0.0.jar` | `com.snowflake.scos.test.ScosJvmHelloApp` | Writes one row, `SCOS_JVM_OK`, into the table named by the first argument. |
| `scos-jvm-args_2.12-1.0.0.jar` | `com.snowflake.scos.test.ScosJvmArgsApp` | Writes one row per remaining argument (`idx`, `arg`) into the table named by the first argument. |

The `.scala` files next to each jar are the sources the jars were built from.
They are not loaded at runtime; they are here so the binaries can be audited and
rebuilt:

```bash
scalac -classpath <spark-connect-client_2.12.jar> ScosJvmHelloApp.scala
jar cf scos-jvm-hello_2.12-1.0.0.jar com/
```

Both apps connect through `SparkSession.builder().getOrCreate()`, so they only
run inside a `type: spark`, `compute_type: warehouse` bundle. The apps also read
`SCOS_ENV_A` / `SCOS_ENV_B` and `SNOWPARK_SUBMIT_SPARK_APPLICATION_ID` into extra
columns; the CLI tests ignore those columns.
