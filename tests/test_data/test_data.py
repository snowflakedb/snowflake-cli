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

from snowflake.cli._plugins.snowpark.models import Requirement

requirements = [
    "pytest==1.0.0",
    "snowflake-sqlalchemy>=3.2.1",
    "Snowflake_Connector_Python<3.3.3",
]

packages = [
    Requirement.parse_line("snowflake-connector-python"),
    Requirement.parse_line("snowflake-snowpark-python"),
    Requirement.parse_line("my-totally-awesome-package"),
]

excluded_anaconda_deps = ["pytest==1.0.0"]

correct_package_metadata = [
    "Metadata-Version: 2.1",
    "Name: my-awesome-package",
    "Version: 0.0.1",
    "Requires-Dist: requests===2.28.1",
    "Requires-Dist: snowflake-connector-python==3.0.2",
    "Requires-Dist: snowflake-snowpark-python==1.1.0",
    "Provides-Extra: dev",
    "Requires-Dist: pytest; extra == 'dev'",
]

anaconda_response = {
    "channeldata_version": 1,
    "packages": {
        "anaconda-clean": {
            "activate.d": False,
            "binary_prefix": False,
            "deactivate.d": False,
            "description": "anaconda-clean removes configuration files and directories from Anaconda and its programs.",
            "dev_url": "https://github.com/ContinuumIO/anaconda-clean",
            "doc_source_url": "https://github.com/ContinuumIO/anaconda-clean/blob/master/README.md",
            "doc_url": "https://github.com/ContinuumIO/anaconda-clean",
            "home": "https://github.com/ContinuumIO/anaconda-clean",
            "license": "BSD-3-Clause",
            "post_link": False,
            "pre_link": False,
            "pre_unlink": False,
            "run_exports": {},
            "source_git_url": "https://github.com/ContinuumIO/anaconda-clean",
            "source_url": "https://github.com/ContinuumIO/anaconda-clean/archive/refs/tags/1.1.1.tar.gz",
            "subdirs": ["linux-64", "linux-aarch64", "osx-64", "osx-arm64", "win-64"],
            "summary": "Delete Anaconda configuration files",
            "text_prefix": True,
            "timestamp": 1652723897,
            "version": "1.1.1",
        },
        "snowflake-connector-python": {
            "activate.d": False,
            "binary_prefix": True,
            "deactivate.d": False,
            "description": "The Snowflake Connector for Python provides an interface for developing Python applications that can connect to Snowflake and perform all standard operations. It provides a programming alternative to developing applications in Java or C/C++ using the Snowflake JDBC or ODBC drivers.",
            "dev_url": "https://github.com/snowflakedb/snowflake-connector-python",
            "doc_url": "https://docs.snowflake.net/manuals/user-guide/python-connector.html",
            "home": "https://github.com/snowflakedb/snowflake-connector-python",
            "license": "Apache-2.0",
            "post_link": False,
            "pre_link": False,
            "pre_unlink": False,
            "run_exports": {},
            "source_url": "https://codeload.github.com/snowflakedb/snowflake-connector-python/tar.gz/v2.7.12",
            "subdirs": ["linux-64", "linux-aarch64", "osx-64", "osx-arm64", "win-64"],
            "summary": "Snowflake Connector for Python",
            "text_prefix": True,
            "timestamp": 1662317582,
            "version": "2.7.12",
        },
        "streamlit": {
            "name": "streamlit",
            "subdirs": ["osx-64", "linux-64", "osx-arm64", "linux-aarch64", "win-64"],
            "version": "1.22.0",
            "license": "Apache-2.0",
            "md5": "a6e6f90bb4d3fef3a2f5778ec7b5196c",
            "timestamp": 1701377760,
        },
        "package-with-non-pep-version": {
            "version": "4d",
            "license": "Apache-2.0",
            "md5": "0bb4d3fef3a4d3fef3a2f5778e345d77",
            "timestamp": 1701393763,
        },
    },
}

describe_function_response = """[
  {
    "property": "signature",
    "value": "()"
  },
  {
    "property": "returns",
    "value": "VARCHAR(16777216)"
  },
  {
    "property": "language",
    "value": "PYTHON"
  },
  {
    "property": "null handling",
    "value": "CALLED ON NULL INPUT"
  },
  {
    "property": "volatility",
    "value": "VOLATILE"
  },
  {
    "property": "body",
    "value": null
  },
  {
    "property": "imports",
    "value": "[@test_snowpark_shared/app.zip]"
  },
  {
    "property": "handler",
    "value": "app.hello"
  },
  {
    "property": "runtime_version",
    "value": "3.10"
  },
  {
    "property": "packages",
    "value": "["snowpark-connector-python"]"
  },
  {
    "property": "installed_packages",
    "value": "['python==3.10.17']"
  }
]"""

definition_v2_duplicated_entity_names = """
definition_version: 2
entities:  
  hello_world:
    artifacts:
      - app/
    handler: hello
    identifier:
      name: test_procedure
    returns: string
    signature:
      - name: "name"
        type: "string"
    stage: dev_deployment
    type: procedure
  hello_world:
    artifacts:
      - app/
    handler: hello
    identifier:
      name: test_procedure
    returns: string
    signature:
      - name: "name"
        type: "string"
    stage: dev_deployment
    type: function
"""
