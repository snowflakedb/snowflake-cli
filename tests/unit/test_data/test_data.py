# test_utils.py
import os

bad_arguments_for_yesnoask = [
    "Yes",
    "No",
    "Ask",
    "yse",
    42,
    "and_now_for_something_completely_different",
]

positive_arguments_for_deploy_names = [
    (
        ("snowhouse_test", "test_schema", "jdoe"),
        {
            "stage": "snowhouse_test.test_schema.deployments",
            "path": "/jdoe/app.zip",
            "full_path": "@snowhouse_test.test_schema.deployments/jdoe/app.zip",
            "directory": "/jdoe",
        },
    )
]

positive_arguments_for_prepareappzip = [(("c:\\app.zip", "temp"), "c:\\temp\\app.zip")]

requirements = ["pytest==1.0.0", "Django==3.2.1", "awesome_lib==3.3.3"]

packages = [
    "snowflake-connector-python",
    "snowflake-snowpark-python",
    "my-totally-awesome-package",
]

correct_package_metadata = """
 package:
  Name: my-awesome-package
  version: 1.2.3

source:
  url: https://snowflake.com

build:
  noarch: python
  number: 0
  script: python -m pip install --no-deps --ignore-installed .
"""
