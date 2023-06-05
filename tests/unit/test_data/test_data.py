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

excluded_anaconda_deps = ["pytest==1.0.0"]

correct_package_metadata = """
Metadata-Version: 2.1
Name: my-awesome-package
Version: 0.0.1
Requires-Dist: requests===2.28.1
Requires-Dist: snowflake-connector-python==3.0.2
Requires-Dist: snowflake-snowpark-python==1.1.0
Provides-Extra: dev
Requires-Dist: pytest; extra == 'dev'
"""

example_resource_details = [
    ("packages", "{'name': 'my-awesome-package','version': '1.2.3'}"),
    ("handler", "handler_function")
]

expected_resource_dict = {
    "packages": {"name": "my-awesome-package",
                "version": "1.2.3"},
    "handler": "handler_function"
}
