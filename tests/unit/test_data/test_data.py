# test_utils.py
import os

bad_arguments_for_yesnoask = ['Yes', 'No', 'Ask', 'yse', 42, 'and_now_for_something_completely_different']

positive_arguments_for_deploy_names = [
    (('snowhouse_test', 'test_schema', 'jdoe'), {
        'stage': 'snowhouse_test.test_schema.deployments',
        'path': '/jdoe/app.zip',
        'full_path': '@snowhouse_test.test_schema.deployments/jdoe/app.zip',
        'directory': '/jdoe'
    })
]

positive_arguments_for_prepareappzip = [
    (('c:\\app.zip','temp'), 'c:\\temp\\app.zip')
]
