# coding: utf-8

"""
    LSP Contracts

    No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)

    The version of the OpenAPI document: 1.0.0
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


import unittest

from lsp_api_contracts.models.context import Context

class TestContext(unittest.TestCase):
    """Context unit test stubs"""

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def make_instance(self, include_optional) -> Context:
        """Test Context
            include_optional is a boolean, when False only required
            params are included, when True both required and
            optional params are included """
        # uncomment below to create an instance of `Context`
        """
        model = Context()
        if include_optional:
            return Context(
                connection = lsp_api_contracts.models.connection.Connection(
                    account = '', 
                    user = '', ),
                env = {
                    'key' : ''
                    },
                project_path = ''
            )
        else:
            return Context(
        )
        """

    def testContext(self):
        """Test Context"""
        # inst_req_only = self.make_instance(include_optional=False)
        # inst_req_and_optional = self.make_instance(include_optional=True)

if __name__ == '__main__':
    unittest.main()
