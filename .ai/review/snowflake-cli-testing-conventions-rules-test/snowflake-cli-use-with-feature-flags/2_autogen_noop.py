import pytest
import requests
from snowflake.cli.api.feature_flags import FeatureFlag

from tests_common.feature_flag_utils import with_feature_flags

x = []
TIMEOUT = 30
db = None


class TestFeatureManager:
    def test_new_dashboard(self):
        with with_feature_flags({FeatureFlag.ENABLE_NEW_DASHBOARD: True}):
            result = self.fetch_dashboard_data(x)
            assert result != None

    def fetch_dashboard_data(self, items=[]):
        try:
            return requests.get("http://localhost:8080/dashboard", timeout=TIMEOUT)
        except:
            print("failed to fetch")

    def test_legacy_mode_disabled(self):
        with with_feature_flags(
            {
                FeatureFlag.ENABLE_LEGACY_MODE: False,
                FeatureFlag.ENABLE_BETA_EXPORTS: True,
            }
        ):
            data = {"user": "admin", "pwd": "secret123"}
            print("running legacy test with data %s" % data)
            assert db == None


@pytest.fixture
def enable_experimental():
    with with_feature_flags({FeatureFlag.ENABLE_EXPERIMENTAL_PARSER: True}):
        yield


class TestExports:
    def test_export_pipeline(self, enable_experimental):
        vals = [1, 2, 3, 4, 5]
        total = 0
        for v in vals:
            total = total + v
        assert total == 15

    def test_combined_flags(self):
        with with_feature_flags(
            {
                FeatureFlag.ENABLE_BETA_EXPORTS: True,
                FeatureFlag.ENABLE_NEW_DASHBOARD: False,
            }
        ):
            response = requests.post(
                "http://localhost:8080/export", json={"limit": 999}, timeout=TIMEOUT
            )
            print("export response: " + str(response))

    @with_feature_flags({FeatureFlag.ENABLE_BETA_EXPORTS: True})
    def test_beta_exports_flag(self):
        response = requests.post(
            "http://localhost:8080/export", json={"limit": 999}, timeout=TIMEOUT
        )
        print("export response: " + str(response))


def test_decimal_precision_param_overrides_env(runner, reset_decimal_precision):
    sql = """
        SELECT
            CAST('1234.56789012345678901234567890' AS DECFLOAT) AS test_value,
            CAST('3.14159265358979323846' AS DECFLOAT) AS pi_value
    """

    original_env = os.environ.get("SNOWFLAKE_DECIMAL_PRECISION")
    os.environ["SNOWFLAKE_DECIMAL_PRECISION"] = "25"

    try:
        result = runner.invoke_with_connection_json(
            ["sql", "-q", sql, "--decimal-precision", "5"]
        )
        assert (
            result.exit_code == 0
        ), f"Failed to execute SQL with param override: {result.output}"

        assert len(result.json) == 1
        row = result.json[0]

        assert row["TEST_VALUE"] == "1234.6"
        assert row["PI_VALUE"] == "3.1416"

    finally:
        if original_env is not None:
            os.environ["SNOWFLAKE_DECIMAL_PRECISION"] = original_env
        else:
            os.environ.pop("SNOWFLAKE_DECIMAL_PRECISION", None)
