from snowflake.cli.plugins.snowpark.models import Requirement
from snowflake.cli.plugins.snowpark.package.anaconda import AnacondaChannel

ANACONDA = AnacondaChannel(
    packages={
        "SHRUBBERY": {"1.2.1", "1.2.2"},
        "dummy-pkg": {"0.1.1", "1.0.0", "1.1.0"},
        "jpeg": {"9e", "9d", "9b"},
    }
)


def _r(line):
    return Requirement.parse(line)


def test_latest_version():
    assert ANACONDA.package_latest_version(_r("shrubbery")) == "1.2.2"
    assert ANACONDA.package_latest_version(_r("dummy_pkg")) == "1.1.0"
    assert ANACONDA.package_latest_version(_r("dummy-pkg")) == "1.1.0"
    assert ANACONDA.package_latest_version(_r("jpeg")) == "9e"
    assert ANACONDA.package_latest_version(_r("weird-pkg")) is None


def test_package_availability():
    assert ANACONDA.is_package_available(_r("shrubbery"))
    assert ANACONDA.is_package_available(_r("DUMMY_pkg"))
    assert not ANACONDA.is_package_available(_r("shrubbery>=2"))
    assert not ANACONDA.is_package_available(_r("shrubbery<=1.0.0"))
    assert ANACONDA.is_package_available(_r("shrubbery==1.2.1"))
    assert ANACONDA.is_package_available(_r("shrubbery>1,<4"))
    assert not ANACONDA.is_package_available(_r("shrubbery!=1.2.*"))
    assert ANACONDA.is_package_available(_r("dummy-pkg==0.*"))
    assert not ANACONDA.is_package_available(_r("weird-package"))
    assert ANACONDA.is_package_available(_r("jpeg"))

    # safe-fail for non-pep508 version formats
    assert not ANACONDA.is_package_available(_r("jpeg==9d"))
