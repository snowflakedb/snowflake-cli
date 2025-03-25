from snowflake.cli.api.secret import SecretType


def test_secret_type():
    secret_type = SecretType("secret")

    assert str(secret_type) == "***"
    assert repr(secret_type) == "SecretType(***)"
    assert f"{secret_type}" == "***"
    assert "{}".format(secret_type) == "***"
    assert "%s" % secret_type == "***"
