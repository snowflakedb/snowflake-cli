import typer

from snowcli.cli.common.decorators import global_options_with_connection, global_options

_KNOWN_SIG_GLOBAL_PARAMETERS_WITH_CONNECTION = [
    "connection",
    "account",
    "user",
    "password",
    "database",
    "schema",
    "role",
    "warehouse",
    "format",
    "verbose",
    "debug",
]

_KNOWN_SIG_GLOBAL_PARAMETERS = [
    "format",
    "verbose",
    "debug",
]


def _extract_arguments(func):
    return list(dict(func.__signature__.parameters).keys())


def test_global_options_with_connection_decorator_no_options_passed():
    test_app = typer.Typer()

    @test_app.command("foo")
    @global_options_with_connection
    def func(**options):
        return options

    assert _extract_arguments(func) == _KNOWN_SIG_GLOBAL_PARAMETERS_WITH_CONNECTION
    assert func() == {}


def test_global_options_with_connection_decorator_connection_options_passed():
    test_app = typer.Typer()

    @test_app.command("foo")
    @global_options_with_connection
    def func(**options):
        return options

    assert func(connection="connName", user="userValue") == dict(
        connection="connName", user="userValue"
    )


def test_global_options_with_connection_decorator_on_function_with_arguments():
    test_app = typer.Typer()

    @test_app.command("foo")
    @global_options_with_connection
    def func(name: str, **options):
        return options

    assert _extract_arguments(func) == [
        "name",
        *_KNOWN_SIG_GLOBAL_PARAMETERS_WITH_CONNECTION,
    ]
    assert func(name="solaris", connection="connName", user="userValue") == dict(
        connection="connName", user="userValue"
    )


def test_global_options_with_connection_decorator_on_function_with_arguments_without_typehints():
    test_app = typer.Typer()

    @test_app.command("foo")
    @global_options_with_connection
    def func(name, **options):
        return options

    assert _extract_arguments(func) == [
        "name",
        *_KNOWN_SIG_GLOBAL_PARAMETERS_WITH_CONNECTION,
    ]


def test_global_options_decorator_no_options_passed():
    test_app = typer.Typer()

    @test_app.command("foo")
    @global_options
    def func(**options):
        return options

    assert _extract_arguments(func) == _KNOWN_SIG_GLOBAL_PARAMETERS
    assert func() == {}


def test_global_options_decorator_connection_options_passed():
    test_app = typer.Typer()

    @test_app.command("foo")
    @global_options
    def func(**options):
        return options

    assert func(format="JSON", verbose=True) == dict(format="JSON", verbose=True)


def test_global_options_decorator_on_function_with_arguments():
    test_app = typer.Typer()

    @test_app.command("foo")
    @global_options
    def func(name: str, **options):
        return options

    assert _extract_arguments(func) == ["name", *_KNOWN_SIG_GLOBAL_PARAMETERS]
    assert func(name="solaris", format="JSON", verbose=True) == dict(
        format="JSON", verbose=True
    )


def test_global_options_decorator_on_function_with_arguments_without_typehints():
    test_app = typer.Typer()

    @test_app.command("foo")
    @global_options
    def func(name, **options):
        return options

    assert _extract_arguments(func) == ["name", *_KNOWN_SIG_GLOBAL_PARAMETERS]
