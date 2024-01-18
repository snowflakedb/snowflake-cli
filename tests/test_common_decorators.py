import typer
from snowflake.cli.api.commands.decorators import (
    global_options,
    global_options_with_connection,
    with_experimental_behaviour,
)

_KNOWN_SIG_GLOBAL_PARAMETERS_WITH_CONNECTION = [
    "connection",
    "account",
    "user",
    "password",
    "authenticator",
    "private_key_path",
    "database",
    "schema",
    "role",
    "warehouse",
    "temporary_connection",
    "format",
    "verbose",
    "debug",
    "silent",
]

_KNOWN_SIG_GLOBAL_PARAMETERS = ["format", "verbose", "debug", "silent"]


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


def test_experimental_decorator_as_standalone_decorator_of_function_without_arguments():
    test_app = typer.Typer()

    @test_app.command("foo")
    @with_experimental_behaviour()
    def func(**options):
        return options

    assert _extract_arguments(func) == ["experimental"]
    assert func(experimental=True) == dict(experimental=True)


def test_experimental_decorator_as_standalone_decorator_of_function_with_arguments():
    test_app = typer.Typer()

    @test_app.command("foo")
    @with_experimental_behaviour()
    def func(name: str, **options):
        return options

    assert _extract_arguments(func) == ["name", "experimental"]
    assert func(name="solaris", experimental=True) == dict(experimental=True)


def test_experimental_decorator_below_global_options_decorator_for_function_without_arguments():
    test_app = typer.Typer()

    @test_app.command("foo")
    @global_options
    @with_experimental_behaviour()
    def func(**options):
        return options

    assert _extract_arguments(func) == ["experimental", *_KNOWN_SIG_GLOBAL_PARAMETERS]
    assert func(experimental=True, format="JSON") == dict(
        experimental=True, format="JSON"
    )


def test_experimental_decorator_below_global_options_decorator_for_function_with_arguments():
    test_app = typer.Typer()

    @test_app.command("foo")
    @global_options
    @with_experimental_behaviour()
    def func(name: str, **options):
        return options

    assert _extract_arguments(func) == [
        "name",
        "experimental",
        *_KNOWN_SIG_GLOBAL_PARAMETERS,
    ]
    assert func(name="solaris", experimental=True, format="JSON") == dict(
        experimental=True, format="JSON"
    )


def test_experimental_decorator_above_global_options_decorator_for_function_without_arguments():
    test_app = typer.Typer()

    @test_app.command("foo")
    @with_experimental_behaviour()
    @global_options
    def func(**options):
        return options

    assert _extract_arguments(func) == [*_KNOWN_SIG_GLOBAL_PARAMETERS, "experimental"]
    assert func(experimental=True, format="JSON") == dict(
        experimental=True, format="JSON"
    )


def test_experimental_decorator_above_global_options_decorator_for_function_with_arguments():
    test_app = typer.Typer()

    @test_app.command("foo")
    @with_experimental_behaviour()
    @global_options
    def func(name: str, **options):
        return options

    assert _extract_arguments(func) == [
        "name",
        *_KNOWN_SIG_GLOBAL_PARAMETERS,
        "experimental",
    ]
    assert func(name="solaris", experimental=True, format="JSON") == dict(
        experimental=True, format="JSON"
    )
