from unittest import mock

from snowflake.cli.__about__ import VERSION
from snowflake.connector.version import VERSION as DRIVER_VERSION


@mock.patch(
    "snowflake.cli.app.telemetry.python_version",
)
@mock.patch("snowflake.cli.app.telemetry.platform.platform")
@mock.patch("snowflake.cli.app.telemetry.get_time_millis")
@mock.patch("snowflake.connector.connect")
@mock.patch("snowflake.cli.plugins.connection.commands.ObjectManager")
def test_executing_command_sends_telemetry_data(
    _, mock_conn, mock_time, mock_platform, mock_version, runner
):
    mock_time.return_value = "123"
    mock_platform.return_value = "FancyOS"
    mock_version.return_value = "2.3.4"

    result = runner.invoke(["connection", "test"], catch_exceptions=False)
    assert result.exit_code == 0, result.output

    # The method is called with a TelemetryData type, so we cast it to dict for simpler comparison
    assert mock_conn.return_value._telemetry.try_add_log_to_batch.call_args.args[  # noqa: SLF001
        0
    ].to_dict() == {
        "message": {
            "driver_type": "PythonConnector",
            "driver_version": ".".join(str(s) for s in DRIVER_VERSION[:3]),
            "source": "snowcli",
            "version_cli": VERSION,
            "version_os": "FancyOS",
            "version_python": "2.3.4",
            "command": ["connection", "test"],
            "command_group": "connection",
            "command_flags": {"diag_log_path": "DEFAULT", "format": "DEFAULT"},
            "command_output_type": "TABLE",
            "type": "executing_command",
        },
        "timestamp": "123",
    }
