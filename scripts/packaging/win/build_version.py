from snowflake.cli.__about__ import VERSION


def parse_version_for_windows_build() -> list[str]:
    """Convert sematntic version to windows installer acceptable version.

    Windows installer internal version is in the format of 4 integers separated by dots.
    """
    version = VERSION.split(".")
    *msv, last = version

    match last:
        case last if last.isdigit():
            version.append("0")
        case last if "rc" in last:
            version = msv + last.split("rc")
        case last if "dev" in last:
            version = msv + last.split("dev")

    return [segment for segment in version if segment]


if __name__ == "__main__":
    version = parse_version_for_windows_build()
    print(".".join(version))
