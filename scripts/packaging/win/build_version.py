from snowflake.cli.__about__ import VERSION


def parse_version_for_windows_build() -> list[str]:
    version = VERSION.split(".")
    *msv, last = version

    match last:
        case last if last.isdigit():
            version.append("0")
        case last if "rc" in last:
            version = msv + last.split("rc")
        case last if "dev" in last:
            version = msv + last.split("dev")

    return version


if __name__ == "__main__":
    version = parse_version_for_windows_build()
    print(".".join(version))
