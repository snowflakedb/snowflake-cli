from pathlib import Path
from uuid import uuid4
from xml.etree import ElementTree

WXS_FILE = Path(
    "/Users/mraba/sources/snowflake-cli/scripts/packaging/win/snowflake_cli.wxs"
)
WXS_FILE = Path(__file__).parent.absolute().joinpath("snowflake_cli.wxs")

wxs = ElementTree.parse(WXS_FILE)
root = wxs.getroot()
snow_files_xpath = ".//{http://schemas.microsoft.com/wix/2006/wi}Component"
snow_files = root.findall(snow_files_xpath)

LIBS = Path(__file__).parent.parent.parent.parent.joinpath("dist").joinpath("snow")

lib_files = list(LIBS.glob("**/*"))

for lib in LIBS.glob("**/*"):
    if lib.is_file():
        l = lib.relative_to(LIBS)

        environment = ElementTree.Element("Environment")
        environment.set("Id", "PATH")
        environment.set("Name", "PATH")
        environment.set("VALUE", "[TESTFILEPRODUCTDIR]")
        environment.set("Permanent", "no")
        environment.set("Part", "last")
        environment.set("Action", "set")
        environment.set("System", "yes")

        file = ElementTree.Element("File")
        file.set("Id", str(l))
        file.set("Source", str(lib.relative_to(LIBS.parent.parent)))
        file.set("Name", "PATH")
        file.set("KeyPath", "yes")
        file.set("Checksum", "yes")

        component = ElementTree.Element("Component")
        component.set("Id", str(l))
        component.set("Guid", str(uuid4()).upper())
        component.set("Win64", "yes")

        component.append(environment)
        component.append(file)
        snow_files[0].append(component)


# ElementTree.dump(snow_files[0])
ElementTree.indent(root, space="  ", level=0)

with Path("~/Downloads/t.xml").expanduser().open("wb") as f:
    wxs.write(f, encoding="utf-8")
