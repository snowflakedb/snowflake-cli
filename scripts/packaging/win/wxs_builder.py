import uuid
from pathlib import Path
from xml.etree import ElementTree

PROJECT_ROOT_PATH = Path(__file__).parent.parent.parent.parent
assert PROJECT_ROOT_PATH.parts[-1] == "snowflake-cli"
DIST_DIR = PROJECT_ROOT_PATH.joinpath("dist")
LIBS = DIST_DIR.joinpath("snow")

WXS_TEMPLATE_FILE = (
    Path(__file__).parent.absolute().joinpath("snowflake_cli_template.wxs")
)
WXS_FILE = Path(__file__).parent.absolute().joinpath("snowflake_cli.wxs")

wxs = ElementTree.parse(WXS_TEMPLATE_FILE)
root = wxs.getroot()
snow_files_xpath = ".//{http://schemas.microsoft.com/wix/2006/wi}Component"
snow_files = root.findall(snow_files_xpath)


lib_files = list(LIBS.glob("**/*"))

for lib_path in LIBS.glob("**/*"):
    if lib_path.is_file():
        relative_lib_path = lib_path.relative_to(LIBS)
        relative_file = str(relative_lib_path)

        environment = ElementTree.Element("Environment")
        environment.set("Id", "PATH")
        environment.set("Name", "PATH")
        environment.set("VALUE", "[TESTFILEPRODUCTDIR]")
        environment.set("Permanent", "no")
        environment.set("Part", "last")
        environment.set("Action", "set")
        environment.set("System", "yes")

        file = ElementTree.Element("File")
        file.set("Id", str(relative_lib_path))
        source_path = lib_path.relative_to(PROJECT_ROOT_PATH)
        file.set("Source", str(source_path))
        file.set("Name", "PATH")
        file.set("KeyPath", "yes")
        file.set("Checksum", "yes")

        component = ElementTree.Element("Component")
        component.set("Id", relative_file)
        guid_hash = str(uuid.uuid3(uuid.NAMESPACE_DNS, relative_file)).upper()
        component.set("Guid", guid_hash)
        component.set("Win64", "yes")

        component.append(environment)
        component.append(file)
        snow_files[0].append(component)


ElementTree.indent(root, space="  ", level=0)

with WXS_FILE.expanduser().open("wb") as f:
    wxs.write(f, encoding="utf-8")
