import uuid
from pathlib import Path
from xml.etree import ElementTree

PROJECT_ROOT_PATH = Path(__file__).parent.parent.parent.parent
assert PROJECT_ROOT_PATH.parts[-1] == "snowflake-cli"
DIST_DIR = PROJECT_ROOT_PATH.joinpath("dist")
LIBS = DIST_DIR.joinpath("snow")

WIN_RES_DIR = Path(__file__).parent.absolute()
WXS_TEMPLATE_FILE = WIN_RES_DIR.joinpath("snowflake_cli_template_v4.wxs")
WXS_OUTPUT_FILE = WXS_TEMPLATE_FILE.parent.joinpath("snowflake_cli.wxs")

ns = {
    "util": "http://wixtoolset.org/schemas/v4/wxs/util",
    "": "http://wixtoolset.org/schemas/v4/wxs",
}
for ns_name, ns_url in ns.items():
    ElementTree.register_namespace(ns_name, ns_url)
wxs = ElementTree.parse(WXS_TEMPLATE_FILE)
root = wxs.getroot()
snow_files = root.find(".//DirectoryRef", namespaces=ns)
if snow_files is None:
    raise ValueError("Component not found in the template")


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
        file.set("KeyPath", "yes")
        file.set("Checksum", "yes")

        component = ElementTree.Element("Component")
        component.set("Id", relative_file)
        guid_hash = str(uuid.uuid3(uuid.NAMESPACE_DNS, relative_file)).upper()
        component.set("Guid", guid_hash)
        component.set("Bitness", "always64")

        component.append(environment)
        component.append(file)
        snow_files.extend(component)


ElementTree.indent(root, space="  ", level=0)

wxs.write(WXS_OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
