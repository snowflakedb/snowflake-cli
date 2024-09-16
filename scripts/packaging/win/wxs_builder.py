import uuid
from pathlib import Path
from xml.etree import ElementTree

PROJECT_ROOT_PATH = Path(__file__).parent.parent.parent.parent
assert PROJECT_ROOT_PATH.parts[-1] == "snowflake-cli"
DIST_DIR = PROJECT_ROOT_PATH.joinpath("dist")
LIBS = DIST_DIR.joinpath("snow")

WIN_RES_DIR = Path(__file__).parent.absolute()
WXS_TEMPLATE_FILE = WIN_RES_DIR.joinpath("snowflake_cli_template_v3.wxs")
WXS_OUTPUT_FILE = PROJECT_ROOT_PATH.joinpath("snowflake_cli.wxs")

ns = {
    "": "http://schemas.microsoft.com/wix/2006/wi",
    "util": "http://schemas.microsoft.com/wix/UtilExtension",
}
for ns_name, ns_url in ns.items():
    ElementTree.register_namespace(ns_name, ns_url)

wxs = ElementTree.parse(WXS_TEMPLATE_FILE)
root = wxs.getroot()
snow_files = root.find(".//Component[@Id='snow.exe']/..", namespaces=ns)
if snow_files is None:
    raise ValueError("Component not found in the template")


lib_files = list(LIBS.glob("**/*"))

for lib_path in LIBS.glob("**/*"):
    if lib_path.is_file():
        relative_lib_path = lib_path.relative_to(LIBS)
        relative_file = str(
            relative_lib_path
        )  # TODO: this cannot be longer than 72 characters

        source_path = lib_path.relative_to(PROJECT_ROOT_PATH)
        file = ElementTree.Element(
            "File",
            Id=str(relative_lib_path),
            Source=str(source_path),
            KeyPath="yes",  # TODO: This is to be set only on the first occureance of the folder
            Checksum="yes",
        )

        component = ElementTree.Element(
            "Component",
            Id=relative_file,
            Guid=str(uuid.uuid3(uuid.NAMESPACE_DNS, relative_file)).upper(),
            Bitness="always64",
        )

        component.append(file)
        snow_files.append(component)


ElementTree.indent(root, space="  ", level=0)

wxs.write(WXS_OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
