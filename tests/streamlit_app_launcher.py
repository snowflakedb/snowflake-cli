from snowflake.snowpark.context import get_active_session
import sys, os, zipfile, importlib

import_dir = "/tmp/streamlit_app"
if not os.path.exists(import_dir):
    os.makedirs(import_dir, exist_ok=True)
    session = get_active_session()
    session.file.get(stage_location="example_stage", target_directory=import_dir)
    if False:
        with zipfile.ZipFile("app.zip", "r") as myzip:
            myzip.extractall(import_dir)
        sys.path.append(import_dir)
    else:
        sys.path.append(f"{import_dir}/app.zip")
    os.chdir(import_dir)

if "example_module" in sys.modules:
    importlib.reload(sys.modules["example_module"])
else:
    importlib.import_module("example_module")
