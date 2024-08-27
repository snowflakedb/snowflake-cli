@echo on
echo %PATH%


"C:\Program Files\Python310\python.exe" --version
"C:\Program Files\Python310\python.exe" -m pip install --upgrade pip uv hatch



@ECHO OFF
@SET CLI_VERSION =
FOR /F %%I IN ('C:\Program Files\Python310\python.exe -m hatch version') DO @SET "CLI_VERSION=%%I"
@ECHO ON

echo %CLI_VERSION%


REM "C:\Program Files\Python310\python.exe" -m hatch -e packaging run pyinstaller --name snow --onedir --clean --noconfirm --noconsole --contents-directory-snowflake-cli
