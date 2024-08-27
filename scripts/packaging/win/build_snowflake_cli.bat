@echo on
echo %PATH%


"C:\Program Files\Python310\python.exe" --version
"C:\Program Files\Python310\python.exe" -m pip install --upgrade pip uv hatch

@echo off
FOR /F "delims=" %%I IN ('"C:\Program Files\Python310\python.exe" -m hatch version') DO CLI_VERSION=%%I
echo %CLI_VERSION%
@echo on

@echo off
set CONTENTSDIR="snowflake-cli-%CLI_VERSION%"
set ENTRYPOINT=src\\snowflake\\cli\\_app\\__main__.py
"C:\Program Files\Python310\python.exe" -m hatch -e packaging run pyinstaller --name snow --onedir --clean --noconfirm --noconsole --contents-directory=%CONTENTSDIR% %ENTRYPOINT%
